from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
import pendulum
import sys

sys.path.append("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline/")

from ingest.run_ingest import run
from ELT.bronze_silver import bronze_to_silver
from ELT.silver_gold import tag_and_score

from redshift.redshift_admin import resume_cluster, pause_cluster, wait_for_status, create_snapshot, prune_snapshots
from redshift.lake_olap import load_olap_for_date, get_redshift_conn
from redshift.marts import export_marts_for_date


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="reddit_end-to-end_pipeline",
    description="""
    --- Ingestion and ELT on lake---
    data ingestion >> upsert OLTP & dump to lake(bronze) >> ELT on lake (bronze -> silver -> gold)

    --- Datawarehouse lifecycle ---
    resume Redshift >> upsert OLAP from lake (gold) >> snapshot and prune >> unload marts
    """,
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
    dagrun_timeout=pendulum.duration(hours=6)
)
def ingest_elt():
    @task
    def fetch_reddit():
        return run()

    @task(task_id="bronze_to_silver")
    def bronze_silver(dt):
        return bronze_to_silver(dt)

    @task
    def silver_to_gold(dt):
        return tag_and_score(dt)
        
    @task(task_id="resume_Redshift")
    def resume():
        resume_cluster()
        wait_for_status("available")

    @task(task_id="update_warehouse")
    def load(dt):
        return load_olap_for_date(dt)

    @task
    def snapshot_and_prune():
        snap_id = create_snapshot(prefix="reddit-olap")
        prune_snapshots(prefix="reddit-olap", retain=5)
        return snap_id

    @task
    def export_marts(dt):
        conn = get_redshift_conn()
        try:
            export_marts_for_date(conn, dt)
        finally:
            conn.close()

    @task
    def pause(task_id="pause_Redshift", trigger_rule=TriggerRule.ALL_DONE):
        pause_cluster()
        wait_for_status("paused")
    
    dt = fetch_reddit()
    dt1 = bronze_silver(dt)
    dt2 = silver_to_gold(dt1)    

    r = resume()
    dt3 = load(dt2)
    s = snapshot_and_prune()
    e = export_marts(dt3)
    p = pause()

    dt2 >> r >> dt3
    dt3 >> s >> e >> p
    
    
ingest_elt()