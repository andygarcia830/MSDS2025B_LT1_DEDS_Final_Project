from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
from datetime import datetime

# Make sure your pipeline module is importable
# Assume your script is in: /opt/airflow/reddit_pipeline/reddit_pipeline.py
# Then Airflow will mount dags/ but not always your repo root.
# Append parent folder so Python can find your script.
sys.path.append("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline/nosql")


from ingest_nosql import run   # import run() from your script

# Default args for DAG tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='reddit_nosql_dag',
    default_args=default_args,
    description='Reddit → DynamoDB ingestion pipeline',
    # schedule_interval='@daily',        # run once a day (change as needed)
    # start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['reddit', 'nosql', 'pipeline'],
    start_date=datetime(2025, 8, 31),   # or any fixed start
    schedule="0 * * * *",   
    
) as dag:

    ingest_nosql_task = PythonOperator(
        task_id='ingest_reddit_nosql',
        python_callable=run,
        op_kwargs={
            'subs': [
                'LivingWithMBC', 'breastcancer', 'ovariancancer_new',
                'BRCA', 'cancer', 'IndustrialPharmacy'
            ],
        }
    )

  
    ingest_nosql_task
