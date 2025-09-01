import os, time
import boto3

from pathlib import Path
from dotenv import load_dotenv


# --- config ---
ROOT = Path(__file__).resolve().parents[1]
# ROOT = Path("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline")    # FOR NOTEBOOK ONLY
load_dotenv(ROOT / ".env")

REGION     = os.getenv("AWS_REGION", "us-east-1")
CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]

_rs = boto3.client("redshift", region_name=REGION)


# --- helper functions ---
def _status():
    resp = _rs.describe_clusters(ClusterIdentifier=CLUSTER_ID)
    return resp["Clusters"][0]["ClusterStatus"]

def resume_cluster():
    st = _status()
    if st in ("available", "resuming"):
        return
    _rs.resume_cluster(ClusterIdentifier=CLUSTER_ID)

def pause_cluster():
    st = _status()
    if st in ("paused", "pausing"):
        return
    _rs.pause_cluster(ClusterIdentifier=CLUSTER_ID)

def wait_for_status(target: str, timeout=3600, poll=20):
    t0 = time.time()
    while True:
        st = _status()
        if target == "available" and st == "available":
            return
        if target == "paused" and st == "paused":
            return
        if time.time() - t0 > timeout:
            raise TimeoutError(f"Timed out waiting for Redshift to be {target}. Last status={st}")
        time.sleep(poll)

def create_snapshot(prefix="reddit-olap"):
    ts = int(time.time())
    snap_id = f"{prefix}-{ts}"
    _rs.create_cluster_snapshot(ClusterIdentifier=CLUSTER_ID, SnapshotIdentifier=snap_id)
    _wait_snapshot_available(snap_id)
    return snap_id

def _wait_snapshot_available(snapshot_id, timeout=3600, poll=20):
    t0 = time.time()
    while True:
        resp = _rs.describe_cluster_snapshots(SnapshotIdentifier=snapshot_id)
        st = resp["Snapshots"][0]["Status"]  # 'available','creating',...
        if st == "available":
            return
        if time.time() - t0 > timeout:
            raise TimeoutError(f"Timed out waiting for snapshot {snapshot_id} to be available (status={st})")
        time.sleep(poll)

def prune_snapshots(prefix="reddit-olap", retain=5):
    """Keep only the newest N snapshots with the prefix."""
    resp = _rs.describe_cluster_snapshots(ClusterIdentifier=CLUSTER_ID)
    snaps = [s for s in resp["Snapshots"] if s["SnapshotIdentifier"].startswith(prefix)]
    snaps.sort(key=lambda s: s["SnapshotCreateTime"], reverse=True)
    for s in snaps[retain:]:
        _rs.delete_cluster_snapshot(SnapshotIdentifier=s["SnapshotIdentifier"])