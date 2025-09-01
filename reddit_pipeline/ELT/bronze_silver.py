import boto3
import pandas as pd
import os, io, json, gzip, re
import datetime as dt
from pathlib import Path
from dotenv import load_dotenv

import pyarrow as pa, pyarrow.parquet as pq


# --- config ---
ROOT = Path(__file__).resolve().parents[1]
# ROOT = Path("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline")    # FOR NOTEBOOK ONLY
load_dotenv(ROOT / ".env")

BUCKET = os.environ["LAKE_BUCKET"]
s3 = boto3.client("s3")


# --- helper functions ---
def list_keys(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            yield o["Key"]

def read_jsonl_gz(key):
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET, key, buf)
    buf.seek(0)
    rows=[]
    with gzip.GzipFile(fileobj=buf, mode="rb") as gz:
        for line in gz:
            rows.append(json.loads(line))
    return rows

def write_parquet(df, key):
    table = pa.Table.from_pandas(df, preserve_index=False)
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)
    s3.upload_fileobj(out, BUCKET, key)

def bronze_to_silver(date_str):
    print(f"Bronze -> Silver")
    bronzepfx = f"bronze/reddit/dt={date_str}/"
    post_frames, comment_frames, author_rows, subr_rows = [], [], [], []

    for key in list_keys(bronzepfx):
        m = re.search(r"dt=(.+?)/subreddit=([^/]+)/run_id=([^/]+)/(.+)\.jsonl\.gz$", key)
        if not m:
            continue
        dt_part, sr, run_id, kind = m.groups()
        print(f"  - Processing {kind} from {sr}")
        rows = read_jsonl_gz(key)
        if not rows:
            continue

        if "posts" in kind:
            df = pd.DataFrame(rows)
            df["created_ts"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
            df["dt"] = dt_part
            df["subreddit"] = sr
            df["run_id"] = run_id
            
            # dedup by post_name (keep last)
            df = df.drop_duplicates(subset=["post_name"], keep="last")

            post_frames.append(df[[
                "post_name","subreddit_id","author_fullname","title","selftext","score","subreddit",
                "upvote_ratio","num_comments","url","created_utc","created_ts","run_id","dt",
                "subreddit_name_prefixed","subreddit_type","subreddit_subscribers","author","author_premium"
            ]])

            # dimension snapshots
            subr_rows += df[["subreddit_id","subreddit_name_prefixed","subreddit_type","subreddit_subscribers"]].to_dict("records")
            
            # author can be null
            author_rows += df[["author_fullname","author","author_premium"]].to_dict("records")

        elif "comments" in kind:
            df = pd.DataFrame(rows)
        
            # normalize parent: set to NULL if parent is a post (t3_)
            df["parent_comment_name"] = df["parent_comment_name"].where(~df["parent_comment_name"].astype(str).str.startswith("t3_"), None)
            
            df["created_ts"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
            df["dt"] = dt_part
            df["subreddit"] = sr
            df["run_id"] = run_id

            # dedup
            df = df.drop_duplicates(subset=["comment_name"], keep="last")

            # drop bot comments
            mask = df["body"].str.contains("I am a bot", case=False, na=False)
            df = df.loc[~mask]
            
            comment_frames.append(df[[
                "comment_name","post_name","parent_comment_name","author_fullname","subreddit",
                "body","score","created_utc","created_ts","run_id","dt","author","author_premium"
            ]])
            
            author_rows += df[["author_fullname","author","author_premium"]].to_dict("records")

    # Concatenate and write partitioned folders
    if post_frames:
        posts = pd.concat(post_frames, ignore_index=True)
        # write per subreddit partition
        for sr, df_sr in posts.groupby("subreddit"):
            key = f"silver/reddit/posts/dt={date_str}/subreddit={sr}/part-0.parquet"
            write_parquet(df_sr.drop(columns=["subreddit"]), key)
    if comment_frames:
        comments = pd.concat(comment_frames, ignore_index=True)
        for sr, df_sr in comments.groupby("subreddit"):
            key = f"silver/reddit/comments/dt={date_str}/subreddit={sr}/part-0.parquet"
            write_parquet(df_sr.drop(columns=["subreddit"]), key)
    if author_rows:
        a = pd.DataFrame(author_rows).drop_duplicates(subset=["author_fullname"], keep="last")
        key = f"silver/reddit/authors/dt={date_str}/part-0.parquet"
        write_parquet(a, key)
    if subr_rows:
        s = pd.DataFrame(subr_rows).drop_duplicates(subset=["subreddit_id"], keep="last")
        key = f"silver/reddit/subreddits/dt={date_str}/part-0.parquet"
        write_parquet(s, key)

    return date_str


if __name__ == "__main__":
    date_str = "2025-09-01"
    bronze_to_silver(date_str)
    