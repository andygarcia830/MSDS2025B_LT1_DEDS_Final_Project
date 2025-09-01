import os, io, json, uuid
from pathlib import Path
from dotenv import load_dotenv

import boto3
import psycopg2
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq


# --- config ---
ROOT = Path(__file__).resolve().parents[1]
# ROOT = Path("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline")    # FOR NOTEBOOK ONLY
load_dotenv(ROOT / ".env")

BUCKET = os.environ["LAKE_BUCKET"]
REDSHIFT_SECRET_ARN = os.environ["REDDIT_OLAP_ARN"]
REDSHIFT_DB = os.environ["OLAP_DB"]
REDSHIFT_IAM_ROLE_ARN = os.environ["IAM_ROLE_ARN"]

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


# --- helper functions ---
def list_keys(prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            yield o["Key"]

def read_parquet_to_df(key: str) -> pd.DataFrame:
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET, key, buf)
    buf.seek(0)
    table = pq.read_table(buf)
    return table.to_pandas()

def write_parquet(df: pd.DataFrame, key: str):
    table = pa.Table.from_pandas(df, preserve_index=False)
    out = io.BytesIO()
    pq.write_table(table, out, compression="snappy")
    out.seek(0)
    s3.upload_fileobj(out, BUCKET, key)

def write_staging_parquet(df: pd.DataFrame, key_prefix: str) -> str:
    if df.empty:
        return None
    key = f"{key_prefix}/part-{uuid.uuid4().hex}.parquet"
    write_parquet(df, key)
    return "/".join(key.split("/")[:-1]) + "/"


# --- Redshift ---
def get_redshift_conn():
    cfg = json.loads(secrets.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)["SecretString"])
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5439),
        user=cfg["username"],
        password=cfg["password"],
        dbname=REDSHIFT_DB,
    )

def copy_from_parquet(cur, table_name: str, s3_prefix: str):
    sql = f"""
        COPY {table_name}
        FROM 's3://{BUCKET}/{s3_prefix}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS PARQUET;
    """
    cur.execute(sql)


# --- tags staging ---
def build_tag_staging(date_str: str) -> str | None:
    posts_prefix    = f"gold/reddit/post_enriched/dt={date_str}/"
    comments_prefix = f"gold/reddit/comment_enriched/dt={date_str}/"

    # Build post_name -> subreddit_id map while reading posts
    post_to_subr = {}
    tag_rows = []

    # POSTS
    post_keys = [k for k in list_keys(posts_prefix) if k.endswith(".parquet")]
    for key in post_keys:
        df = read_parquet_to_df(key)
        if df.empty:
            continue
        # map post to subreddit_id
        if "post_name" in df and "subreddit_id" in df:
            post_to_subr.update(df.set_index("post_name")["subreddit_id"].to_dict())
        if "tags" not in df.columns:
            continue
        # explode tags
        tmp = df[["tags", "created_ts", "subreddit_id", "score", "negative_s", "neutral_s", "positive_s"]].copy()
        tmp = tmp.explode("tags").dropna(subset=["tags"])
        if tmp.empty:
            continue
        tmp["date_value"] = pd.to_datetime(tmp["created_ts"], utc=True).dt.date
        tmp = tmp.rename(columns={
            "tags": "tag_name",
            "negative_s": "negative",
            "neutral_s": "neutral",
            "positive_s": "positive",
        })
        tag_rows.append(tmp[["tag_name", "date_value", "subreddit_id", "score", "negative", "neutral", "positive"]])

    # COMMENTS
    comment_keys = [k for k in list_keys(comments_prefix) if k.endswith(".parquet")]
    for key in comment_keys:
        df = read_parquet_to_df(key)
        if df.empty or "tags" not in df.columns:
            continue
        # ensure subreddit_id exists for comments via post map
        if "subreddit_id" not in df.columns and "post_name" in df.columns:
            df["subreddit_id"] = df["post_name"].map(post_to_subr)
        tmp = df[["tags", "created_ts", "subreddit_id", "score", "negative_s", "neutral_s", "positive_s"]].copy()
        tmp = tmp.explode("tags").dropna(subset=["tags", "subreddit_id"])
        if tmp.empty:
            continue
        tmp["date_value"] = pd.to_datetime(tmp["created_ts"], utc=True).dt.date
        tmp = tmp.rename(columns={
            "tags": "tag_name",
            "negative_s": "negative",
            "neutral_s": "neutral",
            "positive_s": "positive",
        })
        tag_rows.append(tmp[["tag_name", "date_value", "subreddit_id", "score", "negative", "neutral", "positive"]])

    if not tag_rows:
        return None

    stg = pd.concat(tag_rows, ignore_index=True)
    return write_staging_parquet(stg, f"tmp/redshift/stg_tag_rows/dt={date_str}")

def project_gold(prefix: str, out_base: str, date_str: str) -> str | None:
    stg_rows = []
    for key in list_keys(prefix):
        if not key.endswith(".parquet"): 
            continue
        df = read_parquet_to_df(key)
        if df.empty:
            continue
        df = df.drop(columns=[
            'title',
            'selftext',
            'url',
            'created_utc',
            'run_id',
            'dt',
            'tags',
            'sentiment_label',
            'scored_at',
            'parent_comment_name',
            'body',
        ], errors='ignore')
        stg_rows.append(df)
    stg = pd.concat(stg_rows, ignore_index=True)
    return write_staging_parquet(stg, f"tmp/redshift/{out_base}/dt={date_str}")

# --- main ---
def load_olap_for_date(date_str: str):
    posts_prefix = project_gold(
        prefix=f"gold/reddit/post_enriched/dt={date_str}/",
        out_base="post_enriched",
        date_str=date_str
    )

    comments_prefix = project_gold(
        prefix=f"gold/reddit/comment_enriched/dt={date_str}/",
        out_base="comment_enriched",
        date_str=date_str
    )

    has_posts = any(list_keys(posts_prefix))
    has_comments = any(list_keys(comments_prefix))
    if not has_posts and not has_comments:
        print(f"[{date_str}] No Gold data found under {posts_prefix} or {comments_prefix}. Nothing to load.")
        return

    # Build tag staging
    stg_tags_pfx = build_tag_staging(date_str)

    # for upserting new dates
    date_sources = []
    if has_posts:
        date_sources.append("SELECT DISTINCT CAST(created_ts AS DATE) AS date_value FROM stg_post_enriched")
    if has_comments:
        date_sources.append("SELECT DISTINCT CAST(created_ts AS DATE) AS date_value FROM stg_comment_enriched")
    dates_union = " UNION ".join(date_sources)

    conn = get_redshift_conn()
    try:
        with conn, conn.cursor() as cur:
            # -------- staging tables --------
            if has_posts:
                cur.execute("""
                    CREATE TEMP TABLE stg_post_enriched (
                      post_name VARCHAR(32),
                      subreddit_id VARCHAR(32),
                      author_fullname VARCHAR(32),
                      score BIGINT,
                      upvote_ratio DOUBLE PRECISION,
                      num_comments BIGINT,
                      created_ts TIMESTAMPTZ,
                      subreddit_name_prefixed VARCHAR(100),
                      subreddit_type VARCHAR(20),
                      subreddit_subscribers BIGINT,
                      author VARCHAR(100),
                      author_premium BOOLEAN,
                      negative_s DOUBLE PRECISION,
                      neutral_s DOUBLE PRECISION,
                      positive_s DOUBLE PRECISION
                    );
                """)
                copy_from_parquet(cur, "stg_post_enriched", posts_prefix)

            if has_comments:
                cur.execute("""
                    CREATE TEMP TABLE stg_comment_enriched (
                      comment_name VARCHAR(32),
                      post_name VARCHAR(32),
                      author_fullname VARCHAR(32),
                      score BIGINT,
                      created_ts TIMESTAMPTZ,
                      author VARCHAR(100),
                      author_premium BOOLEAN,
                      negative_s DOUBLE PRECISION,
                      neutral_s DOUBLE PRECISION,
                      positive_s DOUBLE PRECISION
                    );
                """)
                copy_from_parquet(cur, "stg_comment_enriched", comments_prefix)

            if stg_tags_pfx:
                cur.execute("""
                    CREATE TEMP TABLE stg_tag_rows (
                      tag_name     VARCHAR(100),
                      date_value   DATE,
                      subreddit_id VARCHAR(32),
                      score BIGINT,
                      negative DOUBLE PRECISION,
                      neutral DOUBLE PRECISION,
                      positive DOUBLE PRECISION
                    );
                """)
                copy_from_parquet(cur, "stg_tag_rows", stg_tags_pfx)

            # --- DIMS ---
            # dim_date from any created_ts
            cur.execute(f"""
                MERGE INTO dim_date
                USING (
                  {dates_union}
                ) s
                ON dim_date.date_value = s.date_value
                WHEN MATCHED THEN UPDATE SET
                  date_value = dim_date.date_value
                WHEN NOT MATCHED THEN INSERT (date_value, "year","month","day","dow",month_name,dow_name,is_weekend)
                VALUES (
                  s.date_value,
                  EXTRACT(year  FROM s.date_value)::SMALLINT,
                  EXTRACT(month FROM s.date_value)::SMALLINT,
                  EXTRACT(day   FROM s.date_value)::SMALLINT,
                  EXTRACT(dow   FROM s.date_value)::SMALLINT,
                  TO_CHAR(s.date_value,'Month'),
                  TO_CHAR(s.date_value,'Dy'),
                  CASE WHEN EXTRACT(dow FROM s.date_value) IN (0,6) THEN 'Weekend' ELSE 'Weekday' END
                );
                """)

            # dim_subreddit (posts are authoritative for metadata)
            if has_posts:
                cur.execute("""
                    MERGE INTO dim_subreddit
                    USING (
                      SELECT DISTINCT subreddit_id, subreddit_name_prefixed, subreddit_type, subreddit_subscribers
                      FROM stg_post_enriched
                      WHERE subreddit_id IS NOT NULL
                    ) s
                    ON dim_subreddit.subreddit_id = s.subreddit_id
                    WHEN MATCHED THEN UPDATE
                      SET subreddit_name_prefixed = s.subreddit_name_prefixed,
                          subreddit_type          = s.subreddit_type,
                          subreddit_subscribers   = s.subreddit_subscribers
                    WHEN NOT MATCHED THEN INSERT (subreddit_id, subreddit_name_prefixed, subreddit_type, subreddit_subscribers)
                      VALUES (s.subreddit_id, s.subreddit_name_prefixed, s.subreddit_type, s.subreddit_subscribers);
                """)

            # dim_author from posts + comments
            cur.execute("""
                MERGE INTO dim_author
                USING (
                  SELECT DISTINCT
                      author_fullname,
                      author,
                      CASE
                          WHEN author_premium IS TRUE  THEN 'PREMIUM'
                          WHEN author_premium IS FALSE THEN 'NOT PREMIUM'
                          ELSE NULL
                      END AS author_premium_txt
                  FROM (
                    SELECT author_fullname, author, author_premium FROM stg_post_enriched
                    UNION ALL
                    SELECT author_fullname, author, author_premium FROM stg_comment_enriched
                  ) u
                  WHERE author_fullname IS NOT NULL
                ) s
                ON dim_author.author_fullname = s.author_fullname
                WHEN MATCHED THEN UPDATE
                  SET author = s.author, author_premium = s.author_premium_txt
                WHEN NOT MATCHED THEN INSERT (author_fullname, author, author_premium)
                  VALUES (s.author_fullname, s.author, s.author_premium_txt);
            """)

            # -------- FACTS --------
            if has_posts:
                cur.execute("""
                    MERGE INTO fact_post
                    USING (
                      SELECT
                        p.post_name,
                        dd.date_key,
                        ds.subreddit_sk,
                        da.author_sk,
                        p.score,
                        p.upvote_ratio,
                        p.num_comments,
                        p.negative_s AS negative,
                        p.neutral_s  AS neutral,
                        p.positive_s AS positive,
                        (p.positive_s - p.negative_s) AS net_sentiment
                      FROM stg_post_enriched p
                      JOIN dim_date dd        ON dd.date_value = CAST(p.created_ts AS DATE)
                      JOIN dim_subreddit ds   ON ds.subreddit_id = p.subreddit_id
                      LEFT JOIN dim_author da ON da.author_fullname = p.author_fullname
                    ) s
                    ON fact_post.post_name = s.post_name
                    WHEN MATCHED THEN UPDATE SET
                      date_key = s.date_key,
                      subreddit_sk = s.subreddit_sk,
                      author_sk = s.author_sk,
                      score = s.score,
                      upvote_ratio = s.upvote_ratio,
                      num_comments = s.num_comments,
                      negative = s.negative,
                      neutral = s.neutral,
                      positive = s.positive,
                      net_sentiment = s.net_sentiment
                    WHEN NOT MATCHED THEN INSERT
                      (post_name, date_key, subreddit_sk, author_sk, score, upvote_ratio, num_comments,
                       negative, neutral, positive, net_sentiment)
                    VALUES
                      (s.post_name, s.date_key, s.subreddit_sk, s.author_sk, s.score, s.upvote_ratio, s.num_comments,
                       s.negative, s.neutral, s.positive, s.net_sentiment);
                """)

            if has_comments:
                cur.execute("""
                    MERGE INTO fact_comment
                    USING (
                      SELECT
                        c.comment_name,
                        c.post_name,
                        dd.date_key,
                        fp.subreddit_sk,
                        da.author_sk,
                        c.score,
                        c.negative_s AS negative,
                        c.neutral_s  AS neutral,
                        c.positive_s AS positive,
                        (c.positive_s - c.negative_s) AS net_sentiment
                      FROM stg_comment_enriched c
                      JOIN dim_date dd        ON dd.date_value = CAST(c.created_ts AS DATE)
                      LEFT JOIN fact_post fp  ON fp.post_name = c.post_name
                      LEFT JOIN dim_author da ON da.author_fullname = c.author_fullname
                    ) s
                    ON fact_comment.comment_name = s.comment_name
                    WHEN MATCHED THEN UPDATE SET
                      post_name = s.post_name,
                      date_key = s.date_key,
                      subreddit_sk = s.subreddit_sk,
                      author_sk = s.author_sk,
                      score = s.score,
                      negative = s.negative,
                      neutral  = s.neutral,
                      positive = s.positive,
                      net_sentiment = s.net_sentiment
                    WHEN NOT MATCHED THEN INSERT
                      (comment_name, post_name, date_key, subreddit_sk, author_sk, score,
                       negative, neutral, positive, net_sentiment)
                    VALUES
                      (s.comment_name, s.post_name, s.date_key, s.subreddit_sk, s.author_sk, s.score,
                       s.negative, s.neutral, s.positive, s.net_sentiment);
                """)

            # -------- TAGS --------
            if stg_tags_pfx:
                cur.execute("""
                    MERGE INTO fact_tags
                    USING (
                      SELECT
                        r.tag_name,
                        dd.date_key,
                        ds.subreddit_sk,
                        SUM(COALESCE(r.score,0))                AS score,
                        AVG(r.negative)                          AS negative,
                        AVG(r.neutral)                           AS neutral,
                        AVG(r.positive)                          AS positive,
                        AVG(r.positive - r.negative)             AS net_sentiment
                      FROM stg_tag_rows r
                      JOIN dim_date dd      ON dd.date_value = r.date_value
                      JOIN dim_subreddit ds ON ds.subreddit_id = r.subreddit_id
                      GROUP BY 1,2,3
                    ) s
                    ON fact_tags.tag_name = s.tag_name AND fact_tags.date_key = s.date_key AND fact_tags.subreddit_sk = s.subreddit_sk
                    WHEN MATCHED THEN UPDATE SET
                      score = s.score,
                      negative = s.negative, neutral = s.neutral,
                      positive = s.positive, net_sentiment = s.net_sentiment
                    WHEN NOT MATCHED THEN INSERT
                      (tag_name, date_key, subreddit_sk, score, negative, neutral, positive, net_sentiment)
                    VALUES
                      (s.tag_name, s.date_key, s.subreddit_sk, s.score, s.negative, s.neutral, s.positive, s.net_sentiment);
                """)

            cur.execute("ANALYZE;")
        print(f"[{date_str}] OLAP load complete.")
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    date_str = "2025-08-29"
    load_olap_for_date(date_str)