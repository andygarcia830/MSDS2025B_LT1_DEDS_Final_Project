import os
import psycopg2

from pathlib import Path
from dotenv import load_dotenv


# --- config ---
ROOT = Path(__file__).resolve().parents[1]
# ROOT = Path("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline")    # FOR NOTEBOOK ONLY
load_dotenv(ROOT / ".env")

BUCKET = os.environ["LAKE_BUCKET"]
REDSHIFT_IAM_ROLE_ARN = os.environ["IAM_ROLE_ARN"]


# --- helper functions ---
def unload_parquet(cur, sql: str, s3_prefix: str):
    cur.execute(f"""
        UNLOAD ($${sql}$$)
        TO 's3://{BUCKET}/{s3_prefix}/part_'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        PARQUET
        ALLOWOVERWRITE;
    """)

def export_marts_for_date(conn, date_str: str):
    with conn, conn.cursor() as cur:
        # Daily subreddit sentiment (posts)
        sql_posts = f"""
            SELECT
              dd.date_value AS dt,
              ds.subreddit_id,
              ds.subreddit_name_prefixed,
              COUNT(*) AS posts,
              AVG(f.positive) AS avg_positive,
              AVG(f.negative) AS avg_negative,
              AVG(f.net_sentiment) AS avg_net_sentiment,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.net_sentiment) AS median_net_sentiment
            FROM fact_post f
            JOIN dim_date dd ON dd.date_key = f.date_key
            JOIN dim_subreddit ds ON ds.subreddit_sk = f.subreddit_sk
            WHERE dd.date_value = DATE '{date_str}'
            GROUP BY 1,2,3
        """
        unload_parquet(cur, sql_posts, f"marts/reddit/subreddit_daily/dt={date_str}")

        # Daily tags sentiment (already aggregated in fact_tags)
        sql_tags = f"""
            SELECT
              dd.date_value AS dt,
              ds.subreddit_name_prefixed,
              t.tag_name,
              t.score,
              t.negative,
              t.neutral,
              t.positive,
              t.net_sentiment
            FROM fact_tags t
            JOIN dim_date dd ON dd.date_key = t.date_key
            JOIN dim_subreddit ds ON ds.subreddit_sk = t.subreddit_sk
            WHERE dd.date_value = DATE '{date_str}'
        """
        unload_parquet(cur, sql_tags, f"marts/reddit/tags_daily/dt={date_str}")

        # Comment volume and sentiment by subreddit
        sql_comments = f"""
            SELECT
              dd.date_value AS dt,
              ds.subreddit_name_prefixed,
              COUNT(*) AS comments,
              AVG(fc.net_sentiment) AS avg_comment_net_sentiment
            FROM fact_comment fc
            JOIN dim_date dd ON dd.date_key = fc.date_key
            JOIN dim_subreddit ds ON ds.subreddit_sk = fc.subreddit_sk
            WHERE dd.date_value = DATE '{date_str}'
            GROUP BY 1,2
        """
        unload_parquet(cur, sql_comments, f"marts/reddit/comments_daily/dt={date_str}")
