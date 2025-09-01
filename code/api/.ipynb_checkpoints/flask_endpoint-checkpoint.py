from flask import Flask, request, jsonify
import json, boto3, os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, date, timedelta
import datetime as dt

from boto3.dynamodb.conditions import Key
from flasgger import Swagger
import psycopg2

from io import BytesIO
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq


# --- config ---
ROOT = Path(__file__).resolve().parents[1] / "reddit_pipeline"
load_dotenv(ROOT / ".env")

REDSHIFT_SECRET_ARN = os.environ["REDDIT_OLAP_ARN"]
REDSHIFT_DB = os.environ["OLAP_DB"]
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.environ["LAKE_BUCKET"]

secrets = boto3.client('secretsmanager')
s3 = boto3.client("s3", region_name=AWS_REGION)

# Marts (S3 prefixes)
PREFIX_SUBREDDIT_DAILY = "marts/reddit/subreddit_daily"
PREFIX_COMMENTS_DAILY  = "marts/reddit/comments_daily"
PREFIX_TAGS_DAILY      = "marts/reddit/tags_daily"


app = Flask(__name__)
swagger = Swagger(app)


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

# --- DynamoDB ---
def dynamodb_client():
    return boto3.resource("dynamodb", region_name="us-east-1")


# ---- S3 / Marts helpers ---
def _list_s3_keys(prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def _read_parquet_key_to_df(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    table = pq.read_table(BytesIO(obj["Body"].read()))
    return table.to_pandas()

def _daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def _read_partitioned_mart(prefix_root: str, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Read all Parquet parts across dt partitions within [start_date, end_date] inclusive.
    Expects layout: {prefix_root}/dt=YYYY-MM-DD/part_*
    """
    dfs = []
    for d in _daterange(start_date, end_date):
        part_prefix = f"{prefix_root}/dt={d.isoformat()}/"
        for key in _list_s3_keys(part_prefix):
            # only read the part files
            if os.path.basename(key).startswith("part_"):
                try:
                    dfs.append(_read_parquet_key_to_df(key))
                except ClientError as e:
                    # Skip missing/deleted objects silently
                    continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def _parse_date(s: str, field: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"{field} must be in YYYY-MM-DD format")


# Helper Function To Retrieve Entries

def get_dynamo_entries(sub, start_time=None, end_time=None):
    if start_time is None:
        now = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        start_time = now.replace(minute=0, second=0, microsecond=0)
    if end_time is None:
        end_time = dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = end_time
    start_time = start_time

    dy = dynamodb_client()
    table = dy.Table("reddit")

    pk = f"SUBREDDIT:{sub}"
    response = table.query(
        KeyConditionExpression=Key("PK").eq(pk) &
                               Key("SK").between(start_time, end_time)
    )

    return response.get("Items", [])

@app.route("/")
def home():
    """Welcome message for the Oncolens Subreddit Stats API.
    ---
    responses:
      200:
        description: Returns a welcome message
        content:
          application/json:
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: "Welcome to the Subreddit Stats API!"
    """
    return jsonify({"message": "Welcome to the Oncolens Subreddit Stats API!"})



@app.route("/interest", methods=["POST"])
def get_interest():
    """
    Retrieve subreddit statistics for a given time range.
    ---
    tags:
      - Subreddit Stats
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            sub:
              type: string
              description: The subreddit name (without "r/").
              example: "LivingWithMBC"
            start_time:
              type: string
              format: date-time
              description: Start of the time range (ISO8601 UTC). Defaults to the top of the current hour.
              example: "2025-08-31T00:00:00Z"
            end_time:
              type: string
              format: date-time
              description: End of the time range (ISO8601 UTC). Defaults to now.
              example: "2025-08-31T12:59:59Z"
    responses:
      200:
        description: List of subreddit statistics within the given range.
        content:
          application/json:
            schema:
              type: array
              items:
                type: object
                properties:
                  PK:
                    type: string
                    example: "SUBREDDIT:Python"
                  SK:
                    type: string
                    example: "2025-08-31T12:00:00Z"
                  subscribers:
                    type: integer
                    example: 1425361
                  active_users:
                    type: integer
                    example: 4512
      400:
        description: Missing or invalid input.
        content:
          application/json:
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Missing subreddit name"
    """
    data = request.json
    sub = data.get("sub")
    start_time = data.get("start_time")
    end_time = data.get("end_time")

    if not sub:
        return jsonify({"error": "Missing subreddit name"}), 400

    return get_dynamo_entries(sub, start_time, end_time)

# @app.route("/subreddit_activity", methods=["GET"])
# def subreddit_activity():
#     """
#     Get daily activity for a subreddit (posts & comments).
#     ---
#     tags:
#       - Subreddit Analytics
#     parameters:
#       - name: subreddit
#         in: query
#         type: string
#         required: true
#         description: Subreddit name (e.g. r/AskPH)
#         example: r/AskPH
#       - name: start_date
#         in: query
#         type: string
#         format: date
#         required: true
#         description: Start date (YYYY-MM-DD)
#         example: 2025-08-01
#       - name: end_date
#         in: query
#         type: string
#         format: date
#         required: true
#         description: End date (YYYY-MM-DD)
#         example: 2025-08-31
#     responses:
#       200:
#         description: Daily post and comment counts
#         schema:
#           type: array
#           items:
#             type: object
#             properties:
#               date:
#                 type: string
#                 example: 2025-08-05
#               subreddit:
#                 type: string
#                 example: r/AskPH
#               posts:
#                 type: integer
#                 example: 25
#               comments:
#                 type: integer
#                 example: 130
#     """
#     subreddit = request.args.get("subreddit")
#     start_date = request.args.get("start_date")
#     end_date = request.args.get("end_date")

#     query = """
#         SELECT d.date_value,
#                s.subreddit_name_prefixed,
#                COUNT(DISTINCT p.post_name) AS total_posts,
#                COUNT(DISTINCT c.comment_name) AS total_comments
#         FROM dim_date d
#         LEFT JOIN fact_post p ON p.date_key = d.date_key
#         LEFT JOIN dim_subreddit s ON p.subreddit_sk = s.subreddit_sk
#         LEFT JOIN fact_comment c ON c.subreddit_sk = s.subreddit_sk
#                                   AND c.date_key = d.date_key
#         WHERE s.subreddit_name_prefixed = %s
#           AND d.date_value BETWEEN %s AND %s
#         GROUP BY d.date_value, s.subreddit_name_prefixed
#         ORDER BY d.date_value;
#     """
#     conn = get_redshift_conn()
#     cur = conn.cursor()
#     cur.execute(query, (subreddit, start_date, end_date))
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     return jsonify([
#         {"date": str(r[0]), "subreddit": r[1], "posts": r[2], "comments": r[3]} for r in rows
#     ])

# ------------------------
# 2. Top Authors
# ------------------------
# # @app.route("/top_authors", methods=["GET"])
# def top_authors():
#     """
#     Get top authors by post score.
#     ---
#     tags:
#       - Author Analytics
#     parameters:
#       - name: limit
#         in: query
#         type: integer
#         required: false
#         default: 10
#         description: Number of authors to return
#       - name: subreddit
#         in: query
#         type: string
#         required: false
#         description: Filter by subreddit (e.g. r/OffMyChestPH)
#     responses:
#       200:
#         description: Top authors sorted by score
#         schema:
#           type: array
#           items:
#             type: object
#             properties:
#               author:
#                 type: string
#                 example: tagalog100
#               total_score:
#                 type: integer
#                 example: 1250
#               post_count:
#                 type: integer
#                 example: 42
#     """
#     limit = request.args.get("limit", 10)
#     subreddit = request.args.get("subreddit")

#     query = """
#         SELECT a.author,
#                SUM(p.score) AS total_score,
#                COUNT(p.post_name) AS post_count
#         FROM fact_post p
#         JOIN dim_author a ON p.author_sk = a.author_sk
#         JOIN dim_subreddit s ON p.subreddit_sk = s.subreddit_sk
#         {where}
#         GROUP BY a.author
#         ORDER BY total_score DESC
#         LIMIT %s;
#     """
#     where = "WHERE s.subreddit_name_prefixed = %s" if subreddit else ""
#     query = query.format(where=where)

#     conn = get_redshift_conn()
#     cur = conn.cursor()
#     if subreddit:
#         cur.execute(query, (subreddit, limit))
#     else:
#         cur.execute(query, (limit,))
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     return jsonify([
#         {"author": r[0], "total_score": r[1], "post_count": r[2]} for r in rows
#     ])

# ------------------------
# 3. Sentiment Analysis
# ------------------------
# @app.route("/sentiment", methods=["GET"])
# def sentiment():
#     """
#     Get average daily sentiment for a subreddit.
#     ---
#     tags:
#       - Sentiment Analytics
#     parameters:
#       - name: subreddit
#         in: query
#         type: string
#         required: true
#         description: Subreddit name (e.g. r/adultingph)
#       - name: start_date
#         in: query
#         type: string
#         format: date
#         required: true
#       - name: end_date
#         in: query
#         type: string
#         format: date
#         required: true
#     responses:
#       200:
#         description: Average sentiment per day
#         schema:
#           type: array
#           items:
#             type: object
#             properties:
#               date:
#                 type: string
#                 example: 2025-08-05
#               avg_sentiment:
#                 type: number
#                 example: 0.73
#     """
#     subreddit = request.args.get("subreddit")
#     start_date = request.args.get("start_date")
#     end_date = request.args.get("end_date")

#     query = """
#         SELECT d.date_value,
#                AVG(CAST(c.sentiment_score AS FLOAT)) AS avg_sentiment
#         FROM fact_comment c
#         JOIN dim_date d ON c.date_key = d.date_key
#         JOIN dim_subreddit s ON c.subreddit_sk = s.subreddit_sk
#         WHERE s.subreddit_name_prefixed = %s
#           AND d.date_value BETWEEN %s AND %s
#         GROUP BY d.date_value
#         ORDER BY d.date_value;
#     """
#     conn = get_redshift_conn()
#     cur = conn.cursor()
#     cur.execute(query, (subreddit, start_date, end_date))
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     return jsonify([
#         {"date": str(r[0]), "avg_sentiment": r[1]} for r in rows
#    ])

# ------------------------
# 4. Post → Comment Drilldown
# ------------------------
# @app.route("/post/<post_name>/comments", methods=["GET"])
# def post_comments(post_name):
#     """
#     Get comments for a specific post.
#     ---
#     tags:
#       - Post Drilldown
#     parameters:
#       - name: post_name
#         in: path
#         type: string
#         required: true
#         description: Post identifier (e.g. t3_1mcow7t)
#     responses:
#       200:
#         description: List of comments for the post
#         schema:
#           type: array
#           items:
#             type: object
#             properties:
#               comment:
#                 type: string
#                 example: t1_n71lr7s
#               score:
#                 type: integer
#                 example: 12
#               sentiment:
#                 type: number
#                 example: 0.85
#     """
#     query = """
#         SELECT c.comment_name, c.score, c.sentiment_score
#         FROM fact_comment c
#         WHERE c.post_name = %s
#         ORDER BY c.score DESC;
#     """
#     conn = get_redshift_conn()
#     cur = conn.cursor()
#     cur.execute(query, (post_name,))
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     return jsonify([
#         {"comment": r[0], "score": r[1], "sentiment": r[2]} for r in rows
#     ])


# --- Marts routes ---
@app.route("/sentiment/posts", methods=["GET"])
def sentiment():
    """
    Retrieve *daily subreddit* metrics from marts (posts & post sentiment) for a given date range.
    ---
    tags:
      - Subreddit Analytics (Marts)
    parameters:
      - name: subreddit
        in: query
        type: string
        required: true
        description: Subreddit name (e.g. r/cancer)
        example: r/cancer
      - name: subreddit_id
        in: query
        type: string
        required: false
        description: Subreddit ID (e.g. t5_2qh0y). Optional if subreddit_id supplied.
        example: t5_2qh0y
      - name: start_date
        in: query
        type: string
        format: date
        required: false
        description: Start date (YYYY-MM-DD). Defaults to 7 days ago.
        example: 2025-08-01
      - name: end_date
        in: query
        type: string
        format: date
        required: false
        description: End date (YYYY-MM-DD). Defaults to today.
        example: 2025-08-31
    responses:
      200:
        description: Daily subreddit metrics (posts & post sentiment)
        content:
          application/json:
            schema:
              type: array
              items:
                type: object
                properties:
                  date:
                    type: string
                  subreddit_id:
                    type: string
                  subreddit:
                    type: string
                  posts:
                    type: integer
                  avg_positive:
                    type: number
                  avg_negative:
                    type: number
                  avg_net_sentiment:
                    type: number
                  median_net_sentiment:
                    type: number
    """
    subreddit = request.args.get("subreddit")       # e.g., "r/AskPH"
    subreddit_id = request.args.get("subreddit_id") # e.g., "2qh0y"
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # Defaults: last 7 days
    today = date.today()
    if not start_date:
        start = today - timedelta(days=7)
    else:
        start = _parse_date(start_date, "start_date")
    if not end_date:
        end = today
    else:
        end = _parse_date(end_date, "end_date")

    df = _read_partitioned_mart(PREFIX_SUBREDDIT_DAILY, start, end)
    if df.empty:
        return jsonify([])

    # Normalize column names as loaded from mart UNLOAD
    # Expected columns: dt, subreddit_id, subreddit_name_prefixed, posts, avg_positive, avg_negative,
    #                   avg_net_sentiment, median_net_sentiment
    if subreddit:
        df = df.loc[df["subreddit_name_prefixed"] == subreddit]
    if subreddit_id:
        df = df.loc[df["subreddit_id"].astype(str) == str(subreddit_id)]

    if df.empty:
        return jsonify([])

    df = df.sort_values("dt")
    out = [
        {
            "date": pd.to_datetime(r["dt"]).date().isoformat(),
            "subreddit_id": str(r["subreddit_id"]),
            "subreddit": r["subreddit_name_prefixed"],
            "posts": int(r["posts"]) if pd.notnull(r["posts"]) else 0,
            "avg_positive": float(r["avg_positive"]) if pd.notnull(r["avg_positive"]) else None,
            "avg_negative": float(r["avg_negative"]) if pd.notnull(r["avg_negative"]) else None,
            "avg_net_sentiment": float(r["avg_net_sentiment"]) if pd.notnull(r["avg_net_sentiment"]) else None,
            "median_net_sentiment": float(r["median_net_sentiment"]) if pd.notnull(r["median_net_sentiment"]) else None,
        }
        for _, r in df.iterrows()
    ]
    return jsonify(out)


@app.route("/sentiment/comments", methods=["GET"])
def sentiment_comments():
    """
    Retrieve *daily subreddit* comment sentiments from marts for a given date range.
    ---
    tags:
      - Subreddit Analytics (Marts)
    parameters:
      - name: subreddit
        in: query
        type: string
        required: true
        description: Subreddit name (e.g. r/cancer)
        example: r/cancer
      - name: start_date
        in: query
        type: string
        format: date
        required: false
        description: Start date (YYYY-MM-DD). Defaults to 7 days ago.
        example: 2025-08-01
      - name: end_date
        in: query
        type: string
        format: date
        required: false
        description: End date (YYYY-MM-DD). Defaults to today.
        example: 2025-08-31
    responses:
      200:
        description: Daily subreddit metrics (comments & comment sentiment)
        content:
          application/json:
            schema:
              type: array
              items:
                type: object
                properties:
                  date:
                    type: string
                  subreddit:
                    type: string
                  comments:
                    type: integer
                  avg_comment_net_sentiment:
                    type: number
    """
    subreddit = request.args.get("subreddit")       # e.g., "r/AskPH"
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # Defaults: last 7 days
    today = date.today()
    if not start_date:
        start = today - timedelta(days=7)
    else:
        start = _parse_date(start_date, "start_date")
    if not end_date:
        end = today
    else:
        end = _parse_date(end_date, "end_date")

    df = _read_partitioned_mart(PREFIX_COMMENTS_DAILY, start, end)
    if df.empty:
        return jsonify([])

    # Normalize column names as loaded from mart UNLOAD
    # Expected columns: dt, subreddit_name_prefixed, comments, avg_comment_net_sentiment
    if subreddit:
        df = df.loc[df["subreddit_name_prefixed"] == subreddit]

    if df.empty:
        return jsonify([])

    df = df.sort_values("dt")
    out = [
        {
            "date": pd.to_datetime(r["dt"]).date().isoformat(),
            "subreddit": r["subreddit_name_prefixed"],
            "comments": int(r["comments"]) if pd.notnull(r["comments"]) else 0,
            "avg_comment_net_sentiment": float(r["avg_comment_net_sentiment"]) if pd.notnull(r["avg_comment_net_sentiment"]) else None,
        }
        for _, r in df.iterrows()
    ]
    return jsonify(out)


@app.route("/sentiment/tags", methods=["GET"])
def sentiment_tags():
    """
    Retrieve *daily subreddit* tag sentiments from marts for a given date range.
    ---
    tags:
      - Subreddit Analytics (Marts)
    parameters:
      - name: start_date
        in: query
        type: string
        format: date
        required: false
        description: Start date (YYYY-MM-DD). Defaults to 7 days ago.
        example: 2025-08-01
      - name: end_date
        in: query
        type: string
        format: date
        required: false
        description: End date (YYYY-MM-DD). Defaults to today.
        example: 2025-08-31
    responses:
      200:
        description: Daily subreddit metrics (tags & tag sentiment)
        content:
          application/json:
            schema:
              type: array
              items:
                type: object
                properties:
                  date:
                    type: string
                  subreddit:
                    type: string
                  tag:
                    type: string
                  score:
                    type: integer
                  negative:
                    type: number
                  neutral:
                    type: number
                  positive:
                    type: number
                  net_sentiment:
                    type: number
    """
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # Defaults: last 7 days
    today = date.today()
    if not start_date:
        start = today - timedelta(days=7)
    else:
        start = _parse_date(start_date, "start_date")
    if not end_date:
        end = today
    else:
        end = _parse_date(end_date, "end_date")

    df = _read_partitioned_mart(PREFIX_TAGS_DAILY, start, end)
    if df.empty:
        return jsonify([])

    # Normalize column names as loaded from mart UNLOAD

    if df.empty:
        return jsonify([])

    df = df.sort_values("dt")
    out = [
        {
            "date": pd.to_datetime(r["dt"]).date().isoformat(),
            "subreddit": r["subreddit_name_prefixed"],
            "tag": r["tag_name"],
            "score": int(r["score"]) if pd.notnull(r["score"]) else 0,
            "negative": float(r["negative"]) if pd.notnull(r["negative"]) else None,
            "neutral": float(r["neutral"]) if pd.notnull(r["neutral"]) else None,
            "positive": float(r["positive"]) if pd.notnull(r["positive"]) else None,
            "net_sentiment": float(r["net_sentiment"]) if pd.notnull(r["net_sentiment"]) else None,
        }
        for _, r in df.iterrows()
    ]
    return jsonify(out)




if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=5000)
