import praw, json, boto3, sys, os
from dotenv import load_dotenv
from pathlib import Path
import boto3
from datetime import datetime
import datetime as dt
from boto3.dynamodb.conditions import Key
from flask_restx import Api, Resource


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / '.env')
REDDIT_SECRET_ARN = os.environ['REDDIT_SECRET_ARN']
secrets = boto3.client('secretsmanager')

def reddit_client():
    cfg = json.loads(secrets.get_secret_value(SecretId=REDDIT_SECRET_ARN)['SecretString'])
    return praw.Reddit(
        client_id=cfg['client_id'],
        client_secret=cfg['client_secret'],
        user_agent='aws:batch-ec2:1.0 (by u/Entire-Success-5370)'
    )

def dynamodb_client():
    return boto3.resource("dynamodb", region_name="us-east-1")
    


def run(subs):
    rd = reddit_client()
    dy = dynamodb_client()
    table = dy.Table("reddit")
    
    now_hour = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
    iso_hour = now_hour.strftime("%Y-%m-%dT%H:%M:%SZ")
    for item in subs:
        subreddit = rd.subreddit(item)
        print(f"r/{item} has {subreddit.subscribers:,} subscribers")
    
        table.put_item(
            Item={
                "PK":f"SUBREDDIT:{item}",
                "SK":iso_hour,
                "timestamp": str(dt.datetime.now(dt.UTC)),  # ISO8601 string
                "subscribers": subreddit.subscribers,
                "active_users": subreddit.active_user_count,
            }
        )

# Helper Function To Retrieve Entries

def get_dynamo_entries(sub, start_time=None, end_time=None):
    if start_time is None:
        now = dt.datetime.now(dt.UTC)
        start_time = now.replace(minute=0, second=0, microsecond=0)
    if end_time is None:
        end_time = dt.datetime.now(dt.UTC)
    end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    dy = dynamodb_client()
    table = dy.Table("reddit")

    pk = f"SUBREDDIT:{sub}"
    response = table.query(
        KeyConditionExpression=Key("PK").eq(pk) &
                               Key("SK").between(start_time, end_time)
    )

    return response.get("Items", [])


if __name__ == '__main__':
    subs = ['LivingWithMBC', 'breastcancer', 'ovariancancer_new',
                'BRCA', 'cancer', 'IndustrialPharmacy'
            ]
    run(subs)
