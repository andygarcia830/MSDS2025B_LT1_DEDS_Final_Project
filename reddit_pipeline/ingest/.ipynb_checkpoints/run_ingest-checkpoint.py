import os, json, time, gzip, io, datetime as dt
from pathlib import Path
from dotenv import load_dotenv
import boto3, praw
import psycopg2
import psycopg2.extras as pgx


# --- config ---
ROOT = Path(__file__).resolve().parents[1]
# ROOT = Path('/home/ubuntu/deds2025b_proj/opt/reddit_pipeline') # FOR NOTEBOOK ONLY
load_dotenv(ROOT / '.env')

BUCKET = os.environ['LAKE_BUCKET']
PREFIX = 'bronze/reddit'
REDDIT_SECRET_ARN = os.environ['REDDIT_SECRET_ARN']
RDS_SECRET_ARN = os.environ['REDDIT_RDS_ARN']
POSTGRES_DB = os.environ['RDS_DB']

s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')

# --- scrape settings ---
subreddits = [
    'LivingWithMBC', 'breastcancer', 'ovariancancer_new',
    'BRCA', 'cancer', 'IndustrialPharmacy'
]
keywords = [
    "olaparib","lynparza","parp","parpi","diagnosis","biopsy",
    "screening","mammogram","ultrasound","ct scan","brca test",
    "genetic testing","starting olaparib","maintenance","chemo",
    "bevacizumab","platinum","surgery","radiation","follow-up",
    "maintenance dose","long term","quality of life","support group",
    "ned","no evidence of disease","clear scan","partial response",
    "stable disease","tumor shrinkage","ca-125 down","responding",
    "anemia","fatigue","nausea","vomiting","diarrhea","constipation",
    "decreased appetite","headache","dizziness","rash","insomnia",
    "cough","asthenia","dyspnea","brca1","brca2","hrd",
    "homologous recombination deficiency","ovarian","breast","mbc",
    "tnbc","her2","hr+","philippines","ph","pinoy","filipino","manila",
    "cebu","davao","tagalog","qc","quezon city"
]
max_posts = 150
since_hours = 8760


# --- helper functions ---
def reddit_client():
    cfg = json.loads(secrets.get_secret_value(SecretId=REDDIT_SECRET_ARN)['SecretString'])
    return praw.Reddit(
        client_id=cfg['client_id'],
        client_secret=cfg['client_secret'],
        user_agent='aws:batch-ec2:1.0 (by u/Entire-Success-5370)'
    )

def get_rds_conn():
    """Create a psycopg2 connection from RDS secret."""
    scfg = json.loads(secrets.get_secret_value(SecretId=RDS_SECRET_ARN)['SecretString'])
    return psycopg2.connect(
        host=scfg['host'],
        port=scfg.get('port', 5432),
        user=scfg['username'],
        password=scfg['password'],
        dbname=scfg.get('dbname', POSTGRES_DB)
    )

def dump_jsonl_gz(objs, key):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        for o in objs:
            gz.write((json.dumps(o, ensure_ascii=False)+'\n').encode('utf-8'))
    buf.seek(0)
    s3.upload_fileobj(buf, BUCKET, key)

def premium_str(val):
    return 'premium' if bool(val) else 'not premium'

# --- UPSERT SQL ---
SQL_UPSERT_AUTHOR = """
INSERT INTO author (author_fullname, author, author_premium)
VALUES (%(author_fullname)s, %(author)s, %(author_premium)s)
ON CONFLICT (author_fullname) DO UPDATE
SET author = EXCLUDED.author,
    author_premium = EXCLUDED.author_premium;
"""

SQL_UPSERT_SUBREDDIT = """
INSERT INTO subreddit (subreddit_id, subreddit_name_prefixed, subreddit_type, subreddit_subscribers)
VALUES (%(subreddit_id)s, %(subreddit_name_prefixed)s, %(subreddit_type)s, %(subreddit_subscribers)s)
ON CONFLICT (subreddit_id) DO UPDATE
SET subreddit_name_prefixed = EXCLUDED.subreddit_name_prefixed,
    subreddit_type = EXCLUDED.subreddit_type,
    subreddit_subscribers = EXCLUDED.subreddit_subscribers;
"""

SQL_UPSERT_POST = """
INSERT INTO post (post_name, subreddit_id, author_fullname, title, selftext, score,
                  upvote_ratio, num_comments, url, created_utc)
VALUES (%(post_name)s, %(subreddit_id)s, %(author_fullname)s, %(title)s, %(selftext)s, %(score)s,
        %(upvote_ratio)s, %(num_comments)s, %(url)s, %(created_utc)s)
ON CONFLICT (post_name) DO UPDATE
SET score = EXCLUDED.score,
    upvote_ratio = EXCLUDED.upvote_ratio,
    num_comments = EXCLUDED.num_comments,
    title = COALESCE(EXCLUDED.title, post.title),
    selftext = COALESCE(EXCLUDED.selftext, post.selftext);
"""

SQL_UPSERT_COMMENT = """
INSERT INTO comment (comment_name, author_fullname, post_name, parent_comment_name,
                     body, score, created_utc)
VALUES (%(comment_name)s, %(author_fullname)s, %(post_name)s, %(parent_comment_name)s,
        %(body)s, %(score)s, %(created_utc)s)
ON CONFLICT (comment_name) DO UPDATE
SET score = EXCLUDED.score,
    body  = COALESCE(EXCLUDED.body, comment.body);
"""

def persist_to_rds(conn, posts, comments):
    if not posts and not comments:
        return
        
    authors = {}
    subreddits = {}
    post_rows = []
    comment_rows = []

    for p in posts:
        if p.get('author_fullname'):
            authors[p['author_fullname']] = {
                'author_fullname': p['author_fullname'],
                'author': p.get('author'),
                'author_premium': premium_str(p.get('author_premium'))
            }
        if p.get('subreddit_id'):
            subreddits[p['subreddit_id']] = {
                'subreddit_id': p['subreddit_id'],
                'subreddit_name_prefixed': p.get('subreddit_name_prefixed'),
                'subreddit_type': p.get('subreddit_type'),
                'subreddit_subscribers': p.get('subreddit_subscribers')
            }
        post_rows.append({
            'post_name': p['post_name'],
            'subreddit_id': p['subreddit_id'],
            'author_fullname': p.get('author_fullname'),
            'title': p.get('title'),
            'selftext': p.get('selftext'),
            'score': p.get('score'),
            'upvote_ratio': p.get('upvote_ratio'),
            'num_comments': p.get('num_comments'),
            'url': p.get('url') or '',
            'created_utc': int(p['created_utc'])
        })

    for c in comments:
        if c.get('author_fullname'):
            authors[c['author_fullname']] = {
                'author_fullname': c['author_fullname'],
                'author': c.get('author'),
                'author_premium': premium_str(c.get('author_premium'))
            }
        parent = c.get('parent_comment_name')
        if parent and parent.startswith('t3_'):
            parent = None
        comment_rows.append({
            'comment_name': c['comment_name'],
            'author_fullname': c.get('author_fullname'),
            'post_name': c['post_name'],
            'parent_comment_name': parent,
            'body': c.get('body'),
            'score': c.get('score'),
            'created_utc': int(c['created_utc'])
        })

    with conn, conn.cursor() as cur:
        if subreddits:
            pgx.execute_batch(cur, SQL_UPSERT_SUBREDDIT, list(subreddits.values()), page_size=100)
        if authors:
            pgx.execute_batch(cur, SQL_UPSERT_AUTHOR, list(authors.values()), page_size=200)
        if post_rows:
            pgx.execute_batch(cur, SQL_UPSERT_POST, post_rows, page_size=200)
        if comment_rows:
            pgx.execute_batch(cur, SQL_UPSERT_COMMENT, comment_rows, page_size=500)


def run(subs=subreddits, max_posts=50, since_hours=8760):
    rd = reddit_client()
    run_id = dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')
    dt_part = dt.datetime.now(dt.UTC).strftime('%Y-%m-%d')
    cutoff = time.time() - since_hours*3600

    conn = get_rds_conn()

    try:
        for sr in subs:
            posts, comments = [], []
            for p in rd.subreddit(sr).new(limit=None):
                if len(posts) >= max_posts or p.created_utc < cutoff:
                    break
                else:
                    print(f'Parsing post with id: {p.name}')
                    posts.append({
                        # for post table
                        'kind':'post',
                        'post_name':p.name,
                        'title': p.title,
                        'selftext': p.selftext or '',
                        'score': p.score,
                        'upvote_ratio': p.upvote_ratio,
                        'num_comments': p.num_comments,
                        'url': p.url or '',
                        'created_utc': p.created_utc,                        
                    
                        #for author table
                        'author_fullname': getattr(p, 'author_fullname', None),
                        'author': str(p.author).split("='")[-1].replace("')", ""),
                        'author_premium': getattr(p, 'author_premium', None),
                        
                        # for subreddit table
                        'subreddit_id':p.subreddit_id,
                        'subreddit_name_prefixed': str(p.subreddit_name_prefixed),
                        'subreddit_type': p.subreddit_type,
                        'subreddit_subscribers': p.subreddit_subscribers,
                    })
                    p.comments.replace_more(limit=None)
                    for c in p.comments.list():    
                        print(f'  - Parsing comment with id: {c.name}')
                        comments.append({
                            # for comment table
                            'kind':'comment',
                            'comment_name':c.name,
                            'post_name':c.link_id,
                            'parent_comment_name':c.parent_id,
                            'body':c.body or '',
                            'score':c.score,
                            'created_utc':c.created_utc,

                            # for author table
                            'author_fullname': getattr(c, 'author_fullname', None),
                            'author': str(c.author).split("='")[-1].replace("')", ""),
                            'author_premium': getattr(c, 'author_premium', None)
                        })
                    time.sleep(0.5)
    
            base = f'{PREFIX}/dt={dt_part}/subreddit={sr}/run_id={run_id}'
            if posts:
                dump_jsonl_gz(posts, f'{base}/posts.jsonl.gz')
            if comments:
                dump_jsonl_gz(comments, f'{base}/comments.jsonl.gz')

            persist_to_rds(conn, posts, comments)
            print(f'{sr}: Collected {len(posts)} posts and {len(comments)} comments')
        print('DONE', run_id)

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return dt_part


if __name__ == '__main__':
    max_posts = 50
    since_hours = 8760
    run(subreddits, max_posts, since_hours)
    