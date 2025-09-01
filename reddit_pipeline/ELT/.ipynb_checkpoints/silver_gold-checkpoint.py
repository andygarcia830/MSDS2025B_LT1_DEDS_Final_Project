import boto3
import pandas as pd
import os, io, json, gzip, re
import datetime as dt
from pathlib import Path
from dotenv import load_dotenv

from transformers import AutoModelForSequenceClassification
from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
import torch
import numpy as np

import pyarrow as pa, pyarrow.parquet as pq


# --- config ---
ROOT = Path(__file__).resolve().parents[1]
# ROOT = Path("/home/ubuntu/deds2025b_proj/opt/reddit_pipeline")    # FOR NOTEBOOK ONLY
load_dotenv(ROOT / ".env")

BUCKET = os.environ["LAKE_BUCKET"]
s3 = boto3.client("s3")
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# --- tag list ---
KEYWORDS = [
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
                      

# --- tagging logic ---
def compile_tag_patterns(keywords):
    """
    Build (keyword, regex) pairs that:
      - are case-insensitive
      - allow spaces OR hyphens between words
      - respect word boundaries at the ends
    """
    re_exp = r'[\s\-]+'
    patterns = []
    for kw in keywords:
        toks = [t for t in re.split(re_exp, kw.strip()) if t]
        if not toks:
            continue
        body = re_exp.join(map(re.escape, toks))  # escape special chars in each token
        pat = re.compile(rf'(?<!\w){body}(?!\w)', re.IGNORECASE)
        patterns.append((kw, pat))
    return patterns

_PATTERNS = compile_tag_patterns(KEYWORDS)

def find_tags_in_text(text: str):
    """Returns unique set of tags found in text"""
    if not text:
        return []
    return list({kw for kw, pat in _PATTERNS if pat.search(text)})


# --- scoring model ---
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
_tokenizer = None
_model = None
_config = None

def load_model():
    global _tokenizer, _model, _config
    if _tokenizer is None or _model is None or _config is None:
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        _config = AutoConfig.from_pretrained(MODEL_NAME)
        _model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME).to(DEVICE)
        _model.eval()
    return _tokenizer, _model, _config

@torch.no_grad()
def score_texts(texts, batch_size=32, max_length=512):
    """
    Batched sentiment scoring.
    Returns a DataFrame with columns: negative_s, neutral_s, positive_s, sentiment_label
    """
    tokenizer, model, config = load_model()
    negs, neuts, poss, labels = [], [], [], []

    for i in range(0, len(texts), batch_size):
        chunk = texts[i:i+batch_size]
        enc = tokenizer(
            chunk,
            return_tensors='pt',
            padding=True,
            truncation=True,
            max_length=max_length
        ).to(DEVICE)

        logits = model(**enc).logits
        probs = torch.softmax(logits, dim=1).cpu().numpy()  # (B,3)

        # Map argmax to label via config
        argmax = probs.argmax(axis=1)
        lbls = [config.id2label[int(ix)] for ix in argmax]

        negs.extend(probs[:, 0].tolist())
        neuts.extend(probs[:, 1].tolist())
        poss.extend(probs[:, 2].tolist())
        labels.extend(lbls)

    return pd.DataFrame({
        "negative_s": negs,
        "neutral_s": neuts,
        "positive_s": poss,
        "sentiment_label": labels
    })


# --- MAIN ---
def tag_and_score(date_str: str, write_gold: bool = True):
    """
    Reads Silver posts & comments for dt={date_str}, adds tags and sentiment,
    writes to Gold partitioned by dt/subreddit.
    """
    print("Silver -> Gold")
    # Paths
    posts_prefix    = f"silver/reddit/posts/dt={date_str}/"
    comments_prefix = f"silver/reddit/comments/dt={date_str}/"

    # --- POSTS ---
    post_keys = [k for k in list_keys(posts_prefix) if k.endswith(".parquet")]
    post_frames = []
    for key in post_keys:
        m = re.search(r"/subreddit=([^/]+)/", key)
        print(f"Tagging and scoring posts from {m}")
        df = read_parquet_to_df(key)
        df["subreddit"] = m.group(1) if m else ""
        # Build a combined text field for matching (title: selftext)
        combined = (df["title"].fillna("") + ": " + df["selftext"].fillna(""))
        df["tags"] = combined.apply(find_tags_in_text)
        scores_df = score_texts(combined.tolist())
        df = pd.concat([df, scores_df], axis=1)
        df["scored_at"] = pd.Timestamp.now(tz="UTC")
        post_frames.append(df)
    posts_tagged = pd.concat(post_frames, ignore_index=True) if post_frames else pd.DataFrame()

    # --- COMMENTS ---
    comment_keys = [k for k in list_keys(comments_prefix) if k.endswith(".parquet")]
    comment_frames = []
    for key in comment_keys:
        m = re.search(r"/subreddit=([^/]+)/", key)
        print(f"Tagging and scoring comments from {m}")
        df = read_parquet_to_df(key)
        df["subreddit"] = m.group(1) if m else ""
        df["tags"] = df["body"].fillna("").apply(find_tags_in_text)
        scores_df = score_texts(df["body"].fillna("").tolist())
        df = pd.concat([df, scores_df], axis=1)
        df["scored_at"] = pd.Timestamp.now(tz="UTC")
        comment_frames.append(df)
    comments_tagged = pd.concat(comment_frames, ignore_index=True) if comment_frames else pd.DataFrame()

    # write to Gold (same partitioning: dt + subreddit)
    if write_gold and not posts_tagged.empty:
        print(f"Writing posts to lake (gold)")
        for sr, df_sr in posts_tagged.groupby("subreddit"):
            out_key = f"gold/reddit/post_enriched/dt={date_str}/subreddit={sr}/part-0.parquet"
            write_parquet(df_sr.drop(columns=["subreddit"]), out_key)

    if write_gold and not comments_tagged.empty:
        print(f"Writing comments to lake (gold)")
        for sr, df_sr in comments_tagged.groupby("subreddit"):
            out_key = f"gold/reddit/comment_enriched/dt={date_str}/subreddit={sr}/part-0.parquet"
            write_parquet(df_sr.drop(columns=["subreddit"]), out_key)

    return date_str


if __name__ == "__main__":
    date_str = "2025-08-29"
    tag_and_score(date_str, write_gold=True)