from __future__ import annotations

import streamlit as st
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_URL = "http://34.205.73.198:5000/interest"  # your endpoint
TIMEOUT = 15  # seconds


SUBS = ['LivingWithMBC', 'breastcancer', 'ovariancancer_new', 'BRCA', 'cancer', 'IndustrialPharmacy']


# ---------- internal utilities ----------
def _floor_to_hour(t: dt.datetime) -> dt.datetime:
    return t.replace(minute=0, second=0, microsecond=0)


def last_complete_hour_utc() -> dt.datetime:
    now_utc = _utcnow()
    return now_utc.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=3)

def utc_window_for_ph_day(day_ph: Optional[dt.date] = None) -> tuple[dt.datetime, dt.datetime]:
    """
    Convert a PH calendar day into an equivalent UTC [start,end] window.
    Useful if you need 'today (PH)' in UTC.
    """
    if day_ph is None:
        day_ph = dt.datetime.now(PH_TZ).date()
    start_ph = dt.datetime.combine(day_ph, dt.time(0, 0, 0), tzinfo=PH_TZ)
    end_ph = start_ph + dt.timedelta(days=1) - dt.timedelta(seconds=1)
    return (
        start_ph.astimezone(dt.timezone.utc),
        end_ph.astimezone(dt.timezone.utc),
    )
    
def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_iso_z(t: dt.datetime) -> str:
    """Return ISO8601 with Z suffix, always UTC."""
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_session() -> requests.Session:
    """Requests session with sane retries for POST."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"POST"},
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _unwrap_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # common containers
        for k in ("data", "items", "results", "rows", "records"):
            if k in payload and isinstance(payload[k], list):
                return payload[k]
        # looks like a single record => wrap it
        if {"PK", "SK"}.issubset(payload.keys()) or "timestamp" in payload:
            return [payload]
    # fallback: empty (don’t raise; we gracefully show “—”)
    return []


def _parse_ts(s: Any) -> Optional[dt.datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _pick_value(rec: Dict[str, Any], prefer: Optional[str] = None) -> Any:
    if rec is None:
        return None
    if prefer and prefer in rec:
        return _num(rec[prefer])
    for k in ("active_users", "subscribers", "interest", "value", "score", "count"):
        if k in rec:
            return _num(rec[k])
    # last resort: first numeric field
    for k, v in rec.items():
        if k.lower() not in {"timestamp", "time", "ts", "datetime", "sk", "pk"}:
            nv = _num(v)
            if nv is not None:
                return nv
    return None

def _num(x):
    try:
        # handle "3682" -> 3682, "1" -> 1, "3.2" -> 3.2
        if isinstance(x, (int, float)):
            return x
        s = str(x).strip()
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
        return float(s)
    except Exception:
        return None

def _record_time(rec: Dict[str, Any]) -> Optional[dt.datetime]:
    # SK like "2025-09-01T11:00:00Z"
    sk = rec.get("SK")
    if isinstance(sk, str):
        t = _parse_ts(sk)
        if t:
            return t
    # fallback to timestamp/time
    ts = rec.get("timestamp") or rec.get("time") or rec.get("ts") or rec.get("datetime")
    return _parse_ts(ts)


# ---------- public functions ----------
def fetch_interest_window(
    sub: str,
    start: dt.datetime,
    end: dt.datetime,
    base_url: str = API_URL,
    session: Optional[requests.Session] = None,
    headers: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Fetch raw hourly records for a subreddit in a time window."""
    payload = {"sub": sub, "start_time": _to_iso_z(start), "end_time": _to_iso_z(end)}
    s = session or _new_session()
    r = s.post(base_url, json=payload, headers=headers, timeout=TIMEOUT)
    r.raise_for_status()
    return _unwrap_items(r.json())


def latest_point_and_prev(records: List[Dict[str, Any]]):
    """
    Pick the latest record for the latest hour bucket, and the latest record for the previous hour bucket.
    Works even if your API returns multiple rows within the same hour.
    """
    by_hour: Dict[dt.datetime, Tuple[dt.datetime, Dict[str, Any]]] = {}
    for rec in records:
        t = _record_time(rec)
        if not t:
            continue
        bucket = _floor_to_hour(t)
        # keep the latest-in-hour record
        prev = by_hour.get(bucket)
        if (prev is None) or (t > prev[0]):
            by_hour[bucket] = (t, rec)

    if not by_hour:
        return None, None

    # sort hour buckets desc and take top 2
    hours_sorted = sorted(by_hour.items(), key=lambda kv: kv[0], reverse=True)
    latest = hours_sorted[0][1][1]
    prev = hours_sorted[1][1][1] if len(hours_sorted) > 1 else None
    return latest, prev

def _hour_bucket_range(hour_utc: dt.datetime) -> tuple[dt.datetime, dt.datetime]:
    start = _floor_to_hour(hour_utc)
    end = start + dt.timedelta(minutes=59, seconds=59)
    return start, end

def fetch_hour_bucket(
    sub: str,
    hour_bucket_utc: dt.datetime,
    base_url: str = API_URL,
    session: Optional[requests.Session] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Fetch the latest record *within a specific hour bucket*."""
    start, end = _hour_bucket_range(hour_bucket_utc)
    recs = fetch_interest_window(sub, start, end, base_url=base_url, session=session, headers=headers)

    # keep the latest record inside the bucket
    best_t, best = None, None
    for rec in recs:
        t = _record_time(rec)
        if not t or _floor_to_hour(t) != _floor_to_hour(start):
            continue
        if best_t is None or t > best_t:
            best_t, best = t, rec
    return best


def to_ph_time(ts: pd.Timestamp) -> Optional[str]:
    """Format a UTC timestamp for Asia/Manila display."""
    if pd.isna(ts):
        return None
    return ts.tz_convert("Asia/Manila").strftime("%Y-%m-%d %H:%M")


def fetch_latest_interests(
    subs: Sequence[str],
    base_url: str = API_URL,
    headers: Optional[Dict[str, str]] = None,
    metric_key: str = "active_users",
    prev_mode: str = "prev_day_same_hour",  # "prev_hour" or "prev_day_same_hour"
) -> pd.DataFrame:
    latest_hour = last_complete_hour_utc()
    prev_hour = (
        latest_hour - dt.timedelta(days=1)
        if prev_mode == "prev_day_same_hour"
        else latest_hour - dt.timedelta(hours=1)
    )

    out = []
    with _new_session() as s:
        for sub in subs:
            try:
                latest = fetch_hour_bucket(sub, latest_hour, base_url=base_url, session=s, headers=headers)
                prev = fetch_hour_bucket(sub, prev_hour, base_url=base_url, session=s, headers=headers)

                if latest is None:
                    out.append({"sub": sub, "timestamp": pd.NaT, "value": None, "prev_value": None, "delta": None, "raw": None})
                    continue

                ts = pd.to_datetime((latest.get("SK") or latest.get("timestamp")), errors="coerce", utc=True)
                val = _pick_value(latest, prefer=metric_key)
                prev_val = _pick_value(prev, prefer=metric_key) if prev else None
                delta = None if (val is None or prev_val is None) else (val - prev_val)

                out.append({
                    "sub": sub,
                    "timestamp": ts,           # latest hour (UTC)
                    "value": val,
                    "prev_value": prev_val,    # same hour yesterday (UTC)
                    "delta": delta,
                    "raw": latest
                })
            except Exception as e:
                out.append({"sub": sub, "timestamp": pd.NaT, "value": None, "prev_value": None, "delta": None, "raw": {"error": str(e)}})

    return pd.DataFrame(out)

from urllib.parse import urljoin
API_BASE = API_URL.rsplit("/", 1)[0]  # "http://34.205.73.198:5000"

def _new_get_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def _get_json(path: str, params: Dict[str, Any]) -> Any:
    url = urljoin(API_BASE + "/", path.lstrip("/"))
    with _new_get_session() as s:
        r = s.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()

def fetch_posts_marts(subreddit_prefixed: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """
    subreddit_prefixed like 'r/cancer'
    """
    data = _get_json("/sentiment/posts", {
        "subreddit": subreddit_prefixed,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    })
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")

def fetch_comments_marts(subreddit_prefixed: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    data = _get_json("/sentiment/comments", {
        "subreddit": subreddit_prefixed,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    })
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")

def fetch_tags_marts(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    # returns all subreddits; filter client-side
    data = _get_json("/sentiment/tags", {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    })
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")