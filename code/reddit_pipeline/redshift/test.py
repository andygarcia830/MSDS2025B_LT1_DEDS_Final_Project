from datetime import timedelta
import pendulum

from lake_olap import get_redshift_conn
from marts import export_marts_for_date

def main():
    # Use your working timezone; adjust if your warehouse expects UTC
    today = pendulum.datetime(2025, 9, 1, tz="UTC")
    # inclusive 365 days (today counts as day 365)
    start = today - timedelta(days=364)

    conn = get_redshift_conn()
    try:
        d = start
        while d <= today:
            ds = d.strftime("%Y-%m-%d")
            print(f"[backfill] exporting marts for {ds} ...")
            export_marts_for_date(conn, ds)
            d += timedelta(days=1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
