import os
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv

from db_utils import init_db, get_conn

load_dotenv("/opt/airflow/.env")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def main():
    sqlite_path = os.getenv("SQLITE_PATH", "/opt/airflow/data/app.db")
    computed_at = now_iso()

    init_db(sqlite_path)
    conn = get_conn(sqlite_path)

    df = pd.read_sql_query(
        "SELECT zone, datetime, signal, value, unit FROM events",
        conn
    )

    if df.empty:
        conn.close()
        return

    df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df = df.dropna(subset=["datetime", "zone", "signal", "value"])
    df["day"] = df["datetime"].dt.strftime("%Y-%m-%d")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    agg = (
        df.groupby(["day", "zone", "signal"], as_index=False)
          .agg(
              count=("value", "count"),
              min=("value", "min"),
              max=("value", "max"),
              avg=("value", "mean"),
              unit=("unit", "first"),
          )
    )

    # rounding
    agg["min"] = agg["min"].round(2)
    agg["max"] = agg["max"].round(2)
    agg["avg"] = agg["avg"].round(2)
    agg["computed_at"] = computed_at

    rows = agg.to_dict("records")
    cur = conn.cursor()
    cur.executemany("""
      INSERT OR IGNORE INTO daily_summary
      (day, zone, signal, count, min, max, avg, unit, computed_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (r["day"], r["zone"], r["signal"], int(r["count"]),
         float(r["min"]) if pd.notna(r["min"]) else None,
         float(r["max"]) if pd.notna(r["max"]) else None,
         float(r["avg"]) if pd.notna(r["avg"]) else None,
         r["unit"], r["computed_at"])
        for r in rows
    ])

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
