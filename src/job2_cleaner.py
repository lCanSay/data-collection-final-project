import json
import os
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv

from db_utils import init_db, get_conn

load_dotenv("/opt/airflow/.env")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def main():
    topic = os.getenv("KAFKA_TOPIC_RAW", "raw_events")
    group = os.getenv("KAFKA_CONSUMER_GROUP", "cleaner_hourly_v1")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    sqlite_path = os.getenv("SQLITE_PATH", "/opt/airflow/data/app.db")

    init_db(sqlite_path)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    rows = []
    ingested_at = now_iso()

    for msg in consumer:
        m = msg.value
        zone = m.get("zone")
        signal = m.get("signal")
        payload = m.get("payload", {})
        dt = payload.get("datetime")

        val = payload.get("value")
        if val is None:
            val = payload.get("carbonIntensity")

        unit = payload.get("unit")
        if unit is None and signal == "carbon_intensity":
            unit = "gCO2eq/kWh"

        is_est = payload.get("isEstimated")
        is_est = int(bool(is_est)) if is_est is not None else 0  # missing -> 0

        est_method = payload.get("estimationMethod") or "NOT_ESTIMATED"

        rows.append({
            "zone": zone,
            "datetime": dt,
            "signal": signal,
            "value": val,
            "unit": unit,
            "is_estimated": is_est,
            "estimation_method": est_method,
            "queried_at": payload.get("updatedAt") or payload.get("createdAt") or ingested_at,
            "ingested_at": ingested_at,
            "raw_json": json.dumps(m),
        })

    print(f"[job2] consumed messages: {len(rows)}", flush=True)
    if not rows:
        consumer.close()
        return

    df = pd.DataFrame(rows)

    # ---- cleaning ----
    # normalize datetime to UTC ISO string
    dt_parsed = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df["datetime"] = dt_parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # numeric + rounding
    df["value"] = pd.to_numeric(df["value"], errors="coerce").round(2)

    # fill + types
    df["is_estimated"] = pd.to_numeric(df["is_estimated"], errors="coerce").fillna(0).astype(int)
    df["estimation_method"] = df["estimation_method"].fillna("NOT_ESTIMATED")

    # drop invalid rows
    df = df.dropna(subset=["zone", "datetime", "signal", "value"])

    print(f"[job2] rows after cleaning: {len(df)}", flush=True)
    if df.empty:
        consumer.commit()
        consumer.close()
        return

    # ---- write to sqlite with UNIQUE(zone, datetime, signal) ----
    conn = get_conn(sqlite_path)
    cur = conn.cursor()

    records = df.to_dict("records")
    cur.executemany("""
      INSERT OR IGNORE INTO events
      (zone, datetime, signal, value, unit, is_estimated, estimation_method, queried_at, ingested_at, raw_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (r["zone"], r["datetime"], r["signal"], float(r["value"]), r["unit"],
         int(r["is_estimated"]), r["estimation_method"], r["queried_at"], r["ingested_at"], r["raw_json"])
        for r in records
    ])

    conn.commit()
    conn.close()

    consumer.commit()
    consumer.close()


if __name__ == "__main__":
    main()
