import json, os, sqlite3
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def main():
    topic = os.getenv("KAFKA_TOPIC_RAW", "raw_events")
    group = os.getenv("KAFKA_CONSUMER_GROUP", "cleaner_hourly_v1")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    sqlite_path = os.getenv("SQLITE_PATH", "/opt/airflow/data/app.db")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group,
        # Use earliest so a new consumer group reads the backlog on first run.
        auto_offset_reset="earliest",
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

        rows.append({
            "zone": zone,
            "datetime": dt,
            "signal": signal,
            "value": val,
            "unit": unit,
            "is_estimated": int(bool(payload.get("isEstimated"))) if "isEstimated" in payload else None,
            "estimation_method": payload.get("estimationMethod"),
            "queried_at": payload.get("updatedAt") or payload.get("createdAt") or ingested_at,
            "ingested_at": ingested_at,
            "raw_json": json.dumps(m),
        })

    print(f"[job2] consumed messages: {len(rows)}", flush=True)
    if not rows:
        consumer.close()
        return

    df = pd.DataFrame(rows)

    # cleaning
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["zone", "datetime", "signal", "value"])

    print(f"[job2] rows after cleaning: {len(df)}", flush=True)
    if df.empty:
        # We still advance offsets so these malformed/duplicate rows are not reprocessed forever.
        consumer.commit()
        consumer.close()
        return

    # write to sqlite
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
      zone TEXT NOT NULL,
      datetime TEXT NOT NULL,
      signal TEXT NOT NULL,
      value REAL,
      unit TEXT,
      is_estimated INTEGER,
      estimation_method TEXT,
      queried_at TEXT,
      ingested_at TEXT,
      raw_json TEXT,
      UNIQUE(zone, datetime, signal)
    );
    """)

    records = df.to_dict("records")
    cur.executemany("""
      INSERT OR IGNORE INTO events
      (zone, datetime, signal, value, unit, is_estimated, estimation_method, queried_at, ingested_at, raw_json)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (r["zone"], r["datetime"], r["signal"], r["value"], r["unit"],
         r["is_estimated"], r["estimation_method"], r["queried_at"], r["ingested_at"], r["raw_json"])
        for r in records
    ])

    conn.commit()
    conn.close()

    consumer.commit()
    consumer.close()

if __name__ == "__main__":
    main()
