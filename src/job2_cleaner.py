import os, json, time
import pandas as pd
from kafka import KafkaConsumer
from dotenv import load_dotenv
from db_utils import init_db, get_engine

load_dotenv("/opt/airflow/.env")

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: implement data cleaning (must be pandas/SQL per rules)
    return df

def main():
    topic = os.getenv("KAFKA_TOPIC_RAW", "raw_events")
    group = os.getenv("KAFKA_CONSUMER_GROUP", "cleaner_hourly_v1")
    sqlite_path = os.getenv("SQLITE_PATH", "/opt/airflow/data/app.db")

    init_db(sqlite_path)
    eng = get_engine(sqlite_path)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="latest",  #first run: start at end (new only)
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=5000,     #stop after 5s idle
    )

    rows = []
    for msg in consumer:
        rows.append({
            "event_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "payload_json": json.dumps(msg.value),
        })

    if rows:
        df = pd.DataFrame(rows)
        df = clean_df(df)
        df.to_sql("events", eng, if_exists="append", index=False)
        consumer.commit()

    consumer.close()

if __name__ == "__main__":
    main()
