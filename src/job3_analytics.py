import os, json
import pandas as pd
from dotenv import load_dotenv
from db_utils import init_db, get_engine

load_dotenv("/opt/airflow/.env")

def compute_summary(events_df: pd.DataFrame) -> dict:
    # TODO: implement real analytics (pandas/SQL)
    return {"rows": int(len(events_df))}

def main():
    sqlite_path = os.getenv("SQLITE_PATH", "/opt/airflow/data/app.db")
    init_db(sqlite_path)
    eng = get_engine(sqlite_path)

    events = pd.read_sql("SELECT * FROM events", eng)
    summary = compute_summary(events)

    # one row per day, simple example
    day = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    out = pd.DataFrame([{"day": day, "metric_json": json.dumps(summary)}])
    out.to_sql("daily_summary", eng, if_exists="append", index=False)

if __name__ == "__main__":
    main()
