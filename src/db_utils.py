import os
from sqlalchemy import create_engine, text

def get_engine(sqlite_path: str):
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    return create_engine(f"sqlite:///{sqlite_path}", future=True)


# Change this later after we know db structure
def init_db(sqlite_path: str):
    eng = get_engine(sqlite_path)
    with eng.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_time TEXT,
          payload_json TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS daily_summary (
          day TEXT PRIMARY KEY,
          metric_json TEXT
        );
        """))
