from sqlalchemy import create_engine, text
import os

def get_engine(sqlite_path: str):
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    return create_engine(f"sqlite:///{sqlite_path}", future=True)

def init_db(sqlite_path: str):
    eng = get_engine(sqlite_path)
    with eng.begin() as conn:
        conn.execute(text("""
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
        """))
