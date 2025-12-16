import os
import sqlite3

def get_conn(sqlite_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db(sqlite_path: str) -> None:
    conn = get_conn(sqlite_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
      zone TEXT NOT NULL,
      datetime TEXT NOT NULL,
      signal TEXT NOT NULL,
      value REAL,
      unit TEXT,
      is_estimated INTEGER DEFAULT 0,
      estimation_method TEXT,
      queried_at TEXT,
      ingested_at TEXT,
      raw_json TEXT,
      UNIQUE(zone, datetime, signal)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_summary (
      day TEXT NOT NULL,
      zone TEXT NOT NULL,
      signal TEXT NOT NULL,
      count INTEGER NOT NULL,
      min REAL,
      max REAL,
      avg REAL,
      unit TEXT,
      computed_at TEXT,
      UNIQUE(day, zone, signal)
    );
    """)

    conn.commit()
    conn.close()
