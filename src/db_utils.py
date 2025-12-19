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

        value REAL NOT NULL,
        unit TEXT NOT NULL,

        is_estimated INTEGER NOT NULL DEFAULT 0,
        estimation_method TEXT NOT NULL DEFAULT 'NOT_ESTIMATED',

        queried_at TEXT NOT NULL,
        ingested_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),

        raw_json TEXT NOT NULL,

        UNIQUE(zone, datetime, signal),
      );
    """)

    cur.execute("""
      CREATE TABLE IF NOT EXISTS daily_summary (
        day TEXT NOT NULL,
        zone TEXT NOT NULL,
        signal TEXT NOT NULL,

        count INTEGER NOT NULL,
        min REAL NOT NULL,
        max REAL NOT NULL,
        avg REAL NOT NULL,
        unit TEXT NOT NULL,

        computed_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),

        UNIQUE(day, zone, signal),
      );
    """)

    conn.commit()
    conn.close()
