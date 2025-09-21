# logs_util.py
import sqlite3
from pathlib import Path
from datetime import datetime
import os

if os.getenv("DB_TYPE", "sqlite").lower() == "postgres":
    from auth_sql import insert_log as _pg_insert_log, query_logs as _pg_query_logs
    def log_event(level, path, message, company=None, survey=None, filename=None, meta=None):
        try:
            _pg_insert_log(level, path, message, company=company, survey=survey, filename=filename, meta=meta)
        except Exception:
            # fallback: print to stdout if DB write fails
            print("log_event fallback:", level, path, message)
    def read_logs(limit=100, company=None, survey=None):
        return _pg_query_logs(limit=limit, company=company, survey=survey)

else:
    LOG_DB = Path("uploads") / "logs.db"
    LOG_DB.parent.mkdir(parents=True, exist_ok=True)

    def get_conn():
        return sqlite3.connect(str(LOG_DB), timeout=10)

    def init_logs_db():
        con = get_conn()
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT,
        level TEXT,
        endpoint TEXT,
        company TEXT,
        survey TEXT,
        filename TEXT,
        message TEXT
        )
        """)
        con.commit()
        con.close()

    def log_event(level: str, endpoint: str, message: str, company: str = None, survey: str = None, filename: str = None):
        """
        level: INFO / WARN / ERROR
        endpoint: e.g. '/api/v1/{company}/surveys/{survey}/upload'
        message: short message or error text
        """
        con = get_conn()
        cur = con.cursor()
        cur.execute(
            "INSERT INTO logs (ts, level, endpoint, company, survey, filename, message) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (datetime.utcnow().isoformat(), level, endpoint, company, survey, filename, message)
        )
        con.commit()
        con.close()
