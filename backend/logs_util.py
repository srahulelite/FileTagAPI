# logs_util.py
import sqlite3
from pathlib import Path
from datetime import datetime

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
