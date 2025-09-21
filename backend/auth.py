# auth.py
import sqlite3
from pathlib import Path
from datetime import date

DB = Path("uploads") / "auth.db"
DB.parent.mkdir(parents=True, exist_ok=True)

def get_conn():
    con = sqlite3.connect(str(DB), timeout=10, isolation_level=None)
    return con

def init_db():
    con = get_conn()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS api_keys (
      id INTEGER PRIMARY KEY,
      company TEXT NOT NULL,
      api_key TEXT NOT NULL UNIQUE,
      daily_limit INTEGER DEFAULT 1000,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS usage (
      id INTEGER PRIMARY KEY,
      api_key TEXT NOT NULL,
      date TEXT NOT NULL,
      count INTEGER DEFAULT 0,
      UNIQUE(api_key, date)
    )
    """)
    con.commit()
    con.close()

def create_api_key(company: str, api_key: str, daily_limit: int = 1000):
    init_db()
    con = get_conn()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO api_keys (company, api_key, daily_limit) VALUES (?, ?, ?)",
                (company, api_key, daily_limit))
    con.commit()
    con.close()

def get_key_record(api_key: str):
    init_db()
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT company, api_key, daily_limit FROM api_keys WHERE api_key = ?", (api_key,))
    row = cur.fetchone()
    con.close()
    return row   # (company, api_key, daily_limit) or None

def increment_usage_and_check(api_key: str):
    """
    Increments usage for today. Returns (ok:bool, count:int, limit:int)
    """
    init_db()
    today = date.today().isoformat()
    con = get_conn()
    cur = con.cursor()
    # ensure a row exists
    try:
        cur.execute("INSERT INTO usage (api_key, date, count) VALUES (?, ?, 0)", (api_key, today))
    except sqlite3.IntegrityError:
        pass
    # increment by 1 and fetch new count
    cur.execute("UPDATE usage SET count = count + 1 WHERE api_key = ? AND date = ?", (api_key, today))
    cur.execute("SELECT count FROM usage WHERE api_key = ? AND date = ?", (api_key, today))
    row = cur.fetchone()
    count = row[0] if row else 0
    # retrieve limit
    cur.execute("SELECT daily_limit FROM api_keys WHERE api_key = ?", (api_key,))
    rl = cur.fetchone()
    limit = rl[0] if rl else None
    con.commit()
    con.close()
    if limit is None:
        return False, count, 0
    return (count <= limit), count, limit

# call once to create DB and a demo key if you want
if __name__ == "__main__":
    import sqlite3
    init_db()
    print("Init DB done")
