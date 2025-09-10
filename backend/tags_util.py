# tags_util.py
import sqlite3
from pathlib import Path
import random
import json

DB_PATH = Path("uploads") / "tags.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def init_db():
    con = sqlite3 = sqlite3_connect()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS file_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT UNIQUE,
        tags TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    con.commit()
    con.close()

def sqlite3_connect():
    return sqlite3.connect(str(DB_PATH), timeout=10)

def add_random_tags_for_file(relative_path: str, min_tags=1, max_tags=4):
    """
    Adds 1..4 random tags for a given file path (relative to uploads/)
    If already exists, it overwrites with a new random selection.
    """
    init_db()
    sample_tags = [
      "portrait","blurry","out-of-focus","text","document","dark","low-light",
      "bright","overexposed","underexposed","contains-face","landscape","partial"
    ]
    count = random.randint(min_tags, max_tags)
    tags = random.sample(sample_tags, count)
    tags_json = json.dumps(tags)

    con = sqlite3_connect()
    cur = con.cursor()
    cur.execute("""
      INSERT INTO file_tags (path, tags, updated_at)
      VALUES (?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(path) DO UPDATE SET tags=excluded.tags, updated_at=CURRENT_TIMESTAMP
    """, (relative_path, tags_json))
    con.commit()
    con.close()
    return tags

def get_tags(relative_path: str):
    init_db()
    con = sqlite3_connect()
    cur = con.cursor()
    cur.execute("SELECT tags FROM file_tags WHERE path = ?", (relative_path,))
    row = cur.fetchone()
    con.close()
    if not row:
        return []
    import json
    return json.loads(row[0])
