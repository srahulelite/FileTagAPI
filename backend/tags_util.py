# backend/tags_util.py
import os
import random
import json
from pathlib import Path

# sqlite helpers (used when DB_TYPE != "postgres")
import sqlite3

DB_PATH = Path("uploads") / "tags.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def sqlite3_connect():
    return sqlite3.connect(str(DB_PATH), timeout=10)


def init_db_sqlite():
    con = sqlite3_connect()
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


# Runtime dispatch: use Postgres adapter when DB_TYPE=postgres, else sqlite
if os.getenv("DB_TYPE", "sqlite").lower() == "postgres":
    # import Postgres-backed functions from auth_sql
    from auth_sql import add_tag as _pg_add_tag, get_tags_for_file as _pg_get_tags_for_file, remove_tag as _pg_remove_tag, add_random_tags_for_file as _pg_add_random, get_tags as _pg_get_tags

    def add_tag_for_file(company, survey, filepath_or_filename, tag):
        """
        Convenience wrapper to add one tag using Postgres adapter.
        """
        fname = Path(filepath_or_filename).name
        return _pg_add_tag(company, survey, fname, tag)

    def add_random_tags_for_file(relative_path: str, min_tags=1, max_tags=4):
        """
        Backwards-compatible: call Postgres wrapper that expects relative path.
        """
        return _pg_add_random(relative_path, min_tags=min_tags, max_tags=max_tags)

    def get_tags(path_or_gs_path):
        """
        Backwards-compatible get_tags: accept uploads/... or gs://... and delegate to Postgres.
        """
        return _pg_get_tags(path_or_gs_path)

    def remove_tag_for_file(company, survey, filename, tag):
        return _pg_remove_tag(company, survey, filename, tag)

else:
    # SQLite-backed implementations (local/dev)
    def add_random_tags_for_file(relative_path: str, min_tags=1, max_tags=4):
        """
        Adds 1..4 random tags for a given file path (relative to uploads/)
        If already exists, it overwrites with a new random selection.
        """
        init_db_sqlite()
        sample_tags = [
            "portrait","blurry","out-of-focus","text","document","dark","low-light",
            "bright","overexposed","underexposed","contains-face","landscape","partial"
        ]
        count = random.randint(min_tags, max_tags)
        tags = random.sample(sample_tags, count)
        tags_json = json.dumps(tags)

        con = sqlite3_connect()
        cur = con.cursor()
        # SQLite UPSERT using ON CONFLICT
        cur.execute("""
        INSERT INTO file_tags (path, tags, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(path) DO UPDATE SET tags=excluded.tags, updated_at=CURRENT_TIMESTAMP
        """, (relative_path, tags_json))
        con.commit()
        con.close()
        return tags

    def get_tags(relative_path: str):
        init_db_sqlite()
        con = sqlite3_connect()
        cur = con.cursor()
        cur.execute("SELECT tags FROM file_tags WHERE path = ? COLLATE NOCASE", (relative_path,))
        row = cur.fetchone()
        con.close()
        if not row:
            # no tags -> return empty list
            return []
        return json.loads(row[0])

    def remove_tag_for_file(company, survey, filename, tag):
        init_db_sqlite()
        con = sqlite3_connect()
        cur = con.cursor()
        # reconstruct path as used earlier: uploads/{company}/{survey}/{filename}
        path = str(Path("uploads") / company / survey / filename)
        cur.execute("DELETE FROM file_tags WHERE path = ?", (path,))
        con.commit()
        con.close()
