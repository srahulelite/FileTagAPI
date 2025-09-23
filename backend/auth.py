# auth.py
"""
Postgres-only auth module (CloudSQL-ready).
Exposes the API used by app.py:
 - init_db()
 - create_api_key(company, api_key, daily_limit=1000) -> api_key (string)
 - get_key_record(api_key) -> (company, api_key, daily_limit) | None
 - get_key_record_for_company(company) -> (company, api_key, daily_limit) | None
 - increment_usage_and_check(api_key) -> (ok: bool, count: int, limit: int)
 - insert_log(...)
 - query_logs(...)
 - add_tag(...), get_tags_for_file(...), remove_tag(...)
 - add_random_tags_for_file(relative_path), get_tags(relative_path)
"""

import os
import random
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Tuple, List

logger = logging.getLogger("filetagapi.auth")
logger.addHandler(logging.NullHandler())

# Postgres connection config via env (CloudSQL-friendly)
DB_USER = os.getenv("DB_USER", "ft_user")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "filetagapi_db")
DB_HOST = os.getenv("DB_HOST", "localhost")   # or "/cloudsql/INSTANCE_CONNECTION_NAME"
DB_PORT = int(os.getenv("DB_PORT", "5432"))
MIN_CONN = int(os.getenv("DB_MIN_CONN", "1"))
MAX_CONN = int(os.getenv("DB_MAX_CONN", "5"))
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")  # e.g. project:region:instance

# Ensure psycopg2 is present; fail fast if not
try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import pool
    import psycopg2.errors
except Exception as e:
    logger.exception("psycopg2 is required for Postgres backend: %s", e)
    raise

_conn_pool: Optional[pool.SimpleConnectionPool] = None


# def _build_conn_kwargs():
#     """
#     Return kwargs for SimpleConnectionPool. When INSTANCE_CONNECTION_NAME is set,
#     use unix socket host `/cloudsql/<INSTANCE_CONNECTION_NAME>`.
#     """
#     if INSTANCE_CONNECTION_NAME:
#         return {
#             "user": DB_USER,
#             "password": DB_PASS,
#             "dbname": DB_NAME,
#             "host": f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
#         }
#     return {
#         "user": DB_USER,
#         "password": DB_PASS,
#         "dbname": DB_NAME,
#         "host": DB_HOST,
#         "port": DB_PORT,
#     }

def _build_conn_kwargs():
    """
    Return kwargs for SimpleConnectionPool. When INSTANCE_CONNECTION_NAME is set,
    use unix socket host `/cloudsql/<INSTANCE_CONNECTION_NAME>`.
    This function prefers common env names (DATABASE_*, PGPASSWORD, DB_*), so the
    container works whether envs are named DATABASE_PASSWORD or DB_PASS or PGPASSWORD.
    """
    if INSTANCE_CONNECTION_NAME:
        host = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
        # prefer DATABASE_USER over DB_USER etc
        user = os.getenv("DATABASE_USER") or os.getenv("PGUSER") or os.getenv("DB_USER") or DB_USER
        password = (
            os.getenv("DATABASE_PASSWORD")
            or os.getenv("PGPASSWORD")
            or os.getenv("DB_PASSWORD")
            or os.getenv("DB_PASS")
            or os.getenv("PASSWORD")
            or DB_PASS
        )
        dbname = os.getenv("DATABASE_NAME") or os.getenv("DB_NAME") or DB_NAME
        return {"user": user, "password": password, "dbname": dbname, "host": host}

    # TCP path
    host = os.getenv("DATABASE_HOST") or os.getenv("PGHOST") or os.getenv("DB_HOST") or DB_HOST
    port = int(os.getenv("DATABASE_PORT") or os.getenv("PGPORT") or os.getenv("DB_PORT") or DB_PORT)
    user = os.getenv("DATABASE_USER") or os.getenv("PGUSER") or os.getenv("DB_USER") or DB_USER
    password = (
        os.getenv("DATABASE_PASSWORD")
        or os.getenv("PGPASSWORD")
        or os.getenv("DB_PASSWORD")
        or os.getenv("DB_PASS")
        or os.getenv("PASSWORD")
        or DB_PASS
    )
    dbname = os.getenv("DATABASE_NAME") or os.getenv("DB_NAME") or DB_NAME

    return {"user": user, "password": password, "dbname": dbname, "host": host, "port": port}



def get_pool():
    global _conn_pool
    if _conn_pool is None:
        kwargs = _build_conn_kwargs()
        # optional: validate password early
        if not kwargs.get("password"):
            raise RuntimeError("DB password is not set: set DATABASE_PASSWORD or PGPASSWORD or DB_PASSWORD/DB_PASS")
        _conn_pool = pool.SimpleConnectionPool(MIN_CONN, MAX_CONN, **kwargs)
    return _conn_pool


def _get_conn():
    return get_pool().getconn()


def _put_conn(conn):
    try:
        get_pool().putconn(conn)
    except Exception:
        # If pool is shutting down or invalid, silently ignore
        pass


# ---------------- SCHEMA & BOILERPLATE ----------------
def init_db():
    """
    Idempotent creation of required tables. Safe to call repeatedly.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        # api_keys
        cur.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            id SERIAL PRIMARY KEY,
            company TEXT NOT NULL,
            api_key TEXT NOT NULL UNIQUE,
            daily_limit INTEGER DEFAULT 1000,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        # usage
        cur.execute("""
        CREATE TABLE IF NOT EXISTS usage (
            id SERIAL PRIMARY KEY,
            api_key TEXT NOT NULL,
            date DATE NOT NULL,
            count INTEGER DEFAULT 0,
            UNIQUE(api_key, date)
        );
        """)
        # logs
        cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id SERIAL PRIMARY KEY,
            level TEXT,
            path TEXT,
            message TEXT,
            company TEXT,
            survey TEXT,
            filename TEXT,
            meta JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        # tags - include 'question' for survey/question granularity; keep nullable for legacy rows
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id SERIAL PRIMARY KEY,
            company TEXT NOT NULL,
            survey TEXT NOT NULL,
            question TEXT,
            filename TEXT NOT NULL,
            tag TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company, survey, question, filename, tag)
        );
        """)
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)


# ---------- API key functions ----------
def create_api_key(company: str, api_key: str, daily_limit: int = 1000) -> str:
    """
    Create an API key for a company. Returns the final api_key string.
    Robust against races: attempts INSERT, on UniqueViolation fetches existing row.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO api_keys (company, api_key, daily_limit) VALUES (%s, %s, %s)",
                (company, api_key, daily_limit),
            )
            conn.commit()
            cur.close()
            return api_key
        except psycopg2.errors.UniqueViolation:
            # someone else inserted concurrently -- rollback and fetch existing
            conn.rollback()
            cur.close()
            rec = get_key_record_for_company(company)
            if rec:
                return rec[1]  # api_key
            raise
        except Exception:
            conn.rollback()
            cur.close()
            raise
    finally:
        _put_conn(conn)


def get_key_record(api_key: str) -> Optional[Tuple[str, str, int]]:
    """
    Return (company, api_key, daily_limit) or None
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT company, api_key, daily_limit FROM api_keys WHERE api_key = %s", (api_key,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return (row[0], row[1], row[2])
    finally:
        _put_conn(conn)


def get_key_record_for_company(company: str) -> Optional[Tuple[str, str, int]]:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT company, api_key, daily_limit FROM api_keys WHERE company = %s", (company,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return (row[0], row[1], row[2])
    finally:
        _put_conn(conn)


# ---------- Usage counter: atomic increment + check ----------
def increment_usage_and_check(api_key: str) -> Tuple[bool, int, int]:
    """
    Atomically increment usage for today and return (ok:bool, count:int, limit:int).
    Uses Postgres upsert with RETURNING count for atomic increment.
    """
    today = date.today()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        # upsert increment and return count
        cur.execute("""
        INSERT INTO usage (api_key, date, count)
        VALUES (%s, %s, 1)
        ON CONFLICT (api_key, date) DO UPDATE SET count = usage.count + 1
        RETURNING count
        """, (api_key, today))
        cnt_row = cur.fetchone()
        count = cnt_row[0] if cnt_row else 0

        # fetch limit
        cur.execute("SELECT daily_limit FROM api_keys WHERE api_key = %s", (api_key,))
        limit_row = cur.fetchone()
        limit = limit_row[0] if limit_row else None

        conn.commit()
        cur.close()
        if limit is None:
            return False, count, 0
        return (count <= limit), count, limit
    finally:
        _put_conn(conn)


# ---------------- Logs helpers ----------------
def insert_log(level: str, path: str, message: str, company: str = None, survey: str = None, filename: str = None, meta: dict = None):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO logs (level, path, message, company, survey, filename, meta)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (level, path, message, company, survey, filename, psycopg2.extras.Json(meta) if meta is not None else None))
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)


def query_logs(limit: int = 100, company: str = None, survey: str = None):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if company and survey:
            cur.execute("SELECT id, level, path, message, company, survey, filename, meta, created_at FROM logs WHERE company=%s AND survey=%s ORDER BY created_at DESC LIMIT %s", (company, survey, limit))
        elif company:
            cur.execute("SELECT id, level, path, message, company, survey, filename, meta, created_at FROM logs WHERE company=%s ORDER BY created_at DESC LIMIT %s", (company, limit))
        else:
            cur.execute("SELECT id, level, path, message, company, survey, filename, meta, created_at FROM logs ORDER BY created_at DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        _put_conn(conn)


# ---------------- Tags helpers (supports optional question) ----------------
def add_tag(company: str, survey: str, filename: str, tag: str, question: Optional[str] = None):
    """
    Add a tag for a specific file. question may be None for legacy items.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO tags (company, survey, question, filename, tag)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (company, survey, question, filename, tag) DO NOTHING
        """, (company, survey, question, filename, tag))
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)


def get_tags_for_file(company: str, survey: str, filename: str, question: Optional[str] = None) -> List[str]:
    """
    Return list of tags (strings) for a file.
    If question is None, return tags irrespective of question (legacy compatibility).
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if question is None:
            cur.execute("SELECT tag FROM tags WHERE company=%s AND survey=%s AND filename=%s ORDER BY created_at DESC", (company, survey, filename))
        else:
            cur.execute("SELECT tag FROM tags WHERE company=%s AND survey=%s AND question=%s AND filename=%s ORDER BY created_at DESC", (company, survey, question, filename))
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    finally:
        _put_conn(conn)


def remove_tag(company: str, survey: str, filename: str, tag: str, question: Optional[str] = None):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if question is None:
            cur.execute("DELETE FROM tags WHERE company=%s AND survey=%s AND filename=%s AND tag=%s", (company, survey, filename, tag))
        else:
            cur.execute("DELETE FROM tags WHERE company=%s AND survey=%s AND question=%s AND filename=%s AND tag=%s", (company, survey, question, filename, tag))
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)


# ---------------- Backwards-compatible helpers parsing relative paths ----------------
def _parse_relative_path(rel: str):
    """
    Parse object paths of multiple shapes:
      - "gs://bucket/company/survey/question/filename"
      - "gs://bucket/company/survey/filename"  (legacy)
      - "/uploads/company/survey/question/filename"
      - "company/survey/question/filename"
    Returns (company, survey, question_or_None, filename) or (None,None,None,None) on failure.
    """
    if not rel:
        return (None, None, None, None)
    s = str(rel)

    # Extract object portion after bucket for gs:// urls
    if s.startswith("gs://"):
        parts = s.split("/", 3)
        obj = parts[3] if len(parts) >= 4 else ""
    else:
        # strip leading slash and optional 'uploads' prefix
        s = s.lstrip("/")
        if s.startswith("uploads/"):
            obj = s[len("uploads/"):]
        else:
            obj = s

    comps = [c for c in obj.split("/") if c != ""]
    # Expect at least company/survey/filename (len >= 3)
    if len(comps) >= 3:
        if len(comps) >= 4:
            # company/survey/question/filename (choose last 4)
            company = comps[-4]
            survey = comps[-3]
            question = comps[-2]
            filename = comps[-1]
        else:
            # legacy 3-component path: company/survey/filename
            company = comps[-3]
            survey = comps[-2]
            question = None
            filename = comps[-1]
        return (company, survey, question, filename)
    return (None, None, None, None)


def add_random_tags_for_file(relative_path: str, min_tags: int = 1, max_tags: int = 4):
    """
    Backwards-compatible wrapper used by app.upload_file.
    Parses relative_path to derive company/survey/question/filename and inserts random tags into Postgres tags table.
    Returns the list of tags inserted.
    """
    if relative_path is None:
        return []

    company, survey, question, filename = _parse_relative_path(relative_path)
    if not company or not survey or not filename:
        try:
            insert_log("WARN", "add_random_tags_for_file", f"failed_to_parse_path:{relative_path}", company=None, survey=None, filename=None)
        except Exception:
            pass
        return []

    sample_tags = [
        "portrait","blurry","out-of-focus","text","document","dark","low-light",
        "bright","overexposed","underexposed","contains-face","landscape","partial"
    ]
    count = random.randint(min_tags, max_tags)
    tags = random.sample(sample_tags, count)

    for t in tags:
        try:
            add_tag(company, survey, filename, t, question=question)
        except Exception:
            try:
                insert_log("ERROR", "add_random_tags_for_file", f"add_tag_error:{t}", company=company, survey=survey, filename=filename)
            except Exception:
                pass

    return tags


def get_tags(relative_path: str):
    """
    Backwards-compatible: given a relative_path, return list of tag strings.
    """
    company, survey, question, filename = _parse_relative_path(relative_path)
    if not company or not survey or not filename:
        return []
    try:
        return get_tags_for_file(company, survey, filename, question=question)
    except Exception:
        return []
