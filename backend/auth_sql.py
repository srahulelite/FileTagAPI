# backend/auth_sql.py
import os
import random
import json
from datetime import date
from pathlib import Path

import psycopg2
import psycopg2.extras
from psycopg2 import pool

# Postgres connection config via env
DB_USER = os.getenv("DB_USER", "ft_user")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "filetagapi_db")
DB_HOST = os.getenv("DB_HOST", "localhost")   # for Cloud Run use "/cloudsql/INSTANCE_CONNECTION"
DB_PORT = int(os.getenv("DB_PORT", "5432"))
MIN_CONN = int(os.getenv("DB_MIN_CONN", "1"))
MAX_CONN = int(os.getenv("DB_MAX_CONN", "5"))


# Cloud Run / Cloud SQL connector
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")  # e.g. filetagapi-prod:asia-south1:filetagapi-sql

_conn_pool = None


def _build_conn_kwargs():
    """
    Return kwargs that can be passed to psycopg2.connect or SimpleConnectionPool.
    If running with INSTANCE_CONNECTION_NAME, use unix socket host `/cloudsql/...`
    """
    if INSTANCE_CONNECTION_NAME:
        # use unix socket path (no port)
        return {
            "user": DB_USER,
            "password": DB_PASS,
            "dbname": DB_NAME,
            "host": f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
        }
    else:
        # traditional TCP host/port (local dev or docker)
        return {
            "user": DB_USER,
            "password": DB_PASS,
            "dbname": DB_NAME,
            "host": DB_HOST,
            "port": DB_PORT
        }
    
def get_pool():
    global _conn_pool
    if _conn_pool is None:
        kwargs = _build_conn_kwargs()
        # SimpleConnectionPool will accept host as unix socket path when provided
        _conn_pool = pool.SimpleConnectionPool(
            MIN_CONN, MAX_CONN, **kwargs
        )
    return _conn_pool

def _get_conn():
    return get_pool().getconn()

def _put_conn(conn):
    get_pool().putconn(conn)


# ---------- Schema & Auth (api_keys, usage) ----------
def init_db():
    """
    Create all required tables: api_keys, usage, logs, tags.
    Safe to call repeatedly.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        # api_keys table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS api_keys (
            id SERIAL PRIMARY KEY,
            company TEXT NOT NULL,
            api_key TEXT NOT NULL UNIQUE,
            daily_limit INTEGER DEFAULT 1000,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)


        # usage table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS usage (
            id SERIAL PRIMARY KEY,
            api_key TEXT NOT NULL,
            date DATE NOT NULL,
            count INTEGER DEFAULT 0,
            UNIQUE(api_key, date)
        );
        """)
        # logs table
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
        ## tags table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id SERIAL PRIMARY KEY,
            company TEXT NOT NULL,
            survey TEXT NOT NULL,
            filename TEXT NOT NULL,
            tag TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company, survey, filename, tag)
        );
        """)
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)



import psycopg2.errors

def create_api_key(company: str, api_key: str, daily_limit: int = 1000):
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
                return rec[1]
            # If not found (unexpected), re-raise
            raise
        except Exception:
            # other DB error: rollback and re-raise
            conn.rollback()
            cur.close()
            raise
    finally:
        _put_conn(conn)


def get_key_record(api_key: str):
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

def get_key_record_for_company(company: str):
    """
    Return (company, api_key, daily_limit) or None
    """
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
   


def increment_usage_and_check(api_key: str):
    today = date.today()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO usage (api_key, date, count)
        VALUES (%s, %s, 0)
        ON CONFLICT (api_key, date) DO NOTHING
        """, (api_key, today))
        cur.execute("UPDATE usage SET count = count + 1 WHERE api_key = %s AND date = %s", (api_key, today))
        cur.execute("SELECT count FROM usage WHERE api_key = %s AND date = %s", (api_key, today))
        cnt_row = cur.fetchone()
        count = cnt_row[0] if cnt_row else 0
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
    """
    Insert a log row. meta is optional dict which will be stored as JSONB.
    """
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
    """
    Simple query: return latest logs optionally filtered by company/survey.
    Returns list of tuple rows.
    """
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


# ---------------- Tags helpers ----------------

def add_tag(company: str, survey: str, filename: str, tag: str):
    """
    Add a tag for a specific file. Duplicate tags are ignored via ON CONFLICT.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO tags (company, survey, filename, tag)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (company, survey, filename, tag) DO NOTHING
        """, (company, survey, filename, tag))
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)


def get_tags_for_file(company: str, survey: str, filename: str):
    """
    Return list of tags (strings) for a file.
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT tag FROM tags WHERE company=%s AND survey=%s AND filename=%s ORDER BY created_at DESC", (company, survey, filename))
        rows = cur.fetchall()
        cur.close()
        return [r[0] for r in rows]
    finally:
        _put_conn(conn)


def remove_tag(company: str, survey: str, filename: str, tag: str):
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM tags WHERE company=%s AND survey=%s AND filename=%s AND tag=%s", (company, survey, filename, tag))
        conn.commit()
        cur.close()
    finally:
        _put_conn(conn)


# ---------- Backwards-compatible tags helpers ----------

def _parse_relative_path(rel: str):
    """
    Accepts:
      - "uploads/company/survey/filename"
      - "/uploads/company/survey/filename"
      - "gs://bucket/company/survey/filename"
      - "company/survey/filename"
    Returns (company, survey, filename) or (None, None, None) if parsing fails.
    """
    if not rel:
        return (None, None, None)
    s = str(rel)

    # gs://bucket/company/survey/filename  -> strip gs://bucket/
    if s.startswith("gs://"):
        parts = s.split("/", 3)
        if len(parts) >= 4:
            obj = parts[3]
        else:
            obj = ""
        comps = obj.split("/")
    else:
        # strip leading / if exists
        if s.startswith("/"):
            s = s.lstrip("/")
        comps = s.split("/")
        if len(comps) >= 1 and comps[0] == "uploads":
            comps = comps[1:]

    if len(comps) >= 3:
        company = comps[-3]
        survey = comps[-2]
        filename = comps[-1]
        return (company, survey, filename)
    return (None, None, None)


def add_random_tags_for_file(relative_path: str, min_tags: int = 1, max_tags: int = 4):
    """
    Backwards-compatible wrapper used by app.upload_file.
    Parses relative_path to derive company/survey/filename and inserts random tags into Postgres tags table.
    Returns the list of tags inserted.
    """
    if relative_path is None:
        return []

    company, survey, filename = _parse_relative_path(relative_path)
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
            add_tag(company, survey, filename, t)
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
    company, survey, filename = _parse_relative_path(relative_path)
    if not company or not survey or not filename:
        return []
    try:
        return get_tags_for_file(company, survey, filename)
    except Exception:
        return []
