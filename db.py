import os
import psycopg2
import psycopg2.extras

def get_conn():
    dsn = os.environ.get("DB_DSN") or os.environ.get("DB_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        # Streamlit Secrets
        try:
            import streamlit as st
            dsn = st.secrets.get("DB_DSN")
        except Exception:
            pass
    if not dsn:
        raise RuntimeError("DB_DSN is not set in Streamlit Secrets.")
    return psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)

def run_sql(sql, params=None, many=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            if many and isinstance(params, list):
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
            try:
                return cur.fetchall()
            except psycopg2.ProgrammingError:
                return []

def run_sql_nores(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
