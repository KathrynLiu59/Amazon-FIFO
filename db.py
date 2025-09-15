import os
import psycopg2
from psycopg2.extras import execute_values
import streamlit as st

@st.cache_resource(show_spinner=False)
def get_conn():
    dsn = st.secrets.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("Missing POSTGRES_DSN in Streamlit secrets")
    return psycopg2.connect(dsn)

def fetch_df(sql, params=None):
    import pandas as pd
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)

def exec_sql(sql, params=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())

def bulk_upsert(table, cols, rows, conflict_cols):
    if not rows:
        return 0
    col_list = ",".join(cols)
    update_set = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])
    sql = f"""
    insert into {table} ({col_list}) values %s
    on conflict ({",".join(conflict_cols)}) do update
    set {update_set};
    """
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
    return len(rows)
