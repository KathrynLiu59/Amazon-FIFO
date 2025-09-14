
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras

DSN = os.environ.get("DB_DSN")

def get_conn():
    if not DSN:
        raise RuntimeError("Missing DB_DSN in Streamlit secrets / environment.")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn

def execute(sql, params=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        try:
            rows = cur.fetchall()
        except psycopg2.ProgrammingError:
            rows = []
        return rows

def df_insert(df: pd.DataFrame, table: str, truncate_first=False):
    if df.empty:
        return 0
    cols = list(df.columns)
    values = [tuple(None if (pd.isna(row[c]) if not isinstance(row[c], (list, dict)) else False) else row[c] for c in cols) for _, row in df.iterrows()]
    with get_conn() as conn, conn.cursor() as cur:
        if truncate_first:
            cur.execute(f"truncate table {table};")
        sql = f"insert into {table} ({', '.join(cols)}) values %s"
        extras.execute_values(cur, sql, values, template=None, page_size=1000)
    return len(values)
