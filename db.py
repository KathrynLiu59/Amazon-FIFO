# db.py
import os
import psycopg
from contextlib import contextmanager

DB_DSN = os.getenv("DB_DSN")
if not DB_DSN:
    raise RuntimeError("Missing DB_DSN in Streamlit Secrets. Put it in the app's Secrets.")

def get_conn():
    return psycopg.connect(DB_DSN)

@contextmanager
def cursor():
    with get_conn() as conn:
        with conn.cursor() as cur:
            yield cur
        conn.commit()

def run_sql(sql_text: str, params: tuple | None = None):
    with cursor() as cur:
        cur.execute(sql_text, params or ())

def fetchall(sql_text: str, params: tuple | None = None):
    with cursor() as cur:
        cur.execute(sql_text, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return cols, rows

def fetchdf(sql_text: str, params: tuple | None = None):
    import pandas as pd
    cols, rows = fetchall(sql_text, params)
    return pd.DataFrame(rows, columns=cols)
