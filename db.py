# db.py
import os
import psycopg
from psycopg.rows import dict_row

def get_dsn() -> str:
    dsn = os.getenv("DB_DSN") or os.getenv("SUPABASE_DB_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing DB_DSN (or SUPABASE_DB_DSN / DATABASE_URL) in secrets.")
    return dsn

def connect():
    # psycopg3 connect, autocommit false; let callers manage commit
    return psycopg.connect(get_dsn(), row_factory=dict_row)

def run_sql(sql: str, params: tuple | None = None):
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            try:
                rows = cur.fetchall()
                return rows
            except psycopg.ProgrammingError:
                # no rows
                return []

def run_sql_one(sql: str, params: tuple | None = None):
    rows = run_sql(sql, params)
    return rows[0] if rows else None

def run_function(name: str, *args):
    # call stored function like: select fn($1,$2)
    placeholders = ",".join(["%s"] * len(args))
    sql = f"select {name}({placeholders})"
    return run_sql(sql, args)
