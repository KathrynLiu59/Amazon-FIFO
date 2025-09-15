import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras

DSN = os.environ.get("DB_DSN")

def get_conn():
    if not DSN:
        raise RuntimeError("Missing DB_DSN in Streamlit secrets (key: DB_DSN).")
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
    values = [tuple(None if (pd.isna(row[c]) if not isinstance(row[c], (list, dict)) else False) else row[c]
                    for c in cols) for _, row in df.iterrows()]
    with get_conn() as conn, conn.cursor() as cur:
        if truncate_first:
            cur.execute(f"truncate table {table};")
        sql = f"insert into {table} ({', '.join(cols)}) values %s"
        extras.execute_values(cur, sql, values, page_size=1000)
    return len(values)

def df_upsert(df: pd.DataFrame, table: str, conflict_cols: list[str]):
    if df.empty:
        return 0
    cols = list(df.columns)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("create temporary table _tmp_upsert as select * from " + table + " limit 0;")
        rows = [tuple(None if (pd.isna(row[c]) if not isinstance(row[c], (list, dict)) else False) else row[c]
                      for c in cols) for _, row in df.iterrows()]
        extras.execute_values(cur, "insert into _tmp_upsert(" + ", ".join(cols) + ") values %s", rows, page_size=1000)
        set_clause = ", ".join([f"{c}=excluded.{c}" for c in cols if c not in conflict_cols])
        conflict = ", ".join(conflict_cols)
        cur.execute(f"""
            insert into {table} ({", ".join(cols)})
            select {", ".join(cols)} from _tmp_upsert
            on conflict ({conflict}) do update set {set_clause};
        """)
    return len(rows)

def fetch_df(sql_text: str, params=None, columns=None):
    rows = execute(sql_text, params)
    return pd.DataFrame(rows, columns=columns)
