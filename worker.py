# worker.py
from db import get_conn

def run_sql(sql: str, params: tuple = ()):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        try:
            rows = cur.fetchall()
        except Exception:
            rows = []
        cur.close()
    return rows

def run_all(selected_month: str):
    """
    例：你把所有需要调用的 SQL 函数在这里串起来
    """
    run_sql("select rebuild_lot_costs()")
    run_sql("select rebuild_lot_balance()")
    run_sql("select summarize_month(%s)", (selected_month,))
    return True

def last_runs(limit: int = 20):
    return run_sql(
        "select happened_at, note from movement order by happened_at desc limit %s",
        (limit,)
    )
