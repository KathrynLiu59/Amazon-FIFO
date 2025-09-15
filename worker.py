# worker.py
from datetime import datetime
from db import run_function, run_sql

def normalize_sales(ym: str):
    run_function("normalize_sales_from_raw", ym)

def rebuild_costs():
    run_function("rebuild_lot_costs")

def run_fifo(ym: str, marketplace: str | None = None):
    run_function("run_fifo_allocation", ym, marketplace)

def summarize(ym: str):
    run_function("summarize_month", ym)

def run_all(ym: str, marketplace: str | None = None):
    """
    后端已提供 run_all(ym, marketplace)；这里优先调用后端一次跑完整。
    若你想在前端逐步执行（方便定位），可以注释掉第一行，改为分步调用。
    """
    run_function("run_all", ym, marketplace)
    # —— 分步替代（保留在此，便于调试）
    # normalize_sales(ym)
    # rebuild_costs()
    # run_fifo(ym, marketplace)
    # summarize(ym)

def last_runs(limit: int = 20):
    return run_sql(
        "select * from run_history order by id desc limit %s",
        (limit,)
    )
