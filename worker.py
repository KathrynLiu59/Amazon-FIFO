# worker.py
from db import run_sql, fetchdf

def rebuild_lot_costs():
    run_sql("select rebuild_lot_costs();")

def build_sales_txn_for_month(ym: str):
    run_sql("select build_sales_txn_for_month(%s);", (ym,))

def run_fifo_allocation(ym: str):
    run_sql("select run_fifo_allocation(%s);", (ym,))

def summarize_month(ym: str):
    run_sql("select summarize_month(%s);", (ym,))

def reverse_order(order_id: str):
    run_sql("select apply_adjustment('REVERSE_ORDER', %s);", (order_id,))

def replay_order(order_id: str):
    run_sql("select apply_adjustment('REPLAY_ORDER', %s);", (order_id,))

def inventory_snapshot_df():
    return fetchdf("""
        select internal_sku, sum(qty_remaining) as qty_remaining
        from lot_balance
        group by internal_sku
        order by internal_sku
    """)
