# worker.py — 串联：build lot costs -> lot balance -> explode kits -> fifo -> summarize
from db import get_conn

def run_all(ym: str, marketplace: str, combo_group: str = 'default'):
    with get_conn() as conn, conn.cursor() as cur:
        # 用到的批次：该站点所有批次都可用于分配（跨月库存）
        # 但每次有新批次录入时，你需要先：build_lot_costs + rebuild_lot_balance
        # 这里给一个自动化安全做法：对所有 inbound_lot 执行一次（幂等）
        cur.execute("select batch_id from inbound_lot where marketplace=%s;", (marketplace,))
        for (batch_id,) in cur.fetchall():
            cur.execute("select build_lot_costs(%s,%s);", (batch_id, marketplace))
            cur.execute("select rebuild_lot_balance(%s,%s);", (batch_id, marketplace))

        # 展开组合柜 → sales_txn
        cur.execute("select explode_kits(%s,%s,%s);", (ym, marketplace, combo_group))

        # FIFO 分配
        cur.execute("select fifo_allocate(%s,%s);", (ym, marketplace))

        # 月度汇总 + 快照
        cur.execute("select summarize_month(%s,%s);", (ym, marketplace))

        conn.commit()

def last_runs():
    # 预留：可查询最近的 month_summary 展示
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select ym, marketplace, orders, units, fob_total, freight_total, entry_total, duty_total, updated_at from month_summary order by updated_at desc limit 20;")
        return cur.fetchall()
