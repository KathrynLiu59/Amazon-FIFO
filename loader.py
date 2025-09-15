# loader.py
from typing import Iterable, Sequence
import psycopg
from db import get_conn

# ============ 批量导入 ============

def insert_inbound_lot(rows: Sequence[tuple]) -> int:
    """
    rows: [(batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit), ...]
    发生冲突时以 batch_id+internal_sku 作为键进行 upsert。
    """
    if not rows:
        return 0

    sql = """
    insert into inbound_lot
        (batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit)
    values
        (%s, %s, %s, %s, %s, %s)
    on conflict (batch_id, internal_sku) do update set
        category     = excluded.category,
        qty_in       = excluded.qty_in,
        fob_unit     = excluded.fob_unit,
        cbm_per_unit = excluded.cbm_per_unit
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount or 0


def upsert_duty_pool(rows: Sequence[tuple]) -> int:
    """
    rows: [(batch_id, category, duty_total), ...]
    以 (batch_id, category) 为键
    """
    if not rows:
        return 0
    sql = """
    insert into batch_duty_pool (batch_id, category, duty_total)
    values (%s, %s, %s)
    on conflict (batch_id, category) do update set
      duty_total = excluded.duty_total
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount or 0


def upsert_entry_fees(rows: Sequence[tuple]) -> int:
    """
    rows: [(batch_id, freight_total, entryfees_total), ...]  一条记录一条柜的头程与清关合计
    以 batch_id 为键
    """
    if not rows:
        return 0
    sql = """
    insert into lot_cost (batch_id, freight_unit, duty_unit, clearance_unit, internal_sku)
    values (%s, 0, 0, 0, '')  -- 仅占位；rebuild 时会重算并写入明细
    on conflict (batch_id, internal_sku) do nothing
    """
    # 这里不真正存入费用，费用实际在 rebuild 函数内计算并写入。
    # 但为了你现有架构不动太大，这个函数保留；若无需要可不调用。
    return 0


def insert_sales_txn(rows: Sequence[tuple]) -> int:
    """
    rows: [(happened_at, type, order_id, amazon_sku, qty, marketplace), ...]
    只保留 type='Order' 的行（建议在上层过滤）
    """
    if not rows:
        return 0

    sql = """
    insert into sales_txn
      (happened_at, type, order_id, amazon_sku, qty, marketplace)
    values
      (%s, %s, %s, %s, %s, %s)
    on conflict do nothing
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount or 0


def upsert_sku_map(rows: Iterable[tuple]) -> int:
    """
    rows: [(amazon_sku, marketplace, internal_sku, unit_multiplier)]
    允许“组合柜”：同一个 amazon_sku 可对应多条 internal_sku + multiplier
    主键在建表时建议定义为 (amazon_sku, marketplace, internal_sku)
    """
    rows = list(rows)
    if not rows:
        return 0

    sql = """
    insert into sku_map (amazon_sku, marketplace, internal_sku, unit_multiplier)
    values (%s, %s, %s, %s)
    on conflict (amazon_sku, marketplace, internal_sku) do update set
      unit_multiplier = excluded.unit_multiplier
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            return cur.rowcount or 0
