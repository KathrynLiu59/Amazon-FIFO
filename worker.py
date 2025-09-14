
from db import execute, get_conn

def expand_movements(from_iso: str):
    # delete existing movements from that date
    execute("delete from movement where happened_at >= %s and source='order';", (from_iso,))
    # kit first
    sql_kit = """
    insert into movement(happened_at, internal_sku, qty, source, order_id, marketplace, amazon_sku)
    select st.happened_at, kb.component_sku, st.qty * kb.units_per_kit, 'order', st.order_id, st.marketplace, st.sku
    from sales_txn st
    join kit_bom kb
      on kb.sku = st.sku and coalesce(st.marketplace,'US') = kb.marketplace
    where st.type='order' and st.happened_at >= %s
    """
    execute(sql_kit, (from_iso,))
    # fallback to sku_map (no kit)
    sql_map = """
    insert into movement(happened_at, internal_sku, qty, source, order_id, marketplace, amazon_sku)
    select st.happened_at, sm.internal_sku, st.qty * coalesce(sm.unit_multiplier,1), 'order', st.order_id, st.marketplace, st.sku
    from sales_txn st
    join sku_map sm
      on sm.sku = st.sku and coalesce(st.marketplace,'US') = sm.marketplace
    where st.type='order' and st.happened_at >= %s
      and not exists (
        select 1 from kit_bom kb
        where kb.sku = st.sku and kb.marketplace = coalesce(st.marketplace,'US')
      )
    """
    execute(sql_map, (from_iso,))
    return True

def fifo_allocate(from_iso: str):
    execute("delete from allocation_detail where happened_at >= %s and reversed_by is null;", (from_iso,))
    execute("select rebuild_lot_costs();")
    rows = execute("""
        select happened_at, internal_sku, qty, order_id 
        from movement
        where happened_at >= %s
        order by happened_at, id
    """, (from_iso,))
    if not rows:
        return 0
    total_alloc = 0
    with get_conn() as conn, conn.cursor() as cur:
        for happened_at, internal_sku, qty, order_id in rows:
            remaining = float(qty)
            lots = execute("""
                select lb.batch_id, lb.qty_remaining,
                       lc.fob_unit, lc.freight_per_unit, lc.duty_per_unit, lc.clearance_per_unit,
                       b.inbound_date
                from lot_balance lb
                join lot_cost lc on lc.batch_id=lb.batch_id and lc.internal_sku=lb.internal_sku
                join batch b on b.batch_id=lb.batch_id
                where lb.internal_sku = %s and lb.qty_remaining > 0
                order by b.inbound_date, lb.batch_id
            """, (internal_sku,))
            for batch_id, qty_left, fob, fr, du, cl, _ in lots:
                if remaining <= 0:
                    break
                take = min(remaining, float(qty_left))
                if take <= 0:
                    continue
                cur.execute("""
                    insert into allocation_detail(happened_at, internal_sku, qty, batch_id, order_id,
                                                  fob_unit, freight_unit, duty_unit, clearance_unit)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """, (happened_at, internal_sku, take, batch_id, order_id, fob, fr, du, cl))
                remaining -= take
                total_alloc += take
        conn.commit()
    execute("select rebuild_lot_costs();")
    return total_alloc

def summarize():
    execute("select summarize_months();")
    return True

def reverse_order(order_id: str, note: str = ''):
    execute("select reverse_order(%s,%s);", (order_id, note))
    return True
