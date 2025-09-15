# app.py — Amazon FIFO Cost Portal (full v3, psycopg3)
import os, json, datetime as dt
import pandas as pd
import streamlit as st
import psycopg
from psycopg.extras import execute_values

st.set_page_config(page_title="Amazon FIFO Cost Portal", layout="wide")

DB_DSN = os.getenv("DB_DSN")  # e.g. postgresql://postgres:PASS@HOST:5432/postgres
if not DB_DSN:
    st.error("Missing DB_DSN in secrets.")
    st.stop()

def _pg():
    # autocommit=True 简化事务处理；Supabase 默认需要 SSL
    return psycopg.connect(DB_DSN, autocommit=True)

def run_sql(sql, args=None, many=False):
    with _pg() as conn, conn.cursor() as cur:
        if many:
            execute_values(cur, sql, args)
        else:
            cur.execute(sql, args)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return None

def now_ym():
    return dt.datetime.utcnow().strftime("%Y-%m")

st.title("Amazon FIFO Cost Portal")

tabs = st.tabs([
    "Inbound", "SKU Map", "Sales Upload",
    "Monthly", "Inventory", "Adjustments", "Admin"
])

# ---------------- Inbound ----------------
with tabs[0]:
    st.subheader("Inbound Batch")
    c0,c1,c2 = st.columns([1,1,1])
    with c0:
        batch_id = st.text_input("Batch ID", placeholder="e.g., AMZ2509").strip()
    with c1:
        inbound_date = st.date_input("Inbound Date", value=dt.date.today())
    with c2:
        st.caption("Container totals")
        freight_total   = st.number_input("Freight Total",   0.0, step=10.0)
        clearance_total = st.number_input("Clearance Total", 0.0, step=10.0)

    st.markdown("#### Duty Pool by Category")
    duty_df = st.data_editor(
        pd.DataFrame(columns=["category","duty_total"]),
        num_rows="dynamic", use_container_width=True
    )

    st.markdown("#### Items")
    lot_df = st.data_editor(
        pd.DataFrame(columns=["internal_sku","category","qty_in","fob_unit","cbm_per_unit"]),
        num_rows="dynamic", use_container_width=True
    )

    if st.button("Save & Allocate", type="primary", disabled=(not batch_id)):
        with _pg() as conn, conn.cursor() as cur:
            # upsert batch
            cur.execute("""
              insert into batch(batch_id,inbound_date,freight_total,clearance_total)
              values (%s,%s,%s,%s)
              on conflict (batch_id) do update
              set inbound_date=excluded.inbound_date,
                  freight_total=excluded.freight_total,
                  clearance_total=excluded.clearance_total
            """, (batch_id, inbound_date, freight_total, clearance_total))

            # replace tax pool for this batch
            cur.execute("delete from inbound_tax_pool where batch_id=%s", (batch_id,))
            if not duty_df.empty:
                execute_values(cur,
                    "insert into inbound_tax_pool(batch_id,category,duty_total) values %s",
                    [(batch_id, str(r.category), float(r.duty_total or 0)) for r in duty_df.itertuples()]
                )

            # replace inbound_lot for this batch
            cur.execute("delete from inbound_lot where batch_id=%s", (batch_id,))
            if not lot_df.empty:
                execute_values(cur, """
                  insert into inbound_lot(batch_id,internal_sku,category,qty_in,fob_unit,cbm_per_unit)
                  values %s
                """, [
                    (batch_id, str(r.internal_sku), str(r.category),
                     int(r.qty_in or 0),
                     None if pd.isna(r.fob_unit) else float(r.fob_unit),
                     None if pd.isna(r.cbm_per_unit) else float(r.cbm_per_unit))
                    for r in lot_df.itertuples()
                ])
        # 分摊 & 同步库存
        run_sql("select rebuild_lot_costs()")
        st.success("Saved and allocated.")

# ---------------- SKU Map ----------------
with tabs[1]:
    st.subheader("Amazon SKU → Internal SKU (bundles)")
    mp = st.selectbox("Marketplace", ["US","UK","DE","FR","ES","IT","CA","MX","JP","AE","AU"], index=0)
    map_df = st.data_editor(
        pd.DataFrame(columns=["amazon_sku","internal_sku","ratio"]),
        num_rows="dynamic", use_container_width=True
    )
    if st.button("Save Mapping"):
        with _pg() as conn, conn.cursor() as cur:
            cur.execute("delete from sku_map where marketplace=%s", (mp,))
            if not map_df.empty:
                execute_values(cur, """
                  insert into sku_map(marketplace,amazon_sku,internal_sku,ratio)
                  values %s
                """, [(mp, str(r.amazon_sku), str(r.internal_sku), float(r.ratio or 1)) for r in map_df.itertuples()])
        st.success("Mapping saved.")

# ---------------- Sales Upload ----------------
with tabs[2]:
    st.subheader("Sales Upload (Monthly Unified Transaction CSV)")
    ym = st.text_input("Year-Month (YYYY-MM)", value=now_ym())
    file = st.file_uploader("Upload CSV", type=["csv"])
    st.caption("We read exactly: date/time, settlement type, order id, marketplace, sku, quantity")

    def normalize_csv(df: pd.DataFrame, ym: str):
        df.columns = [c.strip().lower() for c in df.columns]
        need = ["date/time","settlement type","order id","marketplace","sku","quantity"]
        miss = [c for c in need if c not in df.columns]
        if miss:
            raise ValueError(f"Missing columns: {miss}")

        out = pd.DataFrame({
            "raw_time": pd.to_datetime(df["date/time"], errors="coerce"),
            "raw_type": df["settlement type"].astype(str),
            "order_id": df["order id"].astype(str),
            "marketplace": df["marketplace"].fillna("US").astype(str),
            "amazon_sku": df["sku"].astype(str),
            "quantity": pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int),
        })
        out = out.dropna(subset=["raw_time"])
        # 仅取选择的月份
        s = pd.to_datetime(ym + "-01")
        e = (s + pd.offsets.MonthEnd(1)) + pd.Timedelta(days=1)
        out = out[(out["raw_time"] >= s) & (out["raw_time"] < e)]
        # 备份整行
        out["raw_payload"] = df.apply(lambda r: json.loads(r.to_json(force_ascii=False)), axis=1)
        return out

    if file and st.button("Import & Compute", type="primary"):
        df0 = pd.read_csv(file)
        try:
            data = normalize_csv(df0, ym)
        except Exception as e:
            st.error(str(e)); st.stop()

        if data.empty:
            st.warning("No rows for this month.")
        else:
            tuples = list(data[["raw_time","raw_type","order_id","marketplace","amazon_sku","quantity","raw_payload"]]
                          .itertuples(index=False, name=None))
            run_sql("""
              insert into sales_raw(raw_time,raw_type,order_id,marketplace,amazon_sku,quantity,raw_payload)
              values %s
            """, tuples, many=True)

        # 规范化 → 整月 FIFO（会自动回滚旧分配再重建） → 月度合计写入
        run_sql("select normalize_month_sales(%s)", (ym,))
        run_sql("select fifo_rebuild_month(%s)", (ym,))
        st.success(f"Imported & computed for {ym}")

# ---------------- Monthly ----------------
with tabs[3]:
    st.subheader("Monthly Summary")
    ym_q = st.text_input("Month to view", value=now_ym(), key="ym_view")
    rows = run_sql("""select ym, orders, fob_sum, freight_sum, clearance_sum, duty_sum, updated_at
                      from month_summary where ym=%s""", (ym_q,))
    if rows:
        st.dataframe(pd.DataFrame(rows, columns=["Month","Orders","FOB","Freight","Clearance","Duty","Updated"]),
                     use_container_width=True)

    c1,c2 = st.columns(2)
    if c1.button("Rebuild this month"):
        run_sql("select fifo_rebuild_month(%s)", (ym_q,))
        st.success("Rebuilt (rollback previous allocations, recompute FIFO).")
    if c2.button("Show month history"):
        hist = run_sql("""select rev_id, snapshot_at, reason, fob_sum, freight_sum, clearance_sum, duty_sum, orders
                          from month_history where ym=%s order by rev_id desc""", (ym_q,))
        if hist:
            st.dataframe(pd.DataFrame(hist, columns=["Rev","At","Reason","FOB","Freight","Clearance","Duty","Orders"]),
                         use_container_width=True)
        else:
            st.info("No history yet.")

# ---------------- Inventory ----------------
with tabs[4]:
    st.subheader("Inventory (FIFO balance)")
    df = run_sql("""
      select lb.internal_sku, sum(lb.qty_in) as qty_in, sum(lb.qty_sold) as qty_sold,
             sum(lb.qty_in - lb.qty_sold) as qty_left
      from lot_balance lb
      group by lb.internal_sku
      order by lb.internal_sku
    """)
    inv = pd.DataFrame(df, columns=["Internal SKU","Qty In","Qty Sold","Qty Left"]) if df else pd.DataFrame()

    th = run_sql("select internal_sku, min_qty from inventory_thresholds order by internal_sku")
    thr = pd.DataFrame(th, columns=["Internal SKU","Min Qty"]) if th else pd.DataFrame(columns=["Internal SKU","Min Qty"])

    colA, colB = st.columns([2,1])
    with colA:
        if not inv.empty:
            inv = inv.merge(thr, on="Internal SKU", how="left").fillna({"Min Qty":0})
            inv["Low"] = (inv["Qty Left"] <= inv["Min Qty"]).map({True:"YES", False:""})
            st.dataframe(inv, use_container_width=True)
        else:
            st.info("No inventory yet.")
    with colB:
        st.caption("Edit thresholds")
        edit = st.data_editor(thr if not thr.empty else pd.DataFrame(columns=["Internal SKU","Min Qty"]),
                              num_rows="dynamic", use_container_width=True)
        if st.button("Save thresholds"):
            with _pg() as conn, conn.cursor() as cur:
                cur.execute("delete from inventory_thresholds")
                if not edit.empty:
                    execute_values(cur, """
                      insert into inventory_thresholds(internal_sku, min_qty) values %s
                    """, [(str(r._0), int(r._1 or 0)) for r in edit.itertuples()])
            st.success("Thresholds saved.")

# ---------------- Adjustments ----------------
with tabs[5]:
    st.subheader("Adjustments (reverse a past order by Order ID)")
    order_id = st.text_input("Order ID to reverse")
    note = st.text_input("Note (optional)")
    if st.button("Preview allocations"):
        rows = run_sql("""
          select ym, happened_at, order_id, marketplace, amazon_sku, internal_sku, batch_id, qty,
                 fob_unit, freight_unit, clearance_unit, duty_unit
          from allocation_detail
          where order_id=%s
          order by happened_at
        """, (order_id,))
        if rows:
            cols = ["Month","Happened","Order","Mkt","AmazonSKU","InternalSKU","Batch","Qty",
                    "FOB_u","Freight_u","Clearance_u","Duty_u"]
            st.dataframe(pd.DataFrame(rows, columns=cols), use_container_width=True)
        else:
            st.info("No allocation found for this order.")
    if st.button("Reverse this order", type="primary", disabled=(not order_id)):
        try:
            run_sql("select reverse_order_allocation(%s,%s)", (order_id, note))
            st.success("Reversed. Inventory and month sums updated.")
        except Exception as e:
            st.error(str(e))

# ---------------- Admin ----------------
with tabs[6]:
    st.subheader("Admin")
    st.caption("Monthly controls & data maintenance.")
    ym_admin = st.text_input("Month", value=now_ym())
    c1,c2,c3,c4 = st.columns(4)
    if c1.button("Normalize only"):
        run_sql("select normalize_month_sales(%s)", (ym_admin,))
        st.success("Normalized.")
    if c2.button("FIFO rebuild only"):
        run_sql("select fifo_rebuild_month(%s)", (ym_admin,))
        st.success("Rebuilt.")
    if c3.button("Rebuild costs (CBM/FOB)"):
        run_sql("select rebuild_lot_costs()")
        st.success("Costs rebuilt.")
    if c4.button("Truncate all data (keep schema)"):
        run_sql("select truncate_runtime_data()")
        st.warning("All data cleared (schema kept).")
