import os
import io
import pandas as pd
import streamlit as st
from datetime import date
from db import ensure_schema, insert_rows, call_sql, get_pg

st.set_page_config(page_title="Amazon FIFO Portal", layout="wide")

# 1) 首次自动建库（无感）
try:
    ensure_schema()
except Exception as e:
    st.error(f"Database init failed: {e}")
    st.stop()

# ========== 顶部导航 ==========
tabs = st.tabs(["Inbound", "Sales Import", "SKU Map", "Inventory", "Monthly", "Admin"])

# ========== Tab 1: Inbound ==========
with tabs[0]:
    st.subheader("Inbound (Record a New Container/Batch)")
    col1, col2, col3 = st.columns([1,1,2])
    with col1:
        batch_id = st.text_input("Batch ID", placeholder="e.g., WF2305")
    with col2:
        inbound_date = st.date_input("Inbound Date", value=date.today())
    with col3:
        c1, c2 = st.columns(2)
        with c1:
            freight_total = st.number_input("Freight Total", min_value=0.0, step=0.01)
        with c2:
            clearance_total = st.number_input("Clearance Total", min_value=0.0, step=0.01)

    st.markdown("**Paste items below (internal_sku, category, qty_in, fob_unit, cbm_per_unit)**")
    df_items = st.data_editor(
        pd.DataFrame(columns=["internal_sku","category","qty_in","fob_unit","cbm_per_unit"]),
        use_container_width=True, num_rows="dynamic"
    )

    st.markdown("**Duty by Category (optional)**  —  one row per category")
    df_duty = st.data_editor(
        pd.DataFrame(columns=["category","duty_total"]),
        use_container_width=True, num_rows="dynamic"
    )

    if st.button("Save Batch & Allocate Costs", type="primary", use_container_width=True):
        if not batch_id.strip():
            st.error("Batch ID required"); st.stop()
        # 写 batch
        insert_rows("batch",
                    [(batch_id, inbound_date, float(freight_total), float(clearance_total))],
                    ["batch_id","inbound_date","freight_total","clearance_total"])
        # 写 inbound_lot
        rows = []
        for _, r in df_items.dropna(how="all").iterrows():
            if not str(r.get("internal_sku","")).strip(): continue
            rows.append((
                batch_id,
                str(r["internal_sku"]).strip(),
                str(r["category"]).strip(),
                int(r["qty_in"]),
                float(r["fob_unit"]) if pd.notna(r["fob_unit"]) else None,
                float(r["cbm_per_unit"]) if pd.notna(r["cbm_per_unit"]) else None
            ))
        if rows:
            insert_rows("inbound_lot", rows,
                        ["batch_id","internal_sku","category","qty_in","fob_unit","cbm_per_unit"])
        # 写税金池
        duty_rows=[]
        for _, r in df_duty.dropna(how="all").iterrows():
            if not str(r.get("category","")).strip(): continue
            duty_rows.append((batch_id, str(r["category"]).strip(), float(r["duty_total"])))
        if duty_rows:
            insert_rows("inbound_tax_pool", duty_rows, ["batch_id","category","duty_total"])
        # 分摊&同步库存
        call_sql("select rebuild_lot_costs();")
        st.success("Saved. Costs allocated & inventory synced.")

# ========== Tab 2: Sales Import ==========
with tabs[1]:
    st.subheader("Sales Import (Amazon Monthly Unified Transaction CSV)")
    ym = st.text_input("Month (YYYY-MM)", placeholder="e.g., 2025-03")
    file = st.file_uploader("Upload CSV", type=["csv"])
    st.caption("I only need columns: **date/time**, **type**, **order id**, **marketplace**, **sku**, **quantity**.")

    if file and ym and st.button("Import → Normalize → FIFO", type="primary"):
        df = pd.read_csv(file)
        # 映射常见列名（兼容不同导出）
        colmap = {
            "date/time":"raw_time", "type":"raw_type", "order id":"order_id",
            "marketplace":"marketplace", "sku":"amazon_sku", "quantity":"quantity"
        }
        # 统一小写列名再映射
        df.columns = [c.strip().lower() for c in df.columns]
        need = list(colmap.keys())
        missing = [c for c in need if c not in df.columns]
        if missing:
            st.error(f"Missing columns: {missing}"); st.stop()
        df = df[need].rename(columns=colmap)

        # 写入 sales_raw
        rows = []
        for _, r in df.iterrows():
            rows.append((
                pd.to_datetime(r["raw_time"], errors="coerce"),
                str(r["raw_type"]),
                str(r["order_id"]) if pd.notna(r["order_id"]) else None,
                str(r["marketplace"]) if pd.notna(r["marketplace"]) else "US",
                str(r["amazon_sku"]),
                int(r["quantity"]) if pd.notna(r["quantity"]) else None,
                None
            ))
        insert_rows("sales_raw", rows, ["raw_time","raw_type","order_id","marketplace","amazon_sku","quantity","raw_payload"])

        # 标准化 → 汇总订单数 → FIFO 消耗并落地成本
        call_sql("select normalize_month_sales(%s);", (ym,))
        call_sql("select summarize_month(%s);", (ym,))
        call_sql("select apply_fifo_for_month(%s);", (ym,))
        st.success(f"Done for {ym}.")

# ========== Tab 3: SKU Map ==========
with tabs[2]:
    st.subheader("SKU Map (Amazon SKU → Internal SKU)")
    st.caption("Support kits/combos via multiple rows with ratios. Marketplace default 'US'.")
    conn = get_pg(); cur = conn.cursor()
    cur.execute("select marketplace, amazon_sku, internal_sku, ratio from sku_map order by marketplace, amazon_sku;")
    data = cur.fetchall(); cur.close(); conn.close()
    df_map = pd.DataFrame(data, columns=["marketplace","amazon_sku","internal_sku","ratio"])
    edited = st.data_editor(df_map, use_container_width=True, num_rows="dynamic")
    if st.button("Save Mapping", type="primary"):
        # 简单做法：truncate 后全量重写
        call_sql("truncate table sku_map;")
        rows=[]
        for _, r in edited.dropna(how="all").iterrows():
            if not str(r.get("amazon_sku","")).strip(): continue
            rows.append((
                str(r.get("marketplace","US")).strip() or "US",
                str(r["amazon_sku"]).strip(),
                str(r["internal_sku"]).strip(),
                float(r.get("ratio",1) or 1)
            ))
        if rows:
            insert_rows("sku_map", rows, ["marketplace","amazon_sku","internal_sku","ratio"])
        st.success("SKU map saved.")

# ========== Tab 4: Inventory ==========
with tabs[3]:
    st.subheader("Inventory (FIFO Lots)")
    conn = get_pg(); cur = conn.cursor()
    cur.execute("""
      select lb.batch_id, b.inbound_date, lb.internal_sku, (lb.qty_in - lb.qty_sold) as qty_avail
      from lot_balance lb
      join batch b on b.batch_id = lb.batch_id
      order by b.inbound_date asc, lb.batch_id asc, lb.internal_sku asc;
    """)
    inv = cur.fetchall(); cur.close(); conn.close()
    st.dataframe(pd.DataFrame(inv, columns=["batch_id","inbound_date","internal_sku","qty_avail"]),
                 use_container_width=True)

# ========== Tab 5: Monthly ==========
with tabs[4]:
    st.subheader("Monthly Summary")
    conn = get_pg(); cur = conn.cursor()
    cur.execute("select ym, orders, fob_sum, freight_sum, clearance_sum, duty_sum, updated_at from month_summary order by ym desc;")
    ms = cur.fetchall(); cur.close(); conn.close()
    st.dataframe(pd.DataFrame(ms, columns=["ym","orders","fob_sum","freight_sum","clearance_sum","duty_sum","updated_at"]),
                 use_container_width=True)
    ym2 = st.text_input("Rebuild month (YYYY-MM)")
    colA, colB = st.columns(2)
    if colA.button("Re-run Normalize + Summary", disabled=not ym2):
        call_sql("select normalize_month_sales(%s);", (ym2,))
        call_sql("select summarize_month(%s);", (ym2,))
        st.success("Done.")
    if colB.button("Re-run FIFO for Month", disabled=not ym2):
        call_sql("select apply_fifo_for_month(%s);", (ym2,))
        st.success("FIFO recalculated.")

# ========== Tab 6: Admin ==========
with tabs[5]:
    st.subheader("Admin")
    st.caption("If something goes seriously wrong, you can full-reset schema here.")
    if st.button("Full Reset (Drop & Recreate ALL)", type="secondary"):
        from db import SCHEMA_SQL
        call_sql(SCHEMA_SQL)
        st.success("Schema re-created.")
