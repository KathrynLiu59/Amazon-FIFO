# app.py
import io, os, datetime as dt
import pandas as pd
import streamlit as st

from db import fetchdf, run_sql
from loader import csv_to_df, parse_amz_unified_csv, upsert_df
from worker import (
    rebuild_lot_costs, build_sales_txn_for_month, run_fifo_allocation,
    summarize_month, reverse_order, replay_order, inventory_snapshot_df
)

st.set_page_config(page_title="Amazon FIFO Cost Portal", layout="wide")
st.title("Amazon FIFO Cost Portal")

# ============ 工具 ============
def _note_ok(msg): st.success(msg, icon="✅")
def _note_err(msg): st.error(msg, icon="❌")

# ============ 侧边栏 ============
st.sidebar.header("Quick Actions")
with st.sidebar:
    ym = st.text_input("Target Month (YYYY-MM)", value=dt.date.today().strftime("%Y-%m"))
    marketplace = st.selectbox("Marketplace", ["US", "EU"], index=0)

tabs = st.tabs(["📦 Inbound (New Container)", "🧾 Sales Upload", "⚙️ Run Month",
                "📊 Inventory", "📜 History / Adjustments", "🛠 Settings"])

# ============ Tab 1: 入库（新柜） ============
with tabs[0]:
    st.subheader("Create/Update Container (Batch) & Items, Taxes")
    col1, col2 = st.columns([1,1])
    with col1:
        st.markdown("**1) Batch header**  (sheet: `batch`)")
        batch_df = st.data_editor(
            pd.DataFrame([{
                "batch_id": "",
                "received_at": dt.date.today().isoformat(),
                "marketplace": marketplace,
                "freight_total": 0,
                "entryfees_total": 0,
                "note": ""
            }]),
            key="batch_edit", use_container_width=True, num_rows="dynamic"
        )
        if st.button("Commit Batch"):
            try:
                upsert_df("batch", batch_df, ["batch_id"])
                _note_ok("Batch upserted.")
            except Exception as e:
                _note_err(str(e))

    with col2:
        st.markdown("**2) Batch items** (sheet: `inbound_lot`)")
        items_df = st.data_editor(
            pd.DataFrame([{
                "batch_id": "",
                "internal_sku": "",
                "qty_in": 0,
                "fob_unit": 0,
                "cbm_per_unit": 0,
                "category": ""
            }]),
            key="items_edit", use_container_width=True, num_rows="dynamic"
        )
        if st.button("Commit Items"):
            try:
                upsert_df("inbound_lot", items_df, ["batch_id","internal_sku"])
                _note_ok("Inbound items upserted.")
            except Exception as e:
                _note_err(str(e))

    st.markdown("---")
    col3, col4 = st.columns([1,1])
    with col3:
        st.markdown("**3) Duty by Category (pool)** (sheet: `inbound_tax_pool`)")
        tax_pool_df = st.data_editor(
            pd.DataFrame([{
                "batch_id": "",
                "category": "",
                "duty_total": 0
            }]),
            key="tax_pool_edit", use_container_width=True, num_rows="dynamic"
        )
        if st.button("Commit Duty Pool"):
            try:
                upsert_df("inbound_tax_pool", tax_pool_df, ["batch_id","category"])
                _note_ok("Duty pool upserted.")
            except Exception as e:
                _note_err(str(e))
    with col4:
        st.markdown("**4) Duty override by item (optional)** (sheet: `inbound_tax_override`)")
        tax_item_df = st.data_editor(
            pd.DataFrame([{
                "batch_id": "",
                "internal_sku": "",
                "duty_amount": 0
            }]),
            key="tax_item_edit", use_container_width=True, num_rows="dynamic"
        )
        if st.button("Commit Duty Override"):
            try:
                upsert_df("inbound_tax_override", tax_item_df, ["batch_id","internal_sku"])
                _note_ok("Duty item override upserted.")
            except Exception as e:
                _note_err(str(e))

    st.markdown("---")
    if st.button("Rebuild Lot Costs (allocate freight/duty/entry)"):
        try:
            rebuild_lot_costs()
            _note_ok("Lot costs rebuilt.")
        except Exception as e:
            _note_err(str(e))

# ============ Tab 2: 销售上传 ============
with tabs[1]:
    st.subheader("Upload Amazon Monthly 'All Transactions' CSV")
    up_file = st.file_uploader("Upload CSV", type=["csv"])
    if up_file:
        try:
            raw = csv_to_df(up_file)
            st.caption(f"Raw rows: {len(raw)}")
            parsed = parse_amz_unified_csv(raw, ym=ym, marketplace=marketplace)
            st.dataframe(parsed.head(20), use_container_width=True)
            if st.button("Commit to sales_raw"):
                upsert_df("sales_raw", parsed, ["ym","marketplace","date_time","order_id","sku"])
                _note_ok("sales_raw imported.")
        except Exception as e:
            _note_err(str(e))

# ============ Tab 3: 跑当月 ============
with tabs[2]:
    st.subheader("Build sales_txn → FIFO allocate → Month summary")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("1) Build sales_txn for month"):
            try:
                build_sales_txn_for_month(ym)
                _note_ok("sales_txn built from sales_raw.")
            except Exception as e:
                _note_err(str(e))
    with c2:
        if st.button("2) Run FIFO allocation"):
            try:
                run_fifo_allocation(ym)
                _note_ok("FIFO allocation done.")
            except Exception as e:
                _note_err(str(e))
    with c3:
        if st.button("3) Summarize month"):
            try:
                summarize_month(ym)
                _note_ok("Month summarized.")
            except Exception as e:
                _note_err(str(e))

    st.markdown("#### Month summary (history)")
    ms = fetchdf("select * from month_summary order by ym desc limit 24")
    st.dataframe(ms, use_container_width=True)

# ============ Tab 4: 库存 ============
with tabs[3]:
    st.subheader("Inventory Snapshot & Threshold")
    inv = inventory_snapshot_df()
    st.dataframe(inv, use_container_width=True)
    st.markdown("**Threshold editing** (sheet: `inventory_threshold`)")
    cur = fetchdf("select * from inventory_threshold")
    cur = cur if not cur.empty else pd.DataFrame([{"internal_sku":"","warn_qty":0}])
    edited = st.data_editor(cur, num_rows="dynamic", use_container_width=True, key="warn_edit")
    if st.button("Save Threshold"):
        try:
            upsert_df("inventory_threshold", edited, ["internal_sku"])
            _note_ok("Threshold saved.")
        except Exception as e:
            _note_err(str(e))

# ============ Tab 5: 历史/调整 ============
with tabs[4]:
    st.subheader("Reverse or Replay an Order")
    oid = st.text_input("Order ID")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Reverse Order"):
            try:
                reverse_order(oid)
                _note_ok("Order reversed.")
            except Exception as e:
                _note_err(str(e))
    with c2:
        if st.button("Replay Order"):
            try:
                replay_order(oid)
                _note_ok("Order replayed.")
            except Exception as e:
                _note_err(str(e))

    st.markdown("#### Recent adjustments")
    ad = fetchdf("select * from adjustments order by created_at desc limit 50")
    st.dataframe(ad, use_container_width=True)

# ============ Tab 6: Settings ============
with tabs[5]:
    st.subheader("Mappings")
    st.caption("**SKU Map** (Amazon SKU → Internal SKU)")
    m = fetchdf("select * from sku_map order by amazonsku")
    m = m if not m.empty else pd.DataFrame([{
        "amazonsku":"", "marketplace": marketplace, "internal_sku":"", "unit_multiplier":1
    }])
    m2 = st.data_editor(m, num_rows="dynamic", use_container_width=True, key="sku_map_edit")
    if st.button("Save SKU Map"):
        try:
            upsert_df("sku_map", m2, ["amazonsku","marketplace","internal_sku"])
            _note_ok("SKU map saved.")
        except Exception as e:
            _note_err(str(e))

    st.divider()
    st.caption("**Kit / BOM** (Amazon kit SKU → internal sku components)")
    kb = fetchdf("select * from kit_bom order by amazonsku")
    kb = kb if not kb.empty else pd.DataFrame([{
        "amazonsku":"", "marketplace": marketplace, "internal_sku":"", "qty":1
    }])
    kb2 = st.data_editor(kb, num_rows="dynamic", use_container_width=True, key="kit_bom_edit")
    if st.button("Save Kit/BOM"):
        try:
            upsert_df("kit_bom", kb2, ["amazonsku","marketplace","internal_sku"])
            _note_ok("Kit/BOM saved.")
        except Exception as e:
            _note_err(str(e))
