# app.py
import os
import io
import pandas as pd
import streamlit as st

from loader import load_sales_raw_from_csv
from worker import run_all, last_runs

st.set_page_config(page_title="Amazon FIFO Cost Portal", layout="wide")

st.title("Amazon FIFO Cost Portal")

st.sidebar.header("One-click Close")
with st.sidebar.form("one_click"):
    ym = st.text_input("Month (YYYY-MM)", value="2025-03", help="e.g. 2025-03")
    marketplace = st.text_input("Marketplace (optional)", value="", help="US / EU，留空=全部")
    submitted = st.form_submit_button("Run All")
    if submitted:
        if not ym or len(ym) != 7:
            st.error("Please input month as YYYY-MM.")
        else:
            with st.spinner("Running full pipeline..."):
                run_all(ym, marketplace or None)
            st.success("Done.")

st.sidebar.subheader("Recent Runs")
runs = last_runs()
for r in runs:
    st.sidebar.write(f"• {r['ym']} / {r['marketplace'] or 'ALL'} — {r['started_at']} → {r.get('finished_at')}")

tabs = st.tabs([
    "Upload Orders",
    "Month Summary",
    "Orders (Allocated)",
    "Inventory",
])

# ============ Tab 1: Upload Orders ============
with tabs[0]:
    st.subheader("Upload Amazon Monthly CSV → sales_raw")
    f = st.file_uploader("Choose CSV", type=["csv"])
    if f:
        try:
            n = load_sales_raw_from_csv(f.read())
            st.success(f"Inserted {n} rows into sales_raw.")
        except Exception as e:
            st.exception(e)

# ============ Tab 2: Month Summary ============
from db import run_sql

with tabs[1]:
    st.subheader("Month Summary")
    col1, col2 = st.columns(2)
    with col1:
        ym_q = st.text_input("Query month (YYYY-MM)", value=ym)
    with col2:
        mkt_q = st.text_input("Filter marketplace (optional)", value="")
    q = """
    select ym, marketplace, orders, qty_total, fob_total, freight_total, duty_total, clearance_total, landed_total, updated_at
    from month_summary
    where ym = %s
      and (%s = '' or marketplace = %s or ('ALL' = %s and marketplace='ALL'))
    order by marketplace
    """
    data = run_sql(q, (ym_q, mkt_q, mkt_q, mkt_q))
    if data:
        st.dataframe(pd.DataFrame(data))
    else:
        st.info("No records.")

# ============ Tab 3: Orders (Allocated) ============
with tabs[2]:
    st.subheader("Allocated Orders (FIFO result)")
    ym_q2 = st.text_input("Month (YYYY-MM)", value=ym, key="al1")
    mkt_q2 = st.text_input("Marketplace (optional)", value="", key="al2")
    q2 = """
      select happened_at, marketplace, order_id, internal_sku, batch_id, qty_from_batch,
             fob_unit, freight_unit, duty_unit, clearance_unit, landed_unit,
             ext_fob, ext_freight, ext_duty, ext_clearance, ext_landed
      from allocation_detail
      where ym = %s
        and (%s = '' or marketplace = %s)
      order by happened_at, order_id, internal_sku, batch_id
    """
    data2 = run_sql(q2, (ym_q2, mkt_q2, mkt_q2))
    if data2:
        st.dataframe(pd.DataFrame(data2))
    else:
        st.info("No records.")

# ============ Tab 4: Inventory ============
with tabs[3]:
    st.subheader("Inventory (Lot Balance)")
    sku_q = st.text_input("Internal SKU contains", value="")
    q3 = """
      select lb.internal_sku, lb.batch_id, lb.inbound_date, lb.qty_remaining,
             lc.fob_unit, lc.freight_unit, lc.duty_unit, lc.clearance_unit, lc.landed_unit
      from lot_balance lb
      left join lot_cost lc on lc.batch_id = lb.batch_id and lc.internal_sku = lb.internal_sku
      where (%s = '' or lb.internal_sku ilike %s)
      order by lb.internal_sku, lb.inbound_date, lb.batch_id
    """
    param = f"%{sku_q}%"
    data3 = run_sql(q3, (sku_q, param))
    if data3:
        st.dataframe(pd.DataFrame(data3))
    else:
        st.info("No records.")
