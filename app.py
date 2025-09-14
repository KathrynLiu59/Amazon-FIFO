
import streamlit as st
import pandas as pd
from datetime import datetime, date
from db import execute
from loader import import_inbound_items, import_inbound_tax, import_sales_csv
from worker import expand_movements, fifo_allocate, summarize, reverse_order

st.set_page_config(page_title="Amazon FIFO Costing", layout="wide")

st.title("Amazon FIFO Costing Portal")

tabs = st.tabs(["Inbound (Items)", "Inbound (Tax)", "Sales", "Process", "Inventory", "Summary", "Reverse"])

with tabs[0]:
    st.subheader("Inbound (Items)")
    f = st.file_uploader("Upload Inbound_Items Excel (sheet: items)", type=["xlsx"])
    if f and st.button("Import Items"):
        try:
            n = import_inbound_items(f.read())
            st.success(f"Imported {n} rows. Costs & balances rebuilt.")
        except Exception as e:
            st.error(str(e))

with tabs[1]:
    st.subheader("Inbound (Tax)")
    f = st.file_uploader("Upload Inbound_Tax Excel (sheets: tax_pool, tax_override optional)", type=["xlsx"])
    if f and st.button("Import Tax"):
        try:
            n = import_inbound_tax(f.read())
            st.success(f"Imported tax for {n} pool rows. Costs rebuilt.")
        except Exception as e:
            st.error(str(e))

with tabs[2]:
    st.subheader("Sales (Amazon CSV)")
    tz = st.text_input("Timezone (e.g., America/Toronto)", "UTC")
    f = st.file_uploader("Upload Amazon transaction CSV", type=["csv"])
    if f and st.button("Import CSV"):
        try:
            n = import_sales_csv(f.read(), tz=tz)
            st.success(f"Imported {n} order rows.")
        except Exception as e:
            st.error(str(e))

with tabs[3]:
    st.subheader("Process")
    d = st.date_input("Recompute From", value=date(date.today().year, date.today().month, 1))
    iso = datetime.combine(d, datetime.min.time()).isoformat()
    col1, col2, col3 = st.columns(3)
    if col1.button("Expand Movements"):
        try:
            expand_movements(iso)
            st.success("Expanded movements.")
        except Exception as e:
            st.error(str(e))
    if col2.button("FIFO Consume"):
        try:
            qty = fifo_allocate(iso)
            st.success(f"Allocated {qty} units.")
        except Exception as e:
            st.error(str(e))
    if col3.button("Summarize Month"):
        try:
            summarize()
            st.success("Summary updated.")
        except Exception as e:
            st.error(str(e))

with tabs[4]:
    st.subheader("Inventory")
    rows = execute("""
        select p.internal_sku, p.category, coalesce(sum(lb.qty_remaining),0) as qty_left
        from product p
        left join lot_balance lb on lb.internal_sku=p.internal_sku
        group by 1,2
        order by p.internal_sku;
    """)
    df = pd.DataFrame(rows, columns=["InternalSKU","Category","Qty_Left"])
    st.dataframe(df, use_container_width=True)

with tabs[5]:
    st.subheader("Monthly Summary")
    rows = execute("select month, fob, freight, duty, clearance, headhaul, orders from month_summary order by month desc limit 24;")
    df = pd.DataFrame(rows, columns=["Month","FOB","Freight","Duty","Clearance","Headhaul","Orders"])
    st.dataframe(df, use_container_width=True)

with tabs[6]:
    st.subheader("Reverse by Order ID")
    oid = st.text_input("Order ID")
    note = st.text_input("Note (optional)")
    if st.button("Reverse"):
        try:
            reverse_order(oid, note)
            st.success(f"Reversed allocations for order {oid}. Re-run Process to recompute.")
        except Exception as e:
            st.error(str(e))
