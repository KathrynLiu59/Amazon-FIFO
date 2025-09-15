import streamlit as st
import pandas as pd
from datetime import datetime, date
from db import execute, df_insert, df_upsert, fetch_df
from loader import import_sales_csv, parse_paste
from worker import expand_movements, fifo_allocate, summarize, reverse_order

st.set_page_config(page_title="Amazon FIFO Portal", layout="wide")
st.title("Amazon FIFO Portal")

tabs = st.tabs([
    "Inbound Upload", "Tax Upload", "Sales Upload", "Inventory",
    "Adjustments", "Reports", "Master Data", "Kit BOM", "Admin"
])

# =============== Inbound Upload ===============
with tabs[0]:
    st.subheader("New Inbound Batch (build per-unit costs only)")
    with st.form("batch_header"):
        c1, c2, c3 = st.columns(3)
        batch_id = c1.text_input("Batch ID", placeholder="e.g., WF2305")
        inbound_date = c2.date_input("Inbound Date")
        freight_total = c1.number_input("Freight Total", min_value=0.0, step=100.0, value=0.0)
        clearance_total = c2.number_input("Clearance Total", min_value=0.0, step=50.0, value=0.0)
        submitted_header = st.form_submit_button("Save Batch Header")
    if submitted
