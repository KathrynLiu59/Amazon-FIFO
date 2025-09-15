# app.py —— 多标签操作台（简洁但功能齐全）
import streamlit as st
import pandas as pd
from datetime import datetime
from db import rpc
from loader import (
    load_sales_raw_from_csv, upsert_batch, upsert_inbound_items,
    upsert_batch_cost_pool, upsert_batch_duty_pool,
    upsert_category, upsert_products, upsert_sku_map, upsert_kit_bom
)

st.set_page_config(page_title="Amazon FIFO Portal", layout="wide")
st.title("Amazon FIFO • Inventory & Costing")

tabs = st.tabs(["Inbound","Sales","Inventory","Summary","Mapping"])

# -------- Inbound --------
with tabs[0]:
    st.subheader("New Batch & Cost Pools")
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("**Batches**")
        df_b = st.data_editor(pd.DataFrame([{
            "batch_id":"", "container_no":"", "arrived_at":datetime.today().date(),
            "dest_market":"US", "note":""
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Batches"):
            upsert_batch(df_b.fillna("").to_dict("records"))
            st.success("Batches saved")

        st.markdown("**Inbound Items**")
        df_i = st.data_editor(pd.DataFrame([{
            "batch_id":"", "internal_sku":"", "category":"", "qty_in":0,
            "fob_unit":0, "cbm_per_unit":0, "weight_kg_per_unit":0, "duty_override_unit":None
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Inbound Items"):
            upsert_inbound_items(df_i.fillna("").to_dict("records"))
            st.success("Inbound items saved")

    with c2:
        st.markdown("**Freight & Clearance Pools**")
        df_f = st.data_editor(pd.DataFrame([{
            "batch_id":"", "freight_total":0, "clearance_total":0
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Freight/Clearance"):
            upsert_batch_cost_pool(df_f.fillna(0).to_dict("records"))
            st.success("Cost pool saved")

        st.markdown("**Duty Pools (by Category)**")
        df_d = st.data_editor(pd.DataFrame([{
            "batch_id":"", "category":"", "duty_total":0
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Duty Pools"):
            upsert_batch_duty_pool(df_d.fillna(0).to_dict("records"))
            st.success("Duty pool saved")

# -------- Sales --------
with tabs[1]:
    st.subheader("Upload Amazon Monthly CSV")
    c1, c2 = st.columns([2,1])
    with c1:
        file = st.file_uploader("Amazon Monthly Unified Transaction CSV", type=["csv"])
    with c2:
        marketplace = st.text_input("Marketplace", value="US")
        ym = st.text_input("Month (YYYY-MM)", value=datetime.today().strftime("%Y-%m"))
        run_btn = st.button("Run Month (Rebuild → FIFO → Summarize)")

    if file and st.button("Import Orders"):
        load_sales_raw_from_csv(file.getvalue(), marketplace)
        st.success("Orders imported to sales_raw")

    if run_btn:
        rpc("run_month", {"p_ym": ym, "p_market": marketplace if marketplace else None})
        st.success(f"Done: {ym} {marketplace or 'ALL'}")

# -------- Inventory --------
with tabs[2]:
    st.subheader("Current Inventory & Alerts")
    st.write("在 Supabase Table Editor 可直接查看 `lot_balance`。建议后续做专门的 REST 视图或 RPC 导出。")

# -------- Summary --------
with tabs[3]:
    st.subheader("Monthly Summary")
    st.write("查看 `month_summary` 与 `month_history`（可在 Supabase 直接预览/导出）。")

# -------- Mapping --------
with tabs[4]:
    st.subheader("Category / Products / SKU Map / Kits")
    c1,c2 = st.columns(2)
    with c1:
        st.markdown("**Category**")
        df_c = st.data_editor(pd.DataFrame([{"category":"","duty_rate_default":None}]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Categories"):
            upsert_category(df_c.fillna("").to_dict("records"))
            st.success("Categories saved")

        st.markdown("**Products**")
        df_p = st.data_editor(pd.DataFrame([{
            "internal_sku":"", "category":"", "weight_kg_per_unit":0, "cbm_per_unit":0, "active":True
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Products"):
            upsert_products(df_p.fillna("").to_dict("records"))
            st.success("Products saved")

    with c2:
        st.markdown("**SKU Map (Amazon → Internal)**")
        df_m = st.data_editor(pd.DataFrame([{
            "amazon_sku":"", "marketplace":"US", "internal_sku":"", "unit_multiplier":1, "active":True
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save SKU Map"):
            upsert_sku_map(df_m.fillna("").to_dict("records"))
            st.success("SKU Map saved")

        st.markdown("**Kit BOM (for bundles)**")
        df_k = st.data_editor(pd.DataFrame([{
            "amazon_sku":"", "marketplace":"US", "component_sku":"", "component_qty":1
        }]), num_rows="dynamic", use_container_width=True)
        if st.button("Save Kit BOM"):
            upsert_kit_bom(df_k.fillna("").to_dict("records"))
            st.success("Kit BOM saved")
