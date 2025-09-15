# app.py — Streamlit 入口
import streamlit as st
import pandas as pd
from db import get_conn
from loader import load_sales_raw_from_csv
from worker import run_all, last_runs

st.set_page_config(page_title="Amazon FIFO | Cost Portal", layout="wide")

st.title("Amazon FIFO Cost Portal")

tab_in, tab_sales, tab_close, tab_inv, tab_logs = st.tabs([
    "Inbound & Costs", "Sales Upload", "Month Close", "Inventory", "Logs"
])

with tab_in:
    st.subheader("1) Record inbound lots / costs")

    st.markdown("**Inbound Lot (Header)**")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        batch_id = st.text_input("Batch ID *")
    with col2:
        inbound_date = st.date_input("Inbound Date *")
    with col3:
        marketplace = st.selectbox("Marketplace *", ["US", "EU", "JP"], index=0)
    with col4:
        container_no = st.text_input("Container #", "")

    if st.button("Upsert Lot Header"):
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
              insert into inbound_lot(batch_id, inbound_date, marketplace, container_no)
              values (%s,%s,%s,%s)
              on conflict (batch_id, marketplace) do update
                set inbound_date=excluded.inbound_date,
                    container_no=excluded.container_no;
            """, (batch_id, inbound_date, marketplace, container_no))
            conn.commit()
        st.success("Lot header saved.")

    st.markdown("---")
    st.markdown("**Inbound Items (paste table)**")
    st.caption("Columns: internal_sku | qty_in | fob_unit | cbm_per_unit | weight_kg")
    data_items = st.data_editor(pd.DataFrame(columns=["internal_sku","qty_in","fob_unit","cbm_per_unit","weight_kg"]),
                                num_rows="dynamic", use_container_width=True, key="items_editor")

    if st.button("Commit Items to Lot"):
        df = data_items.dropna(subset=["internal_sku"])
        rows = [ (batch_id, marketplace, r.internal_sku, float(r.qty_in), float(r.fob_unit), float(r.cbm_per_unit or 0), float(r.weight_kg or 0))
                  for r in df.itertuples(index=False) ]
        with get_conn() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                  insert into inbound_items(batch_id, marketplace, internal_sku, qty_in, fob_unit, cbm_per_unit, weight_kg)
                  values (%s,%s,%s,%s,%s,%s,%s)
                  on conflict (batch_id, marketplace, internal_sku) do update
                    set qty_in=excluded.qty_in, fob_unit=excluded.fob_unit, cbm_per_unit=excluded.cbm_per_unit, weight_kg=excluded.weight_kg;
                """, r)
            conn.commit()
        st.success("Inbound items saved.")

    st.markdown("---")
    st.markdown("**Duty/Freight/Entry by Category**")
    st.caption("按‘品类’输入一条柜的三类费用（可多行）")
    tax_df = st.data_editor(pd.DataFrame(columns=["category","freight_total","entry_total","duty_total"]),
                            num_rows="dynamic", use_container_width=True, key="tax_editor")

    if st.button("Commit Category Pools"):
        df = tax_df.dropna(subset=["category"])
        rows = [ (batch_id, marketplace, r.category, float(r.freight_total or 0), float(r.entry_total or 0), float(r.duty_total or 0))
                for r in df.itertuples(index=False)]
        with get_conn() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                  insert into inbound_tax_pool(batch_id, marketplace, category, freight_total, entry_total, duty_total)
                  values (%s,%s,%s,%s,%s,%s)
                  on conflict (batch_id, marketplace, category) do update
                    set freight_total=excluded.freight_total,
                        entry_total=excluded.entry_total,
                        duty_total=excluded.duty_total;
                """, r)
            conn.commit()
        st.success("Pools saved. Next: Build lot costs & balance.")

    if st.button("Build lot_cost & lot_balance (this lot)"):
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("select build_lot_costs(%s,%s);", (batch_id, marketplace))
            cur.execute("select rebuild_lot_balance(%s,%s);", (batch_id, marketplace))
            conn.commit()
        st.success("lot_cost and lot_balance ready ✅")

with tab_sales:
    st.subheader("2) Upload Monthly Sales CSV")
    st.caption("粘贴亚马逊 Monthly Unified Transaction CSV（整月），系统只取 'Order' 和 'Refund'。")

    marketplace = st.selectbox("Marketplace", ["US","EU","JP"], index=0, key="market_sale")
    file = st.file_uploader("Upload CSV", type=["csv"])
    if file:
        stats = load_sales_raw_from_csv(file.read(), marketplace)
        st.success(f"Loaded rows by month: {stats}")

    st.markdown("---")
    st.markdown("**Kit Map**（组合柜映射）")
    st.caption("列: marketplace | amazon_sku | internal_sku | unit_multiplier | combo_group(可选)")
    kit_df = st.data_editor(pd.DataFrame(columns=["marketplace","amazon_sku","internal_sku","unit_multiplier","combo_group"]),
                            num_rows="dynamic", use_container_width=True, key="kit_editor")
    if st.button("Upsert SKU Map"):
        df = kit_df.dropna(subset=["marketplace","amazon_sku","internal_sku","unit_multiplier"])
        with get_conn() as conn, conn.cursor() as cur:
            for r in df.itertuples(index=False):
                cg = getattr(r, "combo_group", None) or 'default'
                cur.execute("""
                    insert into sku_map(marketplace, amazon_sku, internal_sku, unit_multiplier, combo_group)
                    values(%s,%s,%s,%s,%s)
                    on conflict (marketplace, amazon_sku, internal_sku, combo_group) do update
                      set unit_multiplier=excluded.unit_multiplier;
                """, (r.marketplace, r.amazon_sku, r.internal_sku, float(r.unit_multiplier), cg))
            conn.commit()
        st.success("SKU Map saved.")

with tab_close:
    st.subheader("3) Month Close (Explode kits → FIFO → Summaries)")
    ym = st.text_input("Month (YYYY-MM) *", placeholder="2025-03")
    marketplace = st.selectbox("Marketplace", ["US","EU","JP"], index=0, key="market_close")
    combo = st.text_input("Combo group (optional)", value="default")

    if st.button("Run Month Close"):
        run_all(ym=ym, marketplace=marketplace, combo_group=combo)
        st.success("Month closed. Summaries updated & snapshot stored.")

with tab_inv:
    st.subheader("Inventory (by internal SKU)")
    marketplace = st.selectbox("Marketplace", ["US","EU","JP"], index=0, key="market_inv")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("select * from v_inventory where marketplace=%s order by internal_sku;", (marketplace,))
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["marketplace","internal_sku","qty"])
    st.dataframe(df, use_container_width=True)

with tab_logs:
    st.subheader("Recent Month Summaries")
    rows = last_runs()
    if rows:
        st.dataframe(pd.DataFrame(rows, columns=["ym","marketplace","orders","units","fob_total","freight_total","entry_total","duty_total","updated_at"]),
                     use_container_width=True)
    else:
        st.info("No summaries yet.")
