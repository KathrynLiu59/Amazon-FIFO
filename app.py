import streamlit as st
import pandas as pd
from dateutil import parser
from db import fetch_df, exec_sql, bulk_upsert

st.set_page_config(page_title="Amazon FIFO Portal", layout="wide")

st.title("Amazon FIFO – 一体化库存&成本")

def ym_of(dt_str):
    return pd.to_datetime(dt_str, utc=True).strftime("%Y-%m")

# --------- Tab 1: Inbound & Costs ---------
def page_inbound():
    st.subheader("Batch & Cost (批次+成本) / Duty Pools (按品类)")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**批次与成本（合并录入）**")
        df_bc = fetch_df("""
          select b.batch_id, b.arrived_at, b.dest_market, b.note,
                 coalesce(c.freight_total,0) as freight_total,
                 coalesce(c.clearance_total,0) as clearance_total
          from batch b
          left join batch_cost c on c.batch_id = b.batch_id
          order by b.arrived_at, b.batch_id
        """)
        edited_bc = st.data_editor(
            df_bc, num_rows="dynamic", use_container_width=True,
            column_config={
                "arrived_at": st.column_config.DateColumn(format="YYYY-MM-DD"),
                "freight_total": st.column_config.NumberColumn(step=0.01),
                "clearance_total": st.column_config.NumberColumn(step=0.01)
            }
        )
        if st.button("保存批次与成本", type="primary"):
            rows = []
            for _, r in edited_bc.fillna({"dest_market":"US","note":""}).iterrows():
                rows.append((r["batch_id"], r["arrived_at"], r["dest_market"], r["note"]))
            bulk_upsert("batch", ["batch_id","arrived_at","dest_market","note"], rows, ["batch_id"])

            rows = []
            for _, r in edited_bc.iterrows():
                rows.append((r["batch_id"], r.get("freight_total",0) or 0, r.get("clearance_total",0) or 0))
            bulk_upsert("batch_cost", ["batch_id","freight_total","clearance_total"], rows, ["batch_id"])
            st.success("批次与成本已保存")
            exec_sql("select rebuild_lot_costs()")

    with c2:
        st.markdown("**关税池（按批次+品类录入）**")
        df_dp = fetch_df("select batch_id, category, duty_total from duty_pool order by batch_id, category")
        edited_dp = st.data_editor(
            df_dp, num_rows="dynamic", use_container_width=True,
            column_config={"duty_total": st.column_config.NumberColumn(step=0.01)}
        )
        if st.button("保存关税池"):
            rows = [(r["batch_id"], r["category"], r.get("duty_total",0) or 0)
                    for _, r in edited_dp.iterrows() if r.get("batch_id") and r.get("category")]
            bulk_upsert("duty_pool", ["batch_id","category","duty_total"], rows, ["batch_id","category"])
            st.success("关税池已保存")
            exec_sql("select rebuild_lot_costs()")

    st.markdown("---")
    st.subheader("Inbound Items（入库明细）")
    df_inb = fetch_df("""
      select batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit
      from inbound_items order by batch_id, internal_sku
    """)
    edited_inb = st.data_editor(
        df_inb, num_rows="dynamic", use_container_width=True,
        column_config={
            "qty_in": st.column_config.NumberColumn(step=1),
            "fob_unit": st.column_config.NumberColumn(step=0.01),
            "cbm_per_unit": st.column_config.NumberColumn(step=0.0001)
        }
    )
    if st.button("保存入库明细"):
        rows = []
        for _, r in edited_inb.iterrows():
            if not r.get("batch_id") or not r.get("internal_sku"): 
                continue
            rows.append((
                r["batch_id"], r["internal_sku"], r.get("category"),
                int(r.get("qty_in") or 0), float(r.get("fob_unit") or 0), float(r.get("cbm_per_unit") or 0)
            ))
        bulk_upsert("inbound_items",
                    ["batch_id","internal_sku","category","qty_in","fob_unit","cbm_per_unit"],
                    rows, ["batch_id","internal_sku"])
        st.success("入库明细已保存")
        exec_sql("select rebuild_lot_costs(); select rebuild_lot_balance();")

# --------- Tab 2: Sales Upload ----------
def page_sales():
    st.subheader("Sales Upload（支持粘贴/CSV）")
    ym = st.text_input("处理月份（YYYY-MM）", value=pd.Timestamp.utcnow().strftime("%Y-%m"))
    help_cols = st.expander("查看需要的最小列（可自动映射）")
    with help_cols:
        st.markdown("- **order_id**, **amazon_sku**, **marketplace**, **date_time**, **qty**（退款负数）")

    uploader = st.file_uploader("上传 CSV（可空，直接在下表粘贴）", type=["csv"])
    if "sales_buf" not in st.session_state:
        st.session_state.sales_buf = pd.DataFrame(columns=["order_id","amazon_sku","marketplace","date_time","qty"])

    if uploader:
        tmp = pd.read_csv(uploader)
        # 容错映射：列名转小写去空格
        m = {c.lower().strip():c for c in tmp.columns}
        cols = ["order_id","sku","amazon_sku","marketplace","posted-date","date_time","quantity","qty"]
        def pick(*cands):
            for c in cands:
                if c in m: return m[c]
            return None
        tmp = pd.DataFrame({
            "order_id":   tmp.get(pick("order_id")),
            "amazon_sku": tmp.get(pick("amazon_sku","sku")),
            "marketplace":tmp.get(pick("marketplace")),
            "date_time":  tmp.get(pick("date_time","posted-date")),
            "qty":        tmp.get(pick("qty","quantity"))
        })
        st.session_state.sales_buf = tmp

    edit = st.data_editor(
        st.session_state.sales_buf, num_rows="dynamic", use_container_width=True,
        column_config={"date_time": st.column_config.DatetimeColumn()}
    )
    st.session_state.sales_buf = edit

    if st.button("写入 sales_raw 并运行该月", type="primary"):
        rows = []
        for _, r in edit.dropna(subset=["order_id","amazon_sku","date_time","qty"]).iterrows():
            rows.append((
                r["order_id"], r["amazon_sku"], r.get("marketplace","US"),
                pd.to_datetime(r["date_time"], utc=True), int(r["qty"]), ym, None
            ))
        # upsert
        bulk_upsert("sales_raw",
            ["order_id","amazon_sku","marketplace","date_time","qty","ym","raw_json"],
            rows, ["order_id","amazon_sku","date_time","marketplace"])
        exec_sql("select run_month(%s)", (ym,))
        st.success(f"{ym} 已运行完成")

# --------- Tab 3: Inventory ----------
def page_inventory():
    st.subheader("Inventory（当前库存）")
    df = fetch_df("select * from v_inventory_current order by internal_sku")
    st.dataframe(df, use_container_width=True)

# --------- Tab 4: Summary ----------
def page_summary():
    st.subheader("Monthly Summary（月汇总）")
    df = fetch_df("select * from v_monthly_summary")
    st.dataframe(df, use_container_width=True)
    ym = st.text_input("重跑月份（YYYY-MM）", value=pd.Timestamp.utcnow().strftime("%Y-%m"))
    c1, c2 = st.columns(2)
    if c1.button("重跑该月"):
        exec_sql("select run_month(%s)", (ym,))
        st.success(f"{ym} 已重跑")
    if c2.button("写月度快照"):
        exec_sql("select snapshot_month(%s)", (ym,))
        st.success("快照已写入")

# --------- Tab 5: Mapping ----------
def page_mapping():
    st.subheader("SKU Map（Amazon → Internal）")
    df_map = fetch_df("select amazon_sku, marketplace, internal_sku, unit_multiplier, active from sku_map order by marketplace, amazon_sku")
    edit_map = st.data_editor(df_map, num_rows="dynamic", use_container_width=True)
    if st.button("保存 SKU Map"):
        rows = []
        for _, r in edit_map.iterrows():
            if not r.get("amazon_sku") or not r.get("marketplace"): continue
            rows.append((r["amazon_sku"], r["marketplace"], r.get("internal_sku",""), float(r.get("unit_multiplier") or 1), bool(r.get("active") if r.get("active") is not None else True)))
        bulk_upsert("sku_map", ["amazon_sku","marketplace","internal_sku","unit_multiplier","active"],
                    rows, ["amazon_sku","marketplace"])
        st.success("SKU Map 已保存")

    st.markdown("---")
    st.subheader("Kit BOM（套装展开优先）")
    df_kit = fetch_df("select amazon_sku, marketplace, component_sku, component_qty, active from kit_bom order by marketplace, amazon_sku")
    edit_kit = st.data_editor(df_kit, num_rows="dynamic", use_container_width=True)
    if st.button("保存 Kit BOM"):
        rows = []
        for _, r in edit_kit.iterrows():
            if not r.get("amazon_sku") or not r.get("marketplace") or not r.get("component_sku"): 
                continue
            rows.append((r["amazon_sku"], r["marketplace"], r["component_sku"], float(r.get("component_qty") or 1), bool(r.get("active") if r.get("active") is not None else True)))
        bulk_upsert("kit_bom", ["amazon_sku","marketplace","component_sku","component_qty","active"],
                    rows, ["amazon_sku","marketplace","component_sku"])
        st.success("Kit BOM 已保存")

# --------- Tab 6: Adjustments ----------
def page_adjust():
    st.subheader("Adjustments（按订单号回滚/再跑）")
    ids = st.text_area("回滚的 Order IDs（每行一个）")
    c1, c2 = st.columns(2)
    if c1.button("回滚这些订单"):
        arr = [s.strip() for s in ids.splitlines() if s.strip()]
        if arr:
            exec_sql("select reverse_orders(%s::text[])", (arr,))
            st.success(f"已回滚 {len(arr)} 个订单")
            exec_sql("select rebuild_lot_balance()")
    ym = st.text_input("随后要重跑的月份（可留空）")
    if c2.button("回滚后重跑月份"):
        if ym:
            exec_sql("select run_month(%s)", (ym,))
            st.success(f"{ym} 已重跑")

tabs = st.tabs(["Inbound & Costs","Sales Upload","Inventory","Summary","Mapping","Adjustments"])
with tabs[0]: page_inbound()
with tabs[1]: page_sales()
with tabs[2]: page_inventory()
with tabs[3]: page_summary()
with tabs[4]: page_mapping()
with tabs[5]: page_adjust()
