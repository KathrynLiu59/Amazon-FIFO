# app.py
import io
import os
import datetime as dt
import pandas as pd
import streamlit as st

import psycopg  # 需要以 psycopg 3 方式使用
from db import fetchall
from loader import (
    insert_inbound_lot,
    upsert_duty_pool,
    insert_sales_txn,
    upsert_sku_map,
)
from worker import rebuild_costs, summarize_month

st.set_page_config(page_title="Amazon FIFO Cost Portal", layout="wide")

st.title("Amazon FIFO Cost Portal")

tabs = st.tabs([
    "Inbound (柜次入库)",
    "Sales Upload (月度明细)",
    "SKU Map (组合/映射)",
    "Inventory (库存&成本)",
    "Summaries (月度汇总)"
])

# ===================== 1) Inbound =====================
with tabs[0]:
    st.subheader("Inbound – paste items for a batch")

    c1, c2, c3 = st.columns([2,1,1])
    with c1:
        batch_id = st.text_input("Batch ID", placeholder="例如 WF2306")
    with c2:
        ship_date = st.date_input("Ship/Arrive Date", dt.date.today())
    with c3:
        st.caption("费用录入见表格右侧说明")

    st.markdown("**Paste items below** (columns: `internal_sku, category, qty_in, fob_unit, cbm_per_unit`)")

    inbound_df = st.data_editor(
        pd.DataFrame(columns=["internal_sku","category","qty_in","fob_unit","cbm_per_unit"]),
        num_rows="dynamic",
        use_container_width=True,
        key="inbound_editor",
        height=300
    )

    st.write("**Duty by Category for this batch (optional):**")
    duty_df = st.data_editor(
        pd.DataFrame(columns=["category","duty_total"]),
        num_rows="dynamic",
        use_container_width=True,
        key="duty_editor",
        height=180
    )

    commit = st.button("Commit Inbound")
    if commit:
        if not batch_id:
            st.error("Batch ID is required.")
        else:
            # 过滤空行并构造 rows
            idf = inbound_df.dropna(how="all")
            idf = idf[idf["internal_sku"].astype(str).str.len() > 0]
            rows = []
            for _, r in idf.iterrows():
                try:
                    rows.append((
                        batch_id,
                        str(r["internal_sku"]).strip(),
                        str(r.get("category","")).strip(),
                        float(r.get("qty_in",0) or 0),
                        float(r.get("fob_unit",0) or 0),
                        float(r.get("cbm_per_unit",0) or 0),
                    ))
                except Exception as e:
                    st.error(f"Row parse error: {e}")
                    rows = []
                    break
            n1 = insert_inbound_lot(rows) if rows else 0

            ddf = duty_df.dropna(how="all")
            ddf = ddf[ddf["category"].astype(str).str.len() > 0]
            duty_rows = []
            for _, r in ddf.iterrows():
                duty_rows.append((batch_id, str(r["category"]).strip(), float(r.get("duty_total",0) or 0)))
            n2 = upsert_duty_pool(duty_rows) if duty_rows else 0

            # 重算成本（将 freight/entry 按 CBM、duty 按 FOB 占比分摊到 SKU）
            try:
                rebuild_costs()
            except Exception as e:
                st.warning(f"rebuild_lot_costs() failed (you can still run it later): {e}")

            st.success(f"Inbound committed: items={n1}, duty_rows={n2}.")

# ===================== 2) Sales Upload =====================
with tabs[1]:
    st.subheader("Sales Upload – paste Amazon monthly CSV (Orders only)")

    st.caption("最低需要列: `date/time, type, order id, sku, quantity, marketplace`（你的导出名可能略有不同，下方有字段映射）。")

    file = st.file_uploader("Upload CSV", type=["csv"])
    if file:
        raw = pd.read_csv(file)
        st.write("Preview", raw.head())

        # 字段映射（按你的 Amazon 导出名称做默认，必要时可以改动）
        mapping = {
            "date/time": "happened_at",
            "type": "type",
            "order id": "order_id",
            "sku": "amazon_sku",
            "quantity": "qty",
            "marketplace": "marketplace",
        }
        # 容错：尝试大小写、空格等
        def find_col(df, target):
            for c in df.columns:
                if c.strip().lower() == target:
                    return c
            return None

        colmap = {}
        for src, dst in mapping.items():
            col = find_col(raw, src)
            if not col:
                st.error(f"CSV 缺少列: {src}")
            colmap[dst] = col

        ok = all(colmap.values())
        if ok:
            # 保留 type='order'
            df = raw.copy()
            df["__type_lower__"] = df[colmap["type"]].astype(str).str.lower()
            df = df[df["__type_lower__"] == "order"]
            # 构造 rows
            rows = []
            for _, r in df.iterrows():
                try:
                    happened = pd.to_datetime(r[colmap["happened_at"]], errors="coerce")
                    if pd.isna(happened):
                        continue
                    rows.append((
                        happened.to_pydatetime(),
                        "Order",
                        str(r[colmap["order_id"]]),
                        str(r[colmap["amazon_sku"]]),
                        float(r[colmap["qty"]] or 0),
                        str(r[colmap["marketplace"]]).upper().strip(),
                    ))
                except Exception as e:
                    st.warning(f"skip a row: {e}")
            if rows:
                n = insert_sales_txn(rows)
                st.success(f"Inserted order rows: {n}")
            else:
                st.info("没有可导入的订单行。")

# ===================== 3) SKU Map =====================
with tabs[2]:
    st.subheader("SKU Map – support kits / multi-mapping")

    st.markdown("为组合柜或命名不一致建立映射。可多行同一 AmazonSKU → 多个 InternalSKU + UnitMultiplier。")
    map_df = st.data_editor(
        pd.DataFrame(columns=["amazon_sku","marketplace","internal_sku","unit_multiplier"]),
        num_rows="dynamic",
        use_container_width=True,
        height=260,
        key="map_editor"
    )
    if st.button("Upsert Mapping"):
        mdf = map_df.dropna(how="all")
        mdf = mdf[mdf["amazon_sku"].astype(str).str.len() > 0]
        rows = []
        for _, r in mdf.iterrows():
            rows.append((
                str(r["amazon_sku"]).strip(),
                str(r.get("marketplace","US")).upper().strip() or "US",
                str(r["internal_sku"]).strip(),
                float(r.get("unit_multiplier",1) or 1),
            ))
        n = upsert_sku_map(rows) if rows else 0
        st.success(f"Upserted map rows: {n}")

# ===================== 4) Inventory =====================
with tabs[3]:
    st.subheader("Inventory & Cost (lot balance snapshot)")
    q = """
    select internal_sku, qty_on_hand, avg_fob_unit, avg_freight_unit, avg_duty_unit, avg_clearance_unit
    from lot_balance
    order by internal_sku
    """
    try:
        inv = fetchall(q)
        st.dataframe(pd.DataFrame(inv), use_container_width=True, height=380)
    except Exception as e:
        st.warning(f"Query lot_balance failed (did you create views/functions?): {e}")

# ===================== 5) Summary =====================
with tabs[4]:
    st.subheader("Monthly summary")
    ym = st.text_input("Year-Month (YYYY-MM)", (dt.date.today().replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m"))
    c1, c2 = st.columns([1,1])
    with c1:
        if st.button("Rebuild Costs Now"):
            try:
                rebuild_costs()
                st.success("rebuild_lot_costs() done.")
            except Exception as e:
                st.error(f"Rebuild failed: {e}")
    with c2:
        if st.button("Summarize Month"):
            try:
                summarize_month(ym)
                st.success(f"summarize_month({ym}) done.")
            except Exception as e:
                st.error(f"Summarize failed: {e}")

    try:
        ms = fetchall("select * from month_summary order by ym desc limit 24")
        st.dataframe(pd.DataFrame(ms), use_container_width=True, height=360)
    except Exception as e:
        st.info(f"No month_summary yet or view missing: {e}")
