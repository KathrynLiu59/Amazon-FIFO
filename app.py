import io
import time
import pandas as pd
import streamlit as st
from datetime import datetime
from dateutil import tz
from db import run_sql, run_sql_nores

st.set_page_config(page_title="Amazon FIFO Portal", layout="wide")

# ---- Helpers ---------------------------------------------------------------
def _info(msg): st.success(msg)
def _warn(msg): st.warning(msg)
def _err(msg): st.error(msg)

def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def paste_or_upload(title, example_csv="", help_text="Paste CSV or upload file"):
    st.caption(help_text)
    ta = st.text_area(title, height=160, placeholder=example_csv)
    uploaded = st.file_uploader("…or upload CSV", type=["csv"], key=f"up_{title}")
    csv_text = None
    if uploaded is not None:
        csv_text = uploaded.read().decode("utf-8", errors="ignore")
    elif ta.strip():
        csv_text = ta
    if not csv_text:
        return None
    return pd.read_csv(io.StringIO(csv_text))

# ---- Header ----------------------------------------------------------------
st.title("Amazon FIFO Portal")

tabs = st.tabs(["Inbound", "Sales", "Adjustments", "Inventory", "Monthly Summary", "Admin"])

# ========== TAB 1: Inbound ===================================================
with tabs[0]:
    st.subheader("Inbound — Record new batch (container/lot)")
    colA, colB = st.columns([1,1])
    with colA:
        batch_id = st.text_input("Batch ID", placeholder="e.g. AMZ2509 / WF2306 …")
    with colB:
        happened_at = st.date_input("Inbound date", value=datetime.now().date())

    st.markdown("**Items in this batch**")
    # 用户可以直接在表格里粘贴多行
    default = pd.DataFrame(columns=["internal_sku", "category", "qty_in", "fob_per_unit", "cbm_per_unit"])
    df = st.data_editor(
        default, num_rows="dynamic", use_container_width=True,
        column_config={
            "internal_sku": st.column_config.TextColumn("Internal SKU", help="工厂/内部SKU"),
            "category": st.column_config.TextColumn("Category", help="例如 SD/淋浴房，SB/底盆"),
            "qty_in": st.column_config.NumberColumn("Qty In", min_value=0),
            "fob_per_unit": st.column_config.NumberColumn("FOB per unit"),
            "cbm_per_unit": st.column_config.NumberColumn("CBM per unit")
        }
    )

    st.caption("（如需粘贴：Excel 复制多行 → 点击表格第一行第一列 → ⌘/Ctrl+V）")

    col1, col2 = st.columns([1,1])
    with col1:
        do_commit = st.button("Commit inbound", type="primary")
    with col2:
        clear_btn = st.button("Clear inputs")

    if do_commit:
        if not batch_id.strip():
            _warn("Please enter Batch ID.")
        else:
            clean = df.dropna(how="all")
            if clean.empty:
                _warn("No rows to insert.")
            else:
                # 先写入 inbound_lot 基础信息
                # 注意：如果你的表结构是每行一条记录（batch_id+internal_sku唯一），这里直接插入
                rows = []
                for _, r in clean.iterrows():
                    rows.append((
                        batch_id.strip(),
                        str(r.get("internal_sku", "")).strip(),
                        str(r.get("category", "")).strip(),
                        int(r.get("qty_in") or 0),
                        float(r.get("fob_per_unit") or 0.0),
                        float(r.get("cbm_per_unit") or 0.0),
                        happened_at
                    ))

                sql = """
                    insert into inbound_lot
                      (batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit, happened_at)
                    values (%s,%s,%s,%s,%s,%s,%s)
                    on conflict (batch_id, internal_sku) do update set
                      category=excluded.category,
                      qty_in=excluded.qty_in,
                      fob_unit=excluded.fob_unit,
                      cbm_per_unit=excluded.cbm_per_unit,
                      happened_at=excluded.happened_at
                """
                run_sql(sql, rows, many=True)
                _info(f"Inbound committed: {batch_id}  ({len(rows)} rows)")

# ========== TAB 2: Sales =====================================================
with tabs[1]:
    st.subheader("Sales — Paste Amazon monthly transactions (Orders only)")
    mcol1, mcol2 = st.columns([1,1])
    with mcol1:
        month_str = st.text_input("Month (YYYY-MM)", value=month_key(datetime.now()))
    with mcol2:
        marketplace = st.text_input("Marketplace code", value="US", help="例如 US / UK / DE / FR …")

    st.markdown("**Paste or upload the Amazon Monthly Unified Transactions CSV**")
    ex = "date/time,type,sku,description,quantity,fulfillment,marketplace\n"
    ex += "Jan 3, 2025 2:13:30,Order,MVNSB1001WHLS,Example,1,amazon.com,US\n"
    sales_df = paste_or_upload("Transactions CSV (Orders only)", example_csv=ex)

    parse_btn = st.button("Upload & Store (Orders in this month)")
    if parse_btn:
        if sales_df is None or sales_df.empty:
            _warn("Please paste/upload the transaction CSV.")
        else:
            # 只保留 type == Order
            # 兼容不同表头：统一小写
            sdf = sales_df.copy()
            sdf.columns = [str(c).strip().lower() for c in sdf.columns]

            # 需要的最小字段：date/time, type, sku, quantity, marketplace
            needed = ["date/time", "type", "sku", "quantity"]
            for k in needed:
                if k not in sdf.columns:
                    _err(f"Missing column in CSV: {k}")
                    st.stop()

            orders = sdf[sdf["type"].str.lower().eq("order")].copy()
            if "marketplace" not in orders.columns:
                orders["marketplace"] = marketplace

            # 解析日期，仅保留目标月份
            def parse_dt(x):
                try:
                    return pd.to_datetime(x, errors="coerce")
                except Exception:
                    return pd.NaT

            orders["happened_at"] = orders["date/time"].map(parse_dt)
            if month_str:
                try:
                    y, m = month_str.split("-")
                    y, m = int(y), int(m)
                    in_month = (orders["happened_at"].dt.year == y) & (orders["happened_at"].dt.month == m)
                    orders = orders[in_month]
                except Exception:
                    pass

            # 准备入库字段：sku, qty, happened_at, marketplace
            rows = []
            for _, r in orders.iterrows():
                sku = str(r.get("sku", "")).strip()
                qty = int(float(r.get("quantity") or 0))
                if not sku or qty == 0 or pd.isna(r.get("happened_at")):
                    continue
                rows.append((r["happened_at"], "Order", sku, qty, r.get("marketplace", marketplace)))

            if not rows:
                _warn("No valid 'Order' rows for the month.")
            else:
                sql = """
                    insert into sales_txn (happened_at, type, amazon_sku, qty_sold, marketplace)
                    values (%s,%s,%s,%s,%s)
                """
                run_sql(sql, rows, many=True)
                _info(f"Stored {len(rows)} order rows for {month_str} / {marketplace}")

            st.divider()
            st.caption("Rebuild allocations & summaries for this month")
            do_rebuild = st.button("Rebuild month")
            if do_rebuild:
                # 依赖你在 SQL 里已创建的函数：rebuild_lot_costs() / summarize_months()
                # 如果你是按我们之前的脚本建的函数，可以直接调用：
                try:
                    run_sql_nores("select rebuild_lot_costs();")
                except Exception:
                    pass
                try:
                    run_sql_nores("select summarize_months();")
                except Exception:
                    pass
                _info("Rebuild done.")

# ========== TAB 3: Adjustments ==============================================
with tabs[2]:
    st.subheader("Adjustments — Return / Resale corrections by original order id")
    st.caption("当知道某个历史订单的退货/二次上架时，在此录入以回滚那笔订单的消耗与费用，再在新售出时重新扣减。")
    a1, a2, a3 = st.columns([1,1,1])
    with a1:
        adj_date = st.date_input("Adjustment date", value=datetime.now().date())
    with a2:
        order_id = st.text_input("Original order id")
    with a3:
        qty = st.number_input("Qty (+加库存 / -减库存)", value=1)

    notes = st.text_input("Note (optional)")
    do_adj = st.button("Commit adjustment")
    if do_adj:
        if not order_id.strip():
            _warn("Please enter original order id.")
        else:
            sql = """
              insert into adjustments (happened_at, order_id, qty, note)
              values (%s,%s,%s,%s)
            """
            run_sql(sql, [(adj_date, order_id.strip(), int(qty), notes or None)], many=True)
            _info("Adjustment stored. （必要时请在 Admin 页执行 Rebuild）")

# ========== TAB 4: Inventory =================================================
with tabs[3]:
    st.subheader("Inventory — Current lot balance by SKU")
    q = """
      select internal_sku, coalesce(sum(qty_remaining),0) as qty_remaining
      from lot_balance
      group by internal_sku
      order by internal_sku
    """
    try:
        inv = pd.DataFrame(run_sql(q))
        st.dataframe(inv, use_container_width=True)
    except Exception as e:
        _err(f"Failed to load inventory: {e}")

# ========== TAB 5: Monthly Summary ===========================================
with tabs[4]:
    st.subheader("Monthly Summary — FOB vs Freight&Duty")
    m = st.text_input("Month (YYYY-MM)", value=month_key(datetime.now()))
    mk = m.strip()
    if st.button("Load summary"):
        try:
            res = run_sql("""
                select month, marketplace, fob_total, freight_total, duty_total, clearance_total, landed_total
                from month_summary
                where month = %s
                order by marketplace
            """, (mk,))
            df = pd.DataFrame(res)
            if df.empty:
                _warn("No rows for this month. Did you run Rebuild after importing sales?")
            else:
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            _err(str(e))

# ========== TAB 6: Admin =====================================================
with tabs[5]:
    st.subheader("Admin")
    st.caption("当导入了销售或调整后，如果没有自动汇总，可以手动触发重算。")
    if st.button("Rebuild allocations & summaries (all)"):
        try:
            run_sql_nores("select rebuild_lot_costs();")
        except Exception:
            pass
        try:
            run_sql_nores("select summarize_months();")
        except Exception:
            pass
        _info("Rebuild complete.")
