# app.py  —— Amazon FIFO Cost Portal (v2)
import os, io, json, datetime as dt
import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import execute_values

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_DSN = os.getenv("DB_DSN")  # postgresql://postgres:PASSWORD@HOST:5432/postgres

st.set_page_config(page_title="Amazon FIFO Cost Portal", layout="wide")

@st.cache_data(show_spinner=False)
def _now_ym():
    return dt.datetime.utcnow().strftime("%Y-%m")

def _pg():
    return psycopg2.connect(DB_DSN)

def run_sql(sql, args=None, many=False):
    with _pg() as conn, conn.cursor() as cur:
        if many:
            execute_values(cur, sql, args)
        else:
            cur.execute(sql, args)
        try:
            rows = cur.fetchall()
            return rows
        except psycopg2.ProgrammingError:
            return None

st.title("Amazon FIFO Cost Portal")

tabs = st.tabs(["Inbound", "SKU Map", "Sales Import", "Monthly", "Inventory", "Admin"])

# ---------------------- Inbound ----------------------
with tabs[0]:
    st.subheader("Add / Edit Batch (Inbound)")

    col0, col1, col2 = st.columns([1,1,1])
    with col0:
        batch_id = st.text_input("Batch ID", placeholder="e.g., AMZ2509").strip()
    with col1:
        inbound_date = st.date_input("Inbound Date", value=dt.date.today())
    with col2:
        st.caption("Totals for the whole container")
        freight_total = st.number_input("Freight Total", 0.0, step=10.0)
        clearance_total = st.number_input("Clearance Total", 0.0, step=10.0)

    st.divider()
    st.markdown("### Duty Pool by Category")
    duty_df = st.data_editor(
        pd.DataFrame(columns=["category","duty_total"]),
        num_rows="dynamic",
        use_container_width=True
    )

    st.markdown("### Items of this Batch")
    lot_df = st.data_editor(
        pd.DataFrame(columns=["internal_sku","category","qty_in","fob_unit","cbm_per_unit"]),
        num_rows="dynamic",
        use_container_width=True
    )

    if st.button("Save Batch & Allocate Costs", type="primary", disabled=(not batch_id)):
        with _pg() as conn, conn.cursor() as cur:
            # upsert batch
            cur.execute("""
                insert into batch(batch_id, inbound_date, freight_total, clearance_total)
                values (%s,%s,%s,%s)
                on conflict (batch_id) do update
                  set inbound_date=excluded.inbound_date,
                      freight_total=excluded.freight_total,
                      clearance_total=excluded.clearance_total
            """, (batch_id, inbound_date, freight_total, clearance_total))

            # replace tax pool for this batch
            cur.execute("delete from inbound_tax_pool where batch_id=%s", (batch_id,))
            if not duty_df.empty:
                execute_values(cur,
                    "insert into inbound_tax_pool(batch_id,category,duty_total) values %s",
                    [(batch_id, str(r.category), float(r.duty_total or 0)) for r in duty_df.itertuples()]
                )

            # replace inbound_lot for this batch
            cur.execute("delete from inbound_lot where batch_id=%s", (batch_id,))
            if not lot_df.empty:
                execute_values(cur, """
                    insert into inbound_lot(batch_id,internal_sku,category,qty_in,fob_unit,cbm_per_unit)
                    values %s
                """, [
                    (batch_id, str(r.internal_sku), str(r.category),
                     int(r.qty_in or 0),
                     None if pd.isna(r.fob_unit) else float(r.fob_unit),
                     None if pd.isna(r.cbm_per_unit) else float(r.cbm_per_unit))
                    for r in lot_df.itertuples()
                ])
        # allocate and sync inventory
        run_sql("select rebuild_lot_costs()")
        st.success("Batch saved and costs allocated.")

# ---------------------- SKU Map ----------------------
with tabs[1]:
    st.subheader("Amazon SKU → Internal SKU (support bundles)")
    st.caption("One Amazon SKU can map to multiple internal SKUs with ratios (quantity per order).")
    mp = st.selectbox("Marketplace", ["US","UK","DE","FR","ES","IT","CA","MX","JP","AE","AU"], index=0)
    m = st.data_editor(pd.DataFrame(columns=["amazon_sku","internal_sku","ratio"]), num_rows="dynamic", use_container_width=True)
    if st.button("Save Mapping"):
        with _pg() as conn, conn.cursor() as cur:
            cur.execute("delete from sku_map where marketplace=%s", (mp,))
            if not m.empty:
                execute_values(cur, """
                    insert into sku_map(marketplace,amazon_sku,internal_sku,ratio)
                    values %s
                """, [(mp, str(r.amazon_sku), str(r.internal_sku), float(r.ratio or 1)) for r in m.itertuples()])
        st.success("Mapping saved.")

# ---------------------- Sales Import ----------------------
with tabs[2]:
    st.subheader("Sales Import (Monthly Unified Transaction CSV)")
    ym = st.text_input("Year-Month (YYYY-MM)", value=_now_ym())
    up = st.file_uploader("Upload CSV", type=["csv"])
    st.caption("We only read: date/time, settlement type, order id, marketplace, sku, quantity")

    def normalize_csv(df: pd.DataFrame):
        # 兼容列名（去空格/小写）
        df.columns = [c.strip().lower() for c in df.columns]
        # 映射你给的真实表头
        need = {
            "date/time": "raw_time",
            "settlement type": "raw_type",
            "order id": "order_id",
            "marketplace": "marketplace",
            "sku": "amazon_sku",
            "quantity": "quantity",
        }
        miss = [c for c in need if c not in df.columns]
        if miss:
            raise ValueError(f"Missing columns: {miss}")
        out = pd.DataFrame({
            "raw_time": pd.to_datetime(df["date/time"], errors="coerce"),
            "raw_type": df["settlement type"].astype(str),
            "order_id": df["order id"].astype(str),
            "marketplace": df["marketplace"].fillna("US").astype(str),
            "amazon_sku": df["sku"].astype(str),
            "quantity": pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int),
        })
        # 原始整行也备份
        out["raw_payload"] = df.apply(lambda r: json.loads(r.to_json(force_ascii=False)), axis=1)
        return out.dropna(subset=["raw_time"])

    if up and st.button("Import & Run (Normalize → Summary → FIFO)", type="primary"):
        raw = pd.read_csv(up)
        data = normalize_csv(raw)
        # 仅保留该月行，避免误导入
        ym_start = pd.to_datetime(ym + "-01")
        ym_end = (ym_start + pd.offsets.MonthEnd(1)) + pd.Timedelta(days=1)
        data = data[(data["raw_time"] >= ym_start) & (data["raw_time"] < ym_end)]
        if data.empty:
            st.warning("No rows for target month.")
        else:
            tuples = list(data[["raw_time","raw_type","order_id","marketplace","amazon_sku","quantity","raw_payload"]].itertuples(index=False, name=None))
            with _pg() as conn, conn.cursor() as cur:
                execute_values(cur, """
                    insert into sales_raw(raw_time,raw_type,order_id,marketplace,amazon_sku,quantity,raw_payload)
                    values %s
                """, tuples)
            run_sql("select normalize_month_sales(%s)", (ym,))
            run_sql("select summarize_month(%s)", (ym,))
            run_sql("select apply_fifo_for_month(%s)", (ym,))
            st.success(f"Imported & computed for {ym}")

# ---------------------- Monthly ----------------------
with tabs[3]:
    st.subheader("Monthly Summary")
    ym_q = st.text_input("Year-Month to view", value=_now_ym(), key="ym_view")
    rows = run_sql("select ym, orders, fob_sum, freight_sum, clearance_sum, duty_sum, updated_at from month_summary where ym=%s", (ym_q,))
    if rows:
        cols = ["Month","Orders","FOB","Freight","Clearance","Duty","Updated"]
        st.dataframe(pd.DataFrame(rows, columns=cols), use_container_width=True)
    else:
        st.info("No summary yet.")

# ---------------------- Inventory ----------------------
with tabs[4]:
    st.subheader("Inventory by Lot (FIFO balance)")
    df = run_sql("""
      select lb.batch_id, b.inbound_date, lb.internal_sku, lb.qty_in, lb.qty_sold,
             (lb.qty_in - lb.qty_sold) as qty_left
      from lot_balance lb join batch b on b.batch_id=lb.batch_id
      order by b.inbound_date asc, lb.batch_id, lb.internal_sku
    """)
    if df:
        st.dataframe(pd.DataFrame(df, columns=["Batch","Inbound Date","Internal SKU","Qty In","Qty Sold","Qty Left"]),
                     use_container_width=True)
    else:
        st.info("No inventory.")

# ---------------------- Admin ----------------------
with tabs[5]:
    st.subheader("Admin")
    st.caption("These buttons change database data. Use carefully.")

    if st.button("Full Reset (Drop & Recreate ALL)", type="secondary"):
        # 直接在 Supabase 执行清理+重建（与 SQL 文件相同效果：你也可以保留 SQL 文件在手上）
        st.warning("Please run reset_and_install_v2.sql in Supabase SQL Editor for safety/audit.")
        st.stop()

    ym_admin = st.text_input("Recompute month (YYYY-MM)", value=_now_ym(), key="ym_admin")
    c1,c2,c3 = st.columns(3)
    if c1.button("Normalize Only"):
        run_sql("select normalize_month_sales(%s)", (ym_admin,))
        st.success("Normalized.")
    if c2.button("Summarize Only"):
        run_sql("select summarize_month(%s)", (ym_admin,))
        st.success("Summarized.")
    if c3.button("FIFO Only"):
        run_sql("select apply_fifo_for_month(%s)", (ym_admin,))
        st.success("FIFO done.")
