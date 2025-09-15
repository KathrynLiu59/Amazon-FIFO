# app.py  — Amazon FIFO: Inventory & Costing Portal
# -----------------------------------------------
# Tabs:
#   1) Inbound & Costs   : Batch+Costs (single grid), Duty Pools (by Category), Inbound Items
#   2) Sales Upload      : Upload Amazon monthly CSV -> sales_raw
#   3) Inventory         : View lot_balance
#   4) Summary           : Run month, view month_summary
#   5) Mapping           : Maintain SKU Map / Kit BOM / Products
#
# All labels are professional English. The "Batch header" and "Freight/Clearance" are merged
# into ONE grid as you requested. No weight column. No "duty override". Duty is by category pool.

import os
import io
import json
import pandas as pd
import streamlit as st

# ---- DB helpers (psycopg v3) ----
try:
    import psycopg
except Exception as e:
    st.error("psycopg is not installed correctly. Please ensure requirements.txt has `psycopg[binary]==3.1.19` "
             "and runtime.txt is python-3.11.9")
    st.stop()


@st.cache_resource(show_spinner=False)
def get_conn():
    """
    Prefer a single DSN via secrets:
      st.secrets["DB_DSN"]  # Supabase 'Direct connection' string (postgres://...)
    Fallback to env var DB_DSN.
    """
    dsn = None
    if "DB_DSN" in st.secrets:
        dsn = st.secrets["DB_DSN"]
    elif os.environ.get("DB_DSN"):
        dsn = os.environ["DB_DSN"]

    if not dsn:
        st.error("Missing DB_DSN. Please paste your Supabase 'Direct connection' string into "
                 "Streamlit → Settings → Secrets as DB_DSN.")
        st.stop()

    return psycopg.connect(dsn, autocommit=True)


def fetch_df(sql: str, params: tuple | None = None) -> pd.DataFrame:
    with get_conn().cursor() as cur:
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def exec_sql(sql: str, params: tuple | None = None):
    with get_conn().cursor() as cur:
        cur.execute(sql, params or ())


def exec_many(sql: str, rows: list[tuple]):
    if not rows:
        return
    with get_conn().cursor() as cur:
        cur.executemany(sql, rows)


# ---------- UI helpers ----------
st.set_page_config(page_title="Amazon FIFO — Inventory & Costing", layout="wide")
st.title("Amazon FIFO — Inventory & Costing")

TABS = st.tabs(["Inbound & Costs", "Sales Upload", "Inventory", "Summary", "Mapping"])


# ========== 1) Inbound & Costs ==========
def page_inbound():
    st.subheader("Batch & Cost (single grid) / Duty Pools (by Category)")

    left, right = st.columns([2, 2])

    # -------- A. Batch + Costs (single grid) --------
    with left:
        st.markdown("**Batch & Cost (merged)**")
        batch_cols = [
            "batch_id", "arrived_at", "dest_market", "note",
            "freight_total", "clearance_total"
        ]
        # Load current data (merge batch + batch_cost)
        df_batch = fetch_df("""
            select b.batch_id,
                   b.arrived_at,
                   b.dest_market,
                   b.note,
                   coalesce(c.freight_total, 0)  as freight_total,
                   coalesce(c.clearance_total,0) as clearance_total
            from batch b
            left join batch_cost c on c.batch_id = b.batch_id
            order by b.arrived_at nulls last, b.batch_id
        """)
        if df_batch.empty:
            df_batch = pd.DataFrame(columns=batch_cols)

        df_batch = df_batch.reindex(columns=batch_cols)

        edited_batch = st.data_editor(
            df_batch,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "batch_id": st.column_config.TextColumn("Batch ID", required=True),
                "arrived_at": st.column_config.DateColumn("Arrived At"),
                "dest_market": st.column_config.TextColumn("Destination Market", help="e.g. US / EU"),
                "note": st.column_config.TextColumn("Note"),
                "freight_total": st.column_config.NumberColumn("Freight Total"),
                "clearance_total": st.column_config.NumberColumn("Clearance Total")
            },
            key="grid_batch"
        )

        if st.button("Save Batch & Costs", type="primary", use_container_width=True):
            # Upsert into batch + batch_cost
            rows = []
            cost_rows = []
            for _, r in edited_batch.fillna("").iterrows():
                rows.append((
                    r["batch_id"], r["arrived_at"] if r["arrived_at"] != "" else None,
                    r["dest_market"], r["note"]
                ))
                cost_rows.append((
                    r["batch_id"],
                    float(r["freight_total"] or 0),
                    float(r["clearance_total"] or 0)
                ))

            exec_many("""
                insert into batch(batch_id, arrived_at, dest_market, note)
                values (%s, %s, %s, %s)
                on conflict(batch_id) do update set
                  arrived_at = excluded.arrived_at,
                  dest_market = excluded.dest_market,
                  note = excluded.note
            """, rows)

            exec_many("""
                insert into batch_cost(batch_id, freight_total, clearance_total)
                values (%s, %s, %s)
                on conflict(batch_id) do update set
                  freight_total = excluded.freight_total,
                  clearance_total = excluded.clearance_total
            """, cost_rows)

            st.success("Batch & Costs saved.")

    # -------- B. Duty Pools (by Category) --------
    with right:
        st.markdown("**Duty Pools (by Category)**")
        duty_cols = ["batch_id", "category", "duty_total"]
        df_duty = fetch_df("""
            select batch_id, category, duty_total
            from duty_pool
            order by batch_id, category
        """)
        if df_duty.empty:
            df_duty = pd.DataFrame(columns=duty_cols)
        df_duty = df_duty.reindex(columns=duty_cols)

        edited_duty = st.data_editor(
            df_duty,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "batch_id": st.column_config.TextColumn("Batch ID", required=True),
                "category": st.column_config.TextColumn("Category", required=True),
                "duty_total": st.column_config.NumberColumn("Duty Total")
            },
            key="grid_duty"
        )

        if st.button("Save Duty Pools", use_container_width=True):
            duty_rows = []
            for _, r in edited_duty.fillna("").iterrows():
                duty_rows.append((
                    r["batch_id"], r["category"], float(r["duty_total"] or 0)
                ))
            exec_many("""
                insert into duty_pool(batch_id, category, duty_total)
                values (%s, %s, %s)
                on conflict(batch_id, category) do update set
                  duty_total = excluded.duty_total
            """, duty_rows)
            st.success("Duty pools saved.")

    st.markdown("---")

    # -------- C. Inbound Items --------
    st.markdown("**Inbound Items**")
    in_cols = ["batch_id", "internal_sku", "category", "qty_in", "fob_unit", "cbm_per_unit"]
    df_in = fetch_df("""
        select batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit
        from inbound_items order by batch_id, internal_sku
    """)
    if df_in.empty:
        df_in = pd.DataFrame(columns=in_cols)
    df_in = df_in.reindex(columns=in_cols)

    edited_in = st.data_editor(
        df_in,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "batch_id": st.column_config.TextColumn("Batch ID", required=True),
            "internal_sku": st.column_config.TextColumn("Internal SKU", required=True),
            "category": st.column_config.TextColumn("Category"),
            "qty_in": st.column_config.NumberColumn("Qty In"),
            "fob_unit": st.column_config.NumberColumn("FOB per Unit"),
            "cbm_per_unit": st.column_config.NumberColumn("CBM per Unit"),
        },
        key="grid_inbound"
    )

    colA, colB = st.columns(2)
    with colA:
        if st.button("Save Inbound Items", type="primary", use_container_width=True):
            rows = []
            for _, r in edited_in.fillna("").iterrows():
                rows.append((
                    r["batch_id"], r["internal_sku"], r["category"],
                    int(r["qty_in"] or 0),
                    float(r["fob_unit"] or 0.0),
                    float(r["cbm_per_unit"] or 0.0),
                ))
            exec_many("""
                insert into inbound_items(batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit)
                values (%s, %s, %s, %s, %s, %s)
                on conflict(batch_id, internal_sku) do update set
                  category = excluded.category,
                  qty_in = excluded.qty_in,
                  fob_unit = excluded.fob_unit,
                  cbm_per_unit = excluded.cbm_per_unit
            """, rows)
            st.success("Inbound items saved.")

    with colB:
        if st.button("Rebuild Costs & Inventory", use_container_width=True):
            # 分摊成本 + 刷新余额
            exec_sql("select rebuild_lot_costs();")
            exec_sql("select rebuild_lot_balance();")
            st.success("Costs and lot balance rebuilt.")


# ========== 2) Sales Upload ==========
def page_sales_upload():
    st.subheader("Upload Monthly Sales CSV (Amazon export) → sales_raw")
    ym = st.text_input("Year-Month (YYYY-MM)", value="")
    file = st.file_uploader("Upload Amazon monthly CSV", type=["csv"])

    if st.button("Import to sales_raw", type="primary", disabled=not (file and ym)):
        try:
            raw = pd.read_csv(file)
        except Exception:
            raw = pd.read_csv(file, encoding="utf-8", engine="python")

        # 这里做一个最小映射（示例：你们导出的列名请按需调整）
        # 目标列：ym, date_time, marketplace, order_id, amazon_sku, qty
        # —— 若你的样例文件列名不同，请告诉我；我再按你的模板改对应字段。
        cols = raw.columns.str.lower()
        raw.columns = cols

        def pick(*names, default=None):
            for n in names:
                if n in raw.columns:
                    return raw[n]
            return default

        df = pd.DataFrame({
            "ym": ym,
            "date_time": pick("date/time", "date_time", "purchase-date"),
            "marketplace": pick("marketplace", "marketplace-domain", default="amazon.com"),
            "order_id": pick("order id", "order-id", "order_id"),
            "amazon_sku": pick("sku", "seller-sku", "asin", "amazon_sku"),
            "qty": pd.to_numeric(pick("quantity", "quantity-purchased", "qty", default=0), errors="coerce").fillna(0).astype(int)
        })

        df = df.dropna(subset=["date_time", "order_id", "amazon_sku"])  # 基本清洗
        rows = [tuple(x) for x in df[["ym","date_time","marketplace","order_id","amazon_sku","qty"]].values.tolist()]
        exec_many("""
            insert into sales_raw(ym, date_time, marketplace, order_id, amazon_sku, qty)
            values (%s, %s, %s, %s, %s, %s)
        """, rows)
        st.success(f"Imported {len(rows)} rows into sales_raw.")


# ========== 3) Inventory ==========
def page_inventory():
    st.subheader("Current Inventory (lot_balance)")
    df = fetch_df("""
        select internal_sku, batch_id, qty_remaining
        from lot_balance
        order by internal_sku, batch_id
    """)
    st.dataframe(df, use_container_width=True)


# ========== 4) Summary ==========
def page_summary():
    st.subheader("Run Month & View Summary")
    ym = st.text_input("Year-Month (YYYY-MM)", value="")
    c1, c2 = st.columns([1,2])
    with c1:
        if st.button("Run Month (map → costs → FIFO → summarize)", type="primary", disabled=not ym):
            exec_sql("select run_month(%s);", (ym,))
            st.success(f"Month {ym} processed.")
    with c2:
        if st.button("Snapshot Month (inventory & summary)", disabled=not ym):
            exec_sql("select snapshot_month(%s);", (ym,))
            st.info(f"Snapshot for {ym} created in month_history.")

    st.markdown("**month_summary**")
    df = fetch_df("select ym, orders, units, cogs, updated_at from month_summary order by ym desc limit 24;")
    st.dataframe(df, use_container_width=True)


# ========== 5) Mapping ==========
def page_mapping():
    st.subheader("SKU Map / Kit BOM / Products")

    c1, c2 = st.columns(2)

    # SKU Map
    with c1:
        st.markdown("**SKU Map (Amazon → Internal)**")
        df = fetch_df("""
          select amazon_sku, marketplace, internal_sku, unit_multiplier, active
          from sku_map order by amazon_sku
        """)
        if df.empty:
            df = pd.DataFrame(columns=["amazon_sku","marketplace","internal_sku","unit_multiplier","active"])
        edit = st.data_editor(
            df, num_rows="dynamic", use_container_width=True,
            column_config={
                "amazon_sku": st.column_config.TextColumn("Amazon SKU", required=True),
                "marketplace": st.column_config.TextColumn("Marketplace", required=True),
                "internal_sku": st.column_config.TextColumn("Internal SKU", required=True),
                "unit_multiplier": st.column_config.NumberColumn("Unit Multiplier"),
                "active": st.column_config.CheckboxColumn("Active")
            },
            key="map_grid"
        )
        if st.button("Save SKU Map", use_container_width=True):
            rows = []
            for _, r in edit.fillna({"unit_multiplier":1,"active":True}).iterrows():
                rows.append((
                    r["amazon_sku"], r["marketplace"], r["internal_sku"],
                    int(r["unit_multiplier"] or 1), bool(r["active"])
                ))
            exec_many("""
                insert into sku_map(amazon_sku, marketplace, internal_sku, unit_multiplier, active)
                values(%s,%s,%s,%s,%s)
                on conflict(amazon_sku, marketplace) do update set
                  internal_sku = excluded.internal_sku,
                  unit_multiplier = excluded.unit_multiplier,
                  active = excluded.active
            """, rows)
            st.success("SKU Map saved.")

    # Kit BOM
    with c2:
        st.markdown("**Kit BOM (for bundles)**")
        df = fetch_df("""
          select amazon_sku, marketplace, component_sku, component_qty
          from kit_bom order by amazon_sku, component_sku
        """)
        if df.empty:
            df = pd.DataFrame(columns=["amazon_sku","marketplace","component_sku","component_qty"])
        edit = st.data_editor(
            df, num_rows="dynamic", use_container_width=True,
            column_config={
                "amazon_sku": st.column_config.TextColumn("Amazon SKU", required=True),
                "marketplace": st.column_config.TextColumn("Marketplace", required=True),
                "component_sku": st.column_config.TextColumn("Component SKU", required=True),
                "component_qty": st.column_config.NumberColumn("Component Qty", required=True),
            },
            key="bom_grid"
        )
        if st.button("Save Kit BOM", use_container_width=True):
            rows = []
            for _, r in edit.fillna({"component_qty":1}).iterrows():
                rows.append((
                    r["amazon_sku"], r["marketplace"], r["component_sku"], int(r["component_qty"] or 1)
                ))
            exec_many("""
                insert into kit_bom(amazon_sku, marketplace, component_sku, component_qty)
                values(%s,%s,%s,%s)
                on conflict(amazon_sku, marketplace, component_sku) do update set
                  component_qty = excluded.component_qty
            """, rows)
            st.success("Kit BOM saved.")

    st.markdown("---")
    st.markdown("**Products (Internal catalog)**")
    dfp = fetch_df("""
      select internal_sku, category, cbm_per_unit, active
      from product order by internal_sku
    """)
    if dfp.empty:
        dfp = pd.DataFrame(columns=["internal_sku","category","cbm_per_unit","active"])
    editp = st.data_editor(
        dfp, num_rows="dynamic", use_container_width=True,
        column_config={
            "internal_sku": st.column_config.TextColumn("Internal SKU", required=True),
            "category": st.column_config.TextColumn("Category"),
            "cbm_per_unit": st.column_config.NumberColumn("CBM per Unit"),
            "active": st.column_config.CheckboxColumn("Active")
        },
        key="prod_grid"
    )
    if st.button("Save Products", use_container_width=True):
        rows = []
        for _, r in editp.fillna({"cbm_per_unit":0,"active":True}).iterrows():
            rows.append((r["internal_sku"], r["category"], float(r["cbm_per_unit"] or 0), bool(r["active"])))
        exec_many("""
            insert into product(internal_sku, category, cbm_per_unit, active)
            values (%s,%s,%s,%s)
            on conflict(internal_sku) do update set
              category = excluded.category,
              cbm_per_unit = excluded.cbm_per_unit,
              active = excluded.active
        """, rows)
        st.success("Products saved.")


# ---------- Render ----------
with TABS[0]:
    page_inbound()

with TABS[1]:
    page_sales_upload()

with TABS[2]:
    page_inventory()

with TABS[3]:
    page_summary()

with TABS[4]:
    page_mapping()
