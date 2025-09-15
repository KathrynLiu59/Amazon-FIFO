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

# -------- Inbound Upload --------
with tabs[0]:
    st.subheader("New Inbound Batch")
    with st.form("batch_header"):
        c1, c2, c3 = st.columns(3)
        batch_id = c1.text_input("Batch ID", placeholder="e.g., WF2305")
        inbound_date = c2.date_input("Inbound Date")
        freight_total = c1.number_input("Freight Total", min_value=0.0, step=100.0, value=0.0)
        clearance_total = c2.number_input("Clearance Total", min_value=0.0, step=50.0, value=0.0)
        submitted_header = st.form_submit_button("Save Batch Header")
    if submitted_header and batch_id:
        execute("""
            insert into batch(batch_id, inbound_date, freight_total, entryfees_total)
            values (%s,%s,%s,%s)
            on conflict (batch_id) do update
            set inbound_date=excluded.inbound_date,
                freight_total=excluded.freight_total,
                entryfees_total=excluded.entryfees_total;
        """, (batch_id, inbound_date, freight_total, clearance_total))
        st.success("Batch header saved.")

    st.markdown("#### Paste or Upload Items")
    st.caption("Columns: SKU, Category, Quantity In, FOB per Unit, CBM per Unit")
    colu = st.columns(2)
    pasted = colu[0].text_area("Paste from Excel (CSV/TSV)", height=180, key="paste_items")
    upload = colu[1].file_uploader("Or upload CSV", type=["csv"], key="csv_items")

    df = pd.DataFrame(columns=["SKU","Category","Quantity In","FOB per Unit","CBM per Unit"])
    if pasted:
        try:
            df = parse_paste(pasted)
        except Exception as e:
            st.error(f"Paste parse error: {e}")
    elif upload:
        try:
            df = pd.read_csv(upload)
        except Exception as e:
            st.error(f"CSV parse error: {e}")

    if not df.empty:
        lowmap = {c.lower(): c for c in df.columns}
        def pick(*names):
            for n in names:
                if n.lower() in lowmap: return lowmap[n.lower()]
            return None
        cols = {
            "SKU": pick("SKU","internal_sku"),
            "Category": pick("Category","category"),
            "Quantity In": pick("Quantity In","qty_in","quantity"),
            "FOB per Unit": pick("FOB per Unit","fob_unit","fob"),
            "CBM per Unit": pick("CBM per Unit","cbm_per_unit","cbm"),
        }
        missing = [k for k,v in cols.items() if v is None]
        if missing:
            st.error("Missing columns: " + ", ".join(missing))
        else:
            show = df.rename(columns={
                cols["SKU"]:"SKU",
                cols["Category"]:"Category",
                cols["Quantity In"]:"Quantity In",
                cols["FOB per Unit"]:"FOB per Unit",
                cols["CBM per Unit"]:"CBM per Unit",
            })[["SKU","Category","Quantity In","FOB per Unit","CBM per Unit"]]
            st.dataframe(show, use_container_width=True, height=260)

            if st.button("Save Records"):
                if not batch_id:
                    st.error("Please save Batch Header first.")
                else:
                    out = show.copy()
                    out.insert(0,"batch_id", batch_id)
                    out["inbound_date"] = inbound_date
                    out["freight_total"] = freight_total
                    out["entryfees_total"] = clearance_total
                    out = out.rename(columns={
                        "SKU":"internal_sku",
                        "Category":"category",
                        "Quantity In":"qty_in",
                        "FOB per Unit":"fob_unit",
                        "CBM per Unit":"cbm_per_unit",
                    })[["batch_id","inbound_date","internal_sku","category","qty_in","fob_unit","cbm_per_unit","freight_total","entryfees_total"]]
                    df_insert(out, "inbound_items_flat", truncate_first=True)
                    execute("select ingest_inbound_items();")
                    st.success(f"Saved {len(out)} items. Per-unit costs rebuilt (no monthly recompute).")

# -------- Tax Upload --------
with tabs[1]:
    st.subheader("Tax / Duty by Batch")
    st.caption("Enter category-level duty pool and optional per-SKU overrides. (Does not trigger monthly summary)")
    batch_for_tax = st.text_input("Batch ID", placeholder="e.g., WF2305")

    st.markdown("**Category Duty Pool** (Category, Duty Total)")
    pool_paste = st.text_area("Paste CSV/TSV for Pool", height=120, key="tax_pool_paste")
    pool_df = parse_paste(pool_paste) if pool_paste.strip() else pd.DataFrame(columns=["Category","Duty Total"])

    st.markdown("**Item Duty Overrides (optional)** (SKU, Duty Amount)")
    ov_paste = st.text_area("Paste CSV/TSV for Overrides", height=120, key="tax_ov_paste")
    override_df = parse_paste(ov_paste) if ov_paste.strip() else pd.DataFrame(columns=["SKU","Duty Amount"])

    if st.button("Commit Tax"):
        if not batch_for_tax:
            st.error("Please input Batch ID.")
        else:
            if not pool_df.empty:
                p = pool_df.rename(columns={"Category":"category","Duty Total":"duty_total"})
                p["batch_id"] = batch_for_tax
                df_insert(p[["batch_id","category","duty_total"]], "inbound_tax_pool", truncate_first=True)
            else:
                execute("truncate table inbound_tax_pool;")
            if not override_df.empty:
                o = override_df.rename(columns={"SKU":"internal_sku","Duty Amount":"duty_amount"})
                o["batch_id"] = batch_for_tax
                df_insert(o[["batch_id","internal_sku","duty_amount"]], "inbound_tax_item", truncate_first=True)
            else:
                execute("truncate table inbound_tax_item;")
            execute("select ingest_inbound_tax();")
            st.success("Tax saved. Per-unit duty rebuilt.")

# -------- Sales Upload (auto-process) --------
with tabs[2]:
    st.subheader("Amazon Sales")
    tz = st.text_input("Timezone", "UTC")
    auto = st.checkbox("Auto-process after import (expand → FIFO → summarize)", value=True)

    colsu = st.columns(2)
    f = colsu[0].file_uploader("Upload Amazon Monthly Unified Transaction CSV", type=["csv"], key="sales_csv")
    pasted = colsu[1].text_area("Or paste CSV/TSV (with original header)", height=180, key="sales_paste")

    if st.button("Import Sales"):
        try:
            import io
            if f:
                raw = f.read()
                n = import_sales_csv(raw, tz=tz)
                df_tmp = None
            else:
                buf = pasted or ""
                if not buf.strip():
                    st.error("Please upload or paste data.")
                    st.stop()
                raw = buf.encode("utf-8", errors="ignore")
                n = import_sales_csv(raw, tz=tz)

            st.success(f"Imported {n} order rows.")

            if auto and n > 0:
                import pandas as pd
                # Find earliest month in this import by reading again (safe)
                try:
                    from loader import _read_amz_unified_bytes
                    df_tmp = _read_amz_unified_bytes(raw)
                    if 'date/time' in df_tmp.columns:
                        mindt = pd.to_datetime(df_tmp['date/time'], errors='coerce', utc=True).min()
                    elif {'date','time'}.issubset(set(df_tmp.columns)):
                        mindt = pd.to_datetime(df_tmp['date'].astype(str) + ' ' + df_tmp['time'].astype(str), errors='coerce', utc=True).min()
                    else:
                        mindt = pd.Timestamp.utcnow()
                except Exception:
                    mindt = pd.Timestamp.utcnow()

                month_start = pd.Timestamp(mindt.year, mindt.month, 1, tz='UTC').to_pydatetime().isoformat()
                expand_movements(month_start)
                qty = fifo_allocate(month_start)
                summarize()
                st.success(f"Auto-processed. FIFO allocated {qty} units. Monthly summaries updated (by-market & total).")
        except Exception as e:
            st.error(str(e))

# -------- Inventory --------
with tabs[3]:
    st.subheader("Inventory")
    rows = execute("""
        select p.internal_sku as sku, p.category, coalesce(sum(lb.qty_remaining),0) as qty_left
        from product p
        left join lot_balance lb on lb.internal_sku = p.internal_sku
        group by 1,2
        order by p.internal_sku;
    """)
    inv = pd.DataFrame(rows, columns=["SKU","Category","Qty Left"])
    st.dataframe(inv, use_container_width=True)

# -------- Adjustments (auto reprocess month) --------
with tabs[4]:
    st.subheader("Reverse / Adjust by Order ID")
    oid = st.text_input("Order ID")
    note = st.text_input("Note (optional)")
    auto_rev = st.checkbox("Auto-reprocess month after reverse", value=True)

    if st.button("Reverse Allocation"):
        try:
            reverse_order(oid, note)
            st.success(f"Reversed {oid}.")

            if auto_rev and oid.strip():
                row = fetch_df(
                    "select min(happened_at) as ts from sales_txn where order_id = %s;",
                    params=(oid,), columns=["ts"]
                )
                import pandas as pd
                if not row.empty and pd.notna(row.loc[0,"ts"]):
                    ts = pd.to_datetime(row.loc[0,"ts"], utc=True)
                else:
                    row2 = fetch_df(
                        "select min(happened_at) as ts from allocation_detail where order_id = %s;",
                        params=(oid,), columns=["ts"]
                    )
                    ts = pd.to_datetime(row2.loc[0,"ts"], utc=True) if (not row2.empty and pd.notna(row2.loc[0,"ts"])) else pd.Timestamp.utcnow()

                month_start = pd.Timestamp(ts.year, ts.month, 1, tz='UTC').to_pydatetime().isoformat()
                expand_movements(month_start)
                qty = fifo_allocate(month_start)
                summarize()
                st.success(f"Auto re-processed month {ts.strftime('%Y-%m')} after reverse.")
        except Exception as e:
            st.error(str(e))

# -------- Reports --------
with tabs[5]:
    st.subheader("Monthly Reports")
    d = st.date_input("Manual Recompute From (choose 1st of month if needed)", value=date(date.today().year, date.today().month, 1))
    iso = datetime.combine(d, datetime.min.time()).isoformat()

    c1, c2, c3 = st.columns(3)
    if c1.button("Expand Movements"):
        try: expand_movements(iso); st.success("Movements expanded.")
        except Exception as e: st.error(str(e))
    if c2.button("FIFO Allocate"):
        try: q=fifo_allocate(iso); st.success(f"Allocated {q} units.")
        except Exception as e: st.error(str(e))
    if c3.button("Summarize"):
        try: summarize(); st.success("Summaries updated.")
        except Exception as e: st.error(str(e))

    rows_m = execute("""
        select month, marketplace, fob, freight, duty, clearance, headhaul, orders
        from month_summary_market
        order by month desc, marketplace;
    """)
    dm = pd.DataFrame(rows_m, columns=["Month","Marketplace","FOB","Freight","Duty","Clearance","Headhaul","Orders"])

    rows_all = execute("""
        select month, fob, freight, duty, clearance, headhaul, orders
        from month_summary
        order by month desc;
    """)
    da = pd.DataFrame(rows_all, columns=["Month","FOB","Freight","Duty","Clearance","Headhaul","Orders"])

    st.markdown("#### By Marketplace")
    st.dataframe(dm, use_container_width=True)
    st.markdown("#### All Markets (Total)")
    st.dataframe(da, use_container_width=True)

# -------- Master Data --------
with tabs[6]:
    st.subheader("Products")
    pdf = fetch_df(
        "select internal_sku as sku, category, cbm_per_unit, fob_default, reorder_point from product order by sku;",
        columns=["SKU","Category","CBM per Unit","FOB Default","Reorder Point"]
    )
    edit = st.data_editor(pdf, num_rows="dynamic", use_container_width=True)
    if st.button("Save Products"):
        if not edit.empty:
            out = edit.rename(columns={"SKU":"internal_sku"})
            df_upsert(out, "product", conflict_cols=["internal_sku"])
            st.success("Products saved.")

    st.divider()
    st.subheader("SKU Mapping (Amazon → Internal)")
    sm = fetch_df(
        "select sku as amazon_sku, coalesce(marketplace,'US') as marketplace, internal_sku, coalesce(unit_multiplier,1) as unit_multiplier from sku_map order by sku;",
        columns=["Amazon SKU","Marketplace","Internal SKU","Units per Amazon SKU"]
    )
    sm_edit = st.data_editor(sm, num_rows="dynamic", use_container_width=True, key="skumap_edit")
    if st.button("Save Mapping"):
        if not sm_edit.empty:
            out = sm_edit.rename(columns={"Amazon SKU":"sku","Internal SKU":"internal_sku","Units per Amazon SKU":"unit_multiplier"})
            df_upsert(out.fillna({"marketplace":"US","unit_multiplier":1}), "sku_map",
                      conflict_cols=["sku","marketplace","internal_sku"])
            st.success("Mapping saved.")

# -------- Kit BOM --------
with tabs[7]:
    st.subheader("Kit BOM (Amazon SKU → Components)")
    kb = fetch_df(
        "select sku as amazon_sku, coalesce(marketplace,'US') as marketplace, component_sku, units_per_kit from kit_bom order by sku, component_sku;",
        columns=["Amazon SKU","Marketplace","Component SKU","Units per Kit"]
    )
    kb_edit = st.data_editor(kb, num_rows="dynamic", use_container_width=True, key="kitbom_edit")
    if st.button("Save Kit BOM"):
        if not kb_edit.empty:
            out = kb_edit.rename(columns={"Amazon SKU":"sku","Component SKU":"component_sku","Units per Kit":"units_per_kit"})
            df_upsert(out.fillna({"marketplace":"US"}), "kit_bom",
                      conflict_cols=["sku","marketplace","component_sku"])
            st.success("Kit BOM saved.")

# -------- Admin --------
with tabs[8]:
    st.subheader("Admin — Snapshots & Reset")

    st.markdown("### Export Snapshots (CSV)")
    colx1, colx2, colx3, colx4 = st.columns(4)
    if colx1.button("Export Inventory Lots"):
        rows = execute("""
            select b.batch_id, b.inbound_date, il.internal_sku, il.qty_in,
                   coalesce(lc.fob_unit,0), coalesce(lc.freight_per_unit,0),
                   coalesce(lc.duty_per_unit,0), coalesce(lc.clearance_per_unit,0)
            from batch b
            join inbound_lot il on il.batch_id=b.batch_id
            left join lot_cost lc on lc.batch_id=il.batch_id and lc.internal_sku=il.internal_sku
            order by b.inbound_date, b.batch_id, il.internal_sku;
        """)
        df = pd.DataFrame(rows, columns=["batch_id","inbound_date","internal_sku","qty_in","fob_unit","freight_unit","duty_unit","clearance_unit"])
        st.download_button("Download lots.csv", df.to_csv(index=False).encode("utf-8"), "lots.csv", "text/csv")
    if colx2.button("Export Sales"):
        rows = execute("select happened_at, type, order_id, sku, marketplace, qty from sales_txn order by happened_at;")
        df = pd.DataFrame(rows, columns=["happened_at","type","order_id","sku","marketplace","qty"])
        st.download_button("Download sales.csv", df.to_csv(index=False).encode("utf-8"), "sales.csv", "text/csv")
    if colx3.button("Export Allocations"):
        rows = execute("""
            select happened_at, internal_sku, qty, batch_id, order_id, marketplace, fob_unit, freight_unit, duty_unit, clearance_unit
            from allocation_detail where reversed_by is null order by happened_at;
        """)
        df = pd.DataFrame(rows, columns=["happened_at","internal_sku","qty","batch_id","order_id","marketplace","fob_unit","freight_unit","duty_unit","clearance_unit"])
        st.download_button("Download allocations.csv", df.to_csv(index=False).encode("utf-8"), "allocations.csv", "text/csv")
    if colx4.button("Export Summary (All)"):
        rows = execute("select month, fob, freight, duty, clearance, headhaul, orders from month_summary order by month;")
        df = pd.DataFrame(rows, columns=["month","fob","freight","duty","clearance","headhaul","orders"])
        st.download_button("Download summary_total.csv", df.to_csv(index=False).encode("utf-8"), "summary_total.csv", "text/csv")

    st.markdown("### Save / Restore Monthly Summary Snapshot")
    c1, c2 = st.columns(2)
    if c1.button("Save Snapshot Now"):
        execute("""
        create table if not exists month_summary_snapshot (
            snapshot_time timestamptz default now(),
            month date,
            fob numeric, freight numeric, duty numeric, clearance numeric, headhaul numeric, orders integer
        );
        insert into month_summary_snapshot(month, fob, freight, duty, clearance, headhaul, orders)
        select month, fob, freight, duty, clearance, headhaul, orders from month_summary;
        """)
        st.success("Snapshot saved.")
    snap_month = c2.text_input("Restore month (YYYY-MM)", "")
    if st.button("Restore Month Summary"):
        if not snap_month.strip():
            st.error("Enter YYYY-MM.")
        else:
            try:
                execute("""
                delete from month_summary where to_char(month,'YYYY-MM') = %s;
                insert into month_summary(month, fob, freight, duty, clearance, headhaul, orders)
                select month, fob, freight, duty, clearance, headhaul, orders
                from month_summary_snapshot
                where to_char(month,'YYYY-MM') = %s;
                """, (snap_month, snap_month))
                st.success(f"Restored month {snap_month} from snapshot.")
            except Exception as e:
                st.error(str(e))

    st.markdown("### Reset Data")
    choice = st.radio("Reset Level", ["Transactional only (testing)", "Full wipe (keep master data)"])
    confirm = st.text_input("Type: RESET to confirm")
    if st.button("Run Reset"):
        if confirm.strip().upper() != "RESET":
            st.error("Please type RESET to confirm.")
        else:
            try:
                if choice.startswith("Transactional"):
                    execute("delete from allocation_detail;")
                    execute("delete from movement;")
                    execute("delete from sales_txn;")
                    execute("delete from lot_balance;")
                    execute("delete from lot_cost;")
                    execute("delete from month_summary_market;")
                    execute("delete from month_summary;")
                    execute("delete from inbound_tax_item;")
                    execute("delete from inbound_tax_pool;")
                    st.success("Transactional tables cleared.")
                else:
                    execute("delete from allocation_detail;")
                    execute("delete from movement;")
                    execute("delete from sales_txn;")
                    execute("delete from lot_balance;")
                    execute("delete from lot_cost;")
                    execute("delete from month_summary_market;")
                    execute("delete from month_summary;")
                    execute("delete from inbound_tax_item;")
                    execute("delete from inbound_tax_pool;")
                    execute("delete from inbound_lot;")
                    execute("delete from batch;")
                    execute("do $$ begin if to_regclass('public.inbound_items_flat') is not null then delete from inbound_items_flat; end if; end $$;")
                    st.success("Full wipe done (master data kept).")
            except Exception as e:
                st.error(str(e))
