
import io
import pandas as pd
from db import df_insert, execute

def import_inbound_items(file_bytes: bytes):
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name='items')
    df.columns = [c.strip().lower() for c in df.columns]
    required = ['batch_id','inbound_date','internal_sku','category','qty_in','fob_unit','cbm_per_unit']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in items sheet: {missing}")
    for c in ['freight_total','entryfees_total']:
        if c not in df.columns:
            df[c] = None
    if df['inbound_date'].dtype == 'object':
        df['inbound_date'] = pd.to_datetime(df['inbound_date']).dt.date
    df_insert(df[['batch_id','inbound_date','internal_sku','category','qty_in','fob_unit','cbm_per_unit','freight_total','entryfees_total']], 'inbound_items_flat', truncate_first=True)
    execute("select ingest_inbound_items();")
    return len(df)

def import_inbound_tax(file_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    if 'tax_pool' not in xls.sheet_names:
        raise ValueError("Missing sheet 'tax_pool'")
    pool = pd.read_excel(xls, 'tax_pool')
    pool.columns = [c.strip().lower() for c in pool.columns]
    for c in ['batch_id','category','duty_total']:
        if c not in pool.columns:
            raise ValueError("tax_pool missing column: " + c)
    if 'tax_override' in xls.sheet_names:
        override = pd.read_excel(xls, 'tax_override')
        override.columns = [c.strip().lower() for c in override.columns]
        if not set(['batch_id','internal_sku','duty_amount']).issubset(set(override.columns)):
            raise ValueError("tax_override sheet must have: batch_id, internal_sku, duty_amount")
    else:
        override = pd.DataFrame(columns=['batch_id','internal_sku','duty_amount'])
    df_insert(pool[['batch_id','category','duty_total']], 'inbound_tax_pool', truncate_first=True)
    if not override.empty:
        df_insert(override[['batch_id','internal_sku','duty_amount']], 'inbound_tax_item', truncate_first=True)
    else:
        execute("truncate table inbound_tax_item;")
    execute("select ingest_inbound_tax();")
    return len(pool)

def import_sales_csv(file_bytes: bytes, tz: str = 'UTC'):
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [c.strip().lower() for c in df.columns]
    if 'date/time' in df.columns:
        dtcol = 'date/time'
    elif 'date' in df.columns and 'time' in df.columns:
        df['date/time'] = df['date'].astype(str) + ' ' + df['time'].astype(str)
        dtcol = 'date/time'
    else:
        raise ValueError("CSV must contain 'date/time' or 'date'+'time'")
    if 'type' not in df.columns:
        raise ValueError("CSV must contain 'type' column")
    m_order = df['type'].astype(str).str.lower().str.contains('order')
    df = df[m_order].copy()
    for c in ['order id','sku','quantity']:
        if c not in df.columns:
            raise ValueError(f"CSV missing column '{c}'")
    df['happened_at'] = pd.to_datetime(df[dtcol], errors='coerce', utc=True).dt.tz_convert(tz)
    df['marketplace'] = df.get('marketplace', 'US')
    df['qty'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    df = df[['happened_at','type','order id','sku','marketplace','qty']].rename(columns={'order id':'order_id'})
    if not df.empty:
        start = df['happened_at'].min()
        end = df['happened_at'].max() + pd.Timedelta(days=1)
        execute("delete from sales_txn where happened_at >= %s and happened_at < %s;", (start, end))
    rows = list(df.itertuples(index=False, name=None))
    from db import get_conn
    import psycopg2.extras as extras
    with get_conn() as conn, conn.cursor() as cur:
        extras.execute_values(cur,
            "insert into sales_txn(happened_at,type,order_id,sku,marketplace,qty) values %s",
            rows, page_size=1000)
    return len(rows)
