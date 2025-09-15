import io
import pandas as pd
from db import execute

def parse_paste(text: str) -> pd.DataFrame:
    """Detect CSV or TSV from first line and parse to DataFrame."""
    if not text or not text.strip():
        return pd.DataFrame()
    first = text.splitlines()[0]
    sep = "\t" if "\t" in first else ","
    return pd.read_csv(io.StringIO(text), sep=sep)

def import_sales_csv(file_bytes: bytes, tz: str = 'UTC'):
    """
    Import Amazon sales CSV, keeping only type=Order rows.
    Inserts into sales_txn(happened_at,type,order_id,sku,marketplace,qty).
    """
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = [c.strip().lower() for c in df.columns]

    if 'type' not in df.columns:
        raise ValueError("CSV must include 'type' column.")
    m_order = df['type'].astype(str).str.lower().str.contains('order')
    df = df[m_order].copy()

    if 'date/time' in df.columns:
        dtcol = 'date/time'
        dt_series = df[dtcol].astype(str)
    elif 'date' in df.columns and 'time' in df.columns:
        dtcol = 'date/time'
        dt_series = df['date'].astype(str) + ' ' + df['time'].astype(str)
        df[dtcol] = dt_series
    else:
        raise ValueError("CSV must include 'date/time' or 'date'+'time'.")

    for c in ['order id','sku','quantity']:
        if c not in df.columns:
            raise ValueError(f"CSV missing column: {c}")

    df['happened_at'] = pd.to_datetime(df[dtcol], errors='coerce', utc=True)
    df['marketplace'] = df.get('marketplace', 'US')
    df['qty'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)

    out = df[['happened_at','type','order id','sku','marketplace','qty']].rename(columns={'order id':'order_id'})

    # wipe overlapping range to avoid duplicates
    if not out.empty:
        start = out['happened_at'].min()
        end = out['happened_at'].max() + pd.Timedelta(days=1)
        execute("delete from sales_txn where happened_at >= %s and happened_at < %s;", (start, end))

    rows = list(out.itertuples(index=False, name=None))
    from db import get_conn
    import psycopg2.extras as extras
    with get_conn() as conn, conn.cursor() as cur:
        extras.execute_values(cur,
            "insert into sales_txn(happened_at,type,order_id,sku,marketplace,qty) values %s",
            rows, page_size=1000)
    return len(rows)
