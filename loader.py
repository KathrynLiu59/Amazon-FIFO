import io
import pandas as pd
from db import execute

def _read_amz_unified_bytes(file_bytes: bytes) -> pd.DataFrame:
    """Robust reader: skip preface lines until header with 'date/time' appears; keep quotes; lower-case columns."""
    try:
        text = file_bytes.decode("utf-8")
    except UnicodeDecodeError:
        import chardet
        enc = chardet.detect(file_bytes).get("encoding", "utf-8")
        text = file_bytes.decode(enc, errors="ignore")

    lines = text.splitlines()
    hdr_idx = None
    for i, line in enumerate(lines[:200]):
        ll = line.lower()
        if "date/time" in ll and "type" in ll and "order id" in ll:
            hdr_idx = i
            break
    if hdr_idx is None:
        raise ValueError("Cannot find header row (expecting 'date/time,type,order id,...').")

    csv_text = "\n".join(lines[hdr_idx:])
    df = pd.read_csv(io.StringIO(csv_text))
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def parse_paste(text: str) -> pd.DataFrame:
    """Generic paste (used for inbound/tax)."""
    if not text or not text.strip():
        return pd.DataFrame()
    first = text.splitlines()[0]
    sep = "\t" if "\t" in first else ","
    return pd.read_csv(io.StringIO(text), sep=sep)

def import_sales_csv(file_bytes: bytes, tz: str = 'UTC') -> int:
    """Import Amazon monthly unified transactions; keep Type=Order; normalize marketplace; de-dup by time range."""
    df = _read_amz_unified_bytes(file_bytes)

    required = ['type','order id','sku','quantity']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing column(s): {', '.join(missing)}")

    m_order = df['type'].astype(str).str.lower().str.contains(r'\border\b')
    df = df[m_order].copy()
    if df.empty:
        return 0

    if 'date/time' in df.columns:
        dt_series = df['date/time'].astype(str)
    elif {'date','time'}.issubset(df.columns):
        dt_series = df['date'].astype(str) + ' ' + df['time'].astype(str)
    else:
        raise ValueError("CSV must include 'date/time' or both 'date' and 'time'.")

    df['happened_at'] = pd.to_datetime(dt_series, errors='coerce', utc=True)

    def norm_marketplace(s: str) -> str:
        s = (s or "").strip().lower()
        if "amazon.com" in s or s == "us": return "US"
        if "amazon.co.uk" in s or s.endswith(".uk") or s == "uk": return "UK"
        if "amazon.de" in s or s == "de": return "DE"
        if "amazon.fr" in s or s == "fr": return "FR"
        if "amazon.it" in s or s == "it": return "IT"
        if "amazon.es" in s or s == "es": return "ES"
        return s.upper() if s else "US"

    if 'marketplace' in df.columns:
        df['marketplace'] = df['marketplace'].astype(str).map(norm_marketplace)
    else:
        df['marketplace'] = 'US'

    df['qty'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)

    out = df[['happened_at','type','order id','sku','marketplace','qty']].rename(columns={'order id':'order_id'})

    start = out['happened_at'].min()
    end = out['happened_at'].max() + pd.Timedelta(days=1)
    execute("delete from sales_txn where happened_at >= %s and happened_at < %s;", (start, end))

    rows = list(out.itertuples(index=False, name=None))
    from db import get_conn
    import psycopg2.extras as extras
    with get_conn() as conn, conn.cursor() as cur:
        extras.execute_values(
            cur,
            "insert into sales_txn(happened_at,type,order_id,sku,marketplace,qty) values %s",
            rows, page_size=1000
        )
    return len(rows)
