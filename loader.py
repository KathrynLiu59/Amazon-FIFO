# loader.py
import io
import pandas as pd
from psycopg import sql
from psycopg.extras import execute_values
from db import connect

# —— 核心：把 Amazon Monthly Unified Transaction（CSV）导入 sales_raw
# 只取到我们需要的核心列；其余保存在 raw_json（可选）
RAW_COL_MAP = {
    "date/time": "happened_at",
    "type": "type",
    "order id": "order_id",
    "sku": "sku",
    "quantity": "quantity",
    "marketplace": "marketplace",
}

def _normalize_headers(cols):
    # 统一成小写去空格便于映射
    return [str(c).strip().lower() for c in cols]

def load_sales_raw_from_csv(file_bytes: bytes) -> int:
    df = pd.read_csv(io.BytesIO(file_bytes))
    df.columns = _normalize_headers(df.columns)

    keep = {src: RAW_COL_MAP[src] for src in RAW_COL_MAP if src in df.columns}
    df = df[list(keep.keys())].rename(columns=keep)

    # 小清洗
    if "quantity" in df:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)

    # 批量插入
    rows = df.to_dict("records")
    if not rows:
        return 0

    with connect() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into sales_raw(happened_at, type, order_id, sku, quantity, marketplace, raw_json)
            values %s
            """,
            [
                (
                    r.get("happened_at"),
                    r.get("type"),
                    r.get("order_id"),
                    r.get("sku"),
                    r.get("quantity"),
                    (r.get("marketplace") or "US").upper(),
                    None,
                )
                for r in rows
            ],
        )
    return len(rows)
