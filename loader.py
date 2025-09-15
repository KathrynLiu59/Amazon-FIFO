# loader.py
import io
import pandas as pd
from db import get_conn


def _noneify(df: pd.DataFrame) -> list[list]:
    """把 DataFrame 转 list[list]，并将 NaN 转为 None。"""
    return df.where(pd.notna(df), None).values.tolist()


def bulk_insert_dataframe(table: str, df: pd.DataFrame):
    """
    通用批量写入：INSERT INTO table (col1, col2, ...) VALUES (%s, %s, ...)
    兼容 psycopg / pg8000 两种驱动的 paramstyle（都支持 %s）
    """
    if df.empty:
        return 0

    cols = list(df.columns)
    placeholders = "(" + ",".join(["%s"] * len(cols)) + ")"
    sql = f"insert into {table} ({','.join(cols)}) values {placeholders}"

    rows = _noneify(df)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        cur.close()
    return len(rows)


def load_sales_raw_from_csv(csv_bytes: bytes, marketplace: str | None = None):
    """
    读取你每月 Amazon 导出的 CSV（原样列名），按你们既有落表规则写入 sales_raw。
    这里不做复杂清洗，只负责“导入”；之后的匹配、分摊、月结由 worker 调度 SQL 来做。
    """
    buf = io.BytesIO(csv_bytes)
    # 让 pandas 直接用第一行做列名；保留原始列（和你传的模板对齐）
    df = pd.read_csv(buf, dtype=str).fillna("")

    # 可以在这里做最轻量的标准化（示例：去除列名里的空格）
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if marketplace and "marketplace" in df.columns:
        # 如果你想强制写 marketplace 字段（可选）
        df["marketplace"] = marketplace

    # 按你们的 sales_raw 结构，挑选/重命名列（示例）——
    # 如果表结构和列名与你们最终 schema 有差异，请在这里调整映射：
    # （下面是参考映射，你可以按你最终 sales_raw 列名替换）
    rename_map = {
        "date/time": "date_time",
        "order id": "order_id",
        "sku": "sku",
        "quantity": "quantity",
        "product sales": "product_sales",
        "product sales tax": "product_sales_tax",
        "shipping credits": "shipping_credits",
        "gift wrap credits": "gift_wrap_credits",
        "promotional rebates": "promotional_rebates",
        "selling fees": "selling_fees",
        "fba fees": "fba_fees",
        "other transaction fees": "other_fees",
        "other": "other",
        "description": "description",
        "fulfillment": "fulfillment",
        "city": "order_city",
        "state": "order_state",
        "postal": "order_postal",
        # … 你可以继续补充
    }
    # 把 rename_map 里存在的列才重命名
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # 写入 sales_raw（确保 sales_raw 已建好；列要与 df.columns 对齐）
    inserted = bulk_insert_dataframe("sales_raw", df)
    return inserted
