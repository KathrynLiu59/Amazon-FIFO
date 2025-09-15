# loader.py
from typing import Sequence
import pandas as pd
from psycopg import sql
from db import get_conn

try:
    from psycopg.extras import execute_values  # psycopg v3 可用
except Exception:
    execute_values = None

def _cast_str(v):
    if pd.isna(v):
        return None
    return str(v)

def upsert_df(table: str, df: pd.DataFrame, conflict_cols: Sequence[str] | None):
    if df is None or df.empty:
        return
    df = df.copy()
    # 把 pandas 的 NaN 统一转 None，避免插入报错
    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    with get_conn() as conn, conn.cursor() as cur:
        insert_stmt = sql.SQL("insert into {t} ({cols}) values %s").format(
            t=sql.Identifier(table),
            cols=sql.SQL(',').join(map(sql.Identifier, cols))
        )
        if conflict_cols:
            updates = sql.SQL(',').join(
                sql.SQL("{}=excluded.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in cols if c not in conflict_cols
            )
            insert_stmt = insert_stmt + sql.SQL(" on conflict ({keys}) do update set {upd}").format(
                keys=sql.SQL(',').join(map(sql.Identifier, conflict_cols)),
                upd=updates
            )

        if execute_values:
            execute_values(cur, insert_stmt.as_string(conn), values, page_size=2000)
        else:
            # 回退策略：逐条 executemany
            single = sql.SQL("insert into {t} ({cols}) values ({ph})").format(
                t=sql.Identifier(table),
                cols=sql.SQL(',').join(map(sql.Identifier, cols)),
                ph=sql.SQL(',').join(sql.Placeholder() for _ in cols)
            )
            if conflict_cols:
                single = single + sql.SQL(" on conflict ({keys}) do update set {upd}").format(
                    keys=sql.SQL(',').join(map(sql.Identifier, conflict_cols)),
                    upd=updates
                )
            cur.executemany(single, values)

def csv_to_df(file, encoding='utf-8'):
    import pandas as pd
    return pd.read_csv(file, dtype=str, encoding=encoding, keep_default_na=False, na_values=[''])

def parse_amz_unified_csv(df: pd.DataFrame, ym: str, marketplace: str) -> pd.DataFrame:
    # 仅抽取“订单”行；保留原文，按我们统一字段命名落到 sales_raw
    # 兼容不同导出列名的大小写/空格
    rename = {c: c.strip().lower().replace(' ', '_') for c in df.columns}
    df = df.rename(columns=rename)
    # 标准列：date/time, type, order id, sku, quantity
    need = ['date/time', 'type', 'order id', 'sku', 'quantity']
    # 有些导出列会是 date_time / order_id，做一次映射容错
    alt = {
        'date/time': ['date_time', 'date'],
        'order id': ['order_id','amazon_order_id'],
        'sku': ['seller_sku','product_id'],
        'quantity': ['quantity_purchased','qty']
    }
    for k in list(rename.values()):
        pass
    for k in need:
        if k not in df.columns:
            cands = alt.get(k, [])
            found = None
            for a in cands:
                if a in df.columns:
                    found = a
                    break
            if found:
                df[k] = df[found]
            else:
                df[k] = None

    out = pd.DataFrame({
        'ym': ym,
        'marketplace': marketplace,
        'date_time': df['date/time'],
        'type': df['type'],
        'order_id': df['order id'],
        'sku': df['sku'],
        'quantity': df['quantity'],
        'payload': df.to_json(orient='records')
    })
    return out
