# loader.py — 装载 Amazon Monthly Unified Transaction CSV → sales_raw
import io
import pandas as pd
from dateutil import parser
from db import get_conn

# 你给的 CSV 模版列名示例（参考截图与文件）：
# date/time, type, order id, sku, quantity, marketplace, ...（列名大小写和空格不一致没关系，我们会统一lower并去空格）

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]
    return df

def _to_ym(ts) -> str:
    return pd.to_datetime(ts).strftime("%Y-%m")

def load_sales_raw_from_csv(file_bytes: bytes, marketplace: str):
    """
    读取用户上传的 Amazon CSV，仅抽取我们要用的列：
      - date/time -> happened_at
      - type      -> type
      - order id  -> order_id
      - sku       -> amazon_sku
      - quantity  -> qty
    并写入 sales_raw（覆盖同月同站点旧数据）
    """
    df = pd.read_csv(io.BytesIO(file_bytes))
    df = _norm_cols(df)

    need = ['date/time', 'type', 'order_id', 'sku', 'quantity']
    # 兼容不同表头写法
    map_cols = {
        'date/time':   [ 'date/time', 'date_time', 'date', 'datetime' ],
        'type':        [ 'type', 'settlement_type' ],
        'order_id':    [ 'order_id', 'orderid' ],
        'sku':         [ 'sku' ],
        'quantity':    [ 'quantity', 'qty' ],
    }
    cols = {}
    for k, cands in map_cols.items():
        for c in cands:
            if c in df.columns:
                cols[k] = c
                break
        if k not in cols:
            raise ValueError(f"CSV missing column for {k} (tried {cands})")

    use = df[[ cols['date/time'], cols['type'], cols['order_id'], cols['sku'], cols['quantity'] ]].copy()
    use.columns = ['happened_at', 'type', 'order_id', 'amazon_sku', 'qty']

    # 只保留订单/退款；把日期解析；丢掉 qty=0
    use['happened_at'] = pd.to_datetime(use['happened_at'], errors='coerce')
    use = use.dropna(subset=['happened_at'])
    use = use[ use['qty'] != 0 ]

    # ym
    use['ym'] = use['happened_at'].dt.strftime("%Y-%m")
    use['marketplace'] = marketplace

    # 批量入库
    with get_conn() as conn, conn.cursor() as cur:
        # 先清同月（多月数据分开清）
        for ym in use['ym'].unique():
            cur.execute("select clear_sales_raw(%s,%s);", (ym, marketplace))

        rows = list(use[['ym','marketplace','happened_at','order_id','amazon_sku','qty','type']].itertuples(index=False, name=None))
        cur.executemany("""
          insert into sales_raw(ym, marketplace, happened_at, order_id, amazon_sku, qty, type)
          values (%s,%s,%s,%s,%s,%s,%s)
          on conflict (marketplace, order_id, amazon_sku, happened_at) do update
            set qty=excluded.qty, type=excluded.type;
        """, rows)
        conn.commit()

    # 返回每月行数以供界面提示
    return use.groupby('ym').size().to_dict()
