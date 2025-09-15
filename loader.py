# loader.py —— 统一导入：Amazon 月报 / 入库 / 成本池 / 税金池 / 映射 / 组合柜
import io
import pandas as pd
from datetime import datetime
from dateutil import parser
from db import upsert

# 4.1 Amazon 月报 CSV -> sales_raw
def load_sales_raw_from_csv(file_bytes: bytes, marketplace: str):
    df = pd.read_csv(io.BytesIO(file_bytes))
    # 兼容字段名（以你截图为准，可自行补充更多别名）
    col = {c.lower(): c for c in df.columns}
    must = ["date/time","type","order id","sku","quantity","marketplace"]  # 如站点列名不同，这里统一成 marketplace 传参覆盖
    for m in must:
        if m not in col:
            raise ValueError(f"Missing column: {m}")

    def parse_dt(s):
        try: return parser.parse(str(s))
        except: return None

    rows = []
    for _, r in df.iterrows():
        happened_at = parse_dt(r[col["date/time"]])
        if happened_at is None: 
            continue
        rows.append({
            "happened_at": happened_at.isoformat(),
            "type": str(r[col["type"]]),
            "order_id": str(r[col["order id"]]),
            "amazon_sku": str(r[col["sku"]]),
            "quantity": int(pd.to_numeric(r[col["quantity"]], errors="coerce") or 0),
            "marketplace": marketplace or str(r.get(col.get("marketplace",""), marketplace) or marketplace),
            "payload": {k:str(r[k]) for k in df.columns}
        })

    # 仅保留 type=Order & quantity>0 的行，其它行也可以先入库保留
    rows = [x for x in rows if x["type"] in ("Order","order") and x["quantity"]>0]
    if rows:
        upsert("sales_raw", rows, on_conflict=["order_id","amazon_sku","happened_at"])

# 4.2 基础维表导入
def upsert_products(rows: list[dict]):         # {internal_sku, category, weight_kg_per_unit, cbm_per_unit}
    return upsert("product", rows, on_conflict=["internal_sku"])

def upsert_category(rows: list[dict]):         # {category, duty_rate_default}
    return upsert("category", rows, on_conflict=["category"])

def upsert_sku_map(rows: list[dict]):          # {amazon_sku, marketplace, internal_sku, unit_multiplier, active}
    return upsert("sku_map", rows, on_conflict=["amazon_sku","marketplace"])

def upsert_kit_bom(rows: list[dict]):          # {amazon_sku, marketplace, component_sku, component_qty}
    return upsert("kit_bom", rows, on_conflict=["amazon_sku","marketplace","component_sku"])

# 4.3 入库 & 成本池
def upsert_batch(rows: list[dict]):            # {batch_id, container_no, arrived_at, dest_market, note}
    for r in rows:
        if isinstance(r.get("arrived_at"), (pd.Timestamp, datetime)):
            r["arrived_at"] = r["arrived_at"].date().isoformat()
    return upsert("batch", rows, on_conflict=["batch_id"])

def upsert_inbound_items(rows: list[dict]):    # {batch_id, internal_sku, category, qty_in, fob_unit, cbm_per_unit, weight_kg_per_unit, duty_override_unit}
    return upsert("inbound_items", rows, on_conflict=["batch_id","internal_sku"])

def upsert_batch_cost_pool(rows: list[dict]):  # {batch_id, freight_total, clearance_total}
    return upsert("batch_cost_pool", rows, on_conflict=["batch_id"])

def upsert_batch_duty_pool(rows: list[dict]):  # {batch_id, category, duty_total}
    return upsert("batch_duty_pool", rows, on_conflict=["batch_id","category"])
