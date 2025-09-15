# worker.py
from __future__ import annotations

from typing import Iterable, Any
from db import execute, fetchall

# ====== 基础动作：直接触发后端 SQL 函数/过程 ======

def rebuild_costs() -> None:
    """
    重算：把每条柜的 freight/clearance 按 CBM 分摊到 SKU，
         把 duty 按“本柜内该品类 FOB 占比”分摊（若有 SKU 级覆盖则以覆盖优先）。
    依赖数据库中的函数：rebuild_lot_costs()
    """
    execute("select rebuild_lot_costs();")


def summarize_month(ym: str) -> None:
    """
    统计某月汇总（比如订单数/金额/本月分摊的 FOB 与头程等）。
    依赖数据库中的函数：summarize_month(text)
    ym 形如 '2025-03'
    """
    execute("select summarize_month(%s);", (ym,))


def refresh_inventory_view() -> None:
    """
    有些方案会把库存/成本做成物化视图（或普通视图）。
    如果你用的是物化视图，比如 lot_balance_mv，则刷新它。
    默认尝试两种写法：materialized 与普通 view 的刷新。
    注意：若你只有普通视图（非物化），这一步可忽略。
    """
    try:
        # 物化视图
        execute("refresh materialized view concurrently lot_balance;")
    except Exception:
        try:
            # 普通视图场景（这里书写一个 no-op 或者简易查询确保视图可用）
            _ = fetchall("select 1;")
        except Exception:
            # 既不是物化也不是普通：忽略
            pass


# ====== 复合动作：为按钮/自动化提供一键流程 ======

def rebuild_all_for_month(ym: str, *, do_refresh: bool = True) -> None:
    """
    “一键月结”管道：
      1) 重算所有成本（考虑当月之前的库存沿用 FIFO）
      2) 刷新库存快照（若有物化视图）
      3) 生成该月汇总
    """
    rebuild_costs()
    if do_refresh:
        refresh_inventory_view()
    summarize_month(ym)


def quick_health_check() -> dict[str, Any]:
    """
    简易健康检查（用在 UI 顶部/调试）：
    - 库里关键表是否存在
    - 是否有 inbound / sales / sku_map 基础数据
    """
    out: dict[str, Any] = {}
    try:
        # 表存在性 & 行数
        for name in ("inbound_lot", "sales_txn", "sku_map"):
            try:
                cnt = fetchall(f"select count(*) as c from {name}")[0]["c"]
                out[name] = cnt
            except Exception as e:
                out[name] = f"missing ({e})"
        # 视图/快照
        try:
            _ = fetchall("select * from lot_balance limit 1")
            out["lot_balance"] = "ok"
        except Exception as e:
            out["lot_balance"] = f"missing ({e})"
    except Exception as e:
        out["error"] = str(e)
    return out


# ====== 可选：清理动作（谨慎！） ======

def wipe_month_summary() -> None:
    """清空月度汇总（仅用于重试/调试）"""
    execute("truncate table month_summary;")

def wipe_sales_for_month(ym: str) -> None:
    """
    清空某个月份的销售明细（按 happened_at 的年月重算）
    注意：仅当你真的要重新导入该月 CSV 时再用！
    """
    execute(
        "delete from sales_txn where to_char(happened_at, 'YYYY-MM') = %s;",
        (ym,)
    )
