# db.py
import os
import psycopg
from psycopg.rows import dict_row

def _dsn() -> str:
    # 你在 Streamlit Secrets 里填的那个连接串变量名
    # 支持两种命名，取其一：
    return (
        os.environ.get("DB_DSN")
        or os.environ.get("SUPABASE_DSN")
        or os.environ.get("SUPABASE_URL")  # 兜底（如果你误放了 URL，此处也能报错提醒）
    )

def get_conn() -> psycopg.Connection:
    dsn = _dsn()
    if not dsn or not dsn.startswith("postgres://") and not dsn.startswith("postgresql://"):
        raise RuntimeError(
            "Database DSN is not configured. Set DB_DSN (or SUPABASE_DSN) in Streamlit → App → Settings → Secrets."
        )
    # 建议统一由 with conn: 控制事务（自动提交/回滚）
    return psycopg.connect(dsn)

def fetchall(sql: str, params: tuple | None = None):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

def execute(sql: str, params: tuple | None = None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        # with conn: 会自动 commit
