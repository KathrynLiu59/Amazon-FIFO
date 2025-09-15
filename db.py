# db.py — 统一数据库连接
import os
import psycopg

SUPABASE_URL = os.environ.get("SUPABASE_URL")     # https://xxxx.supabase.co
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")     # anon key
DB_DSN        = os.environ.get("DB_DSN")          # 可选直连字符串

def get_conn():
    """
    优先走 DB_DSN（直连），否则用 Supabase 连接参数（pgbouncer连接池）。
    在 Streamlit Cloud 里建议使用 DB_DSN（Supabase → Project Settings → Database → Connection string → "Direct connection"）。
    """
    if DB_DSN:
        return psycopg.connect(DB_DSN)
    # 兜底：如果只给了 URL/KEY，仍建议配置 Supabase "Service Role" + pgpass。
    # 简化起见，这里提示使用 DB_DSN。
    raise RuntimeError("Please set DB_DSN in Streamlit Secrets (recommended).")
