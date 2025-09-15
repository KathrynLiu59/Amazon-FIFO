# db.py
import os
import ssl
from urllib.parse import urlparse, parse_qs

try:
    import psycopg  # 可选：如 requirements 里没装，会 ImportError
except Exception:
    psycopg = None

try:
    import pg8000  # 兜底驱动（纯 Python）
except Exception:
    pg8000 = None


DB_DSN = os.environ.get("DB_DSN")  # 你在 Streamlit Secrets 里配的 Postgres 连接串


def _parse_dsn_for_pg8000(dsn: str) -> dict:
    """
    把 postgresql://user:pass@host:port/db?sslmode=require
    解析成 pg8000.connect(**kwargs) 所需的参数
    """
    u = urlparse(dsn)
    if u.scheme not in ("postgres", "postgresql"):
        raise ValueError(f"Unsupported DSN scheme: {u.scheme}")

    qs = parse_qs(u.query or "")
    sslmode = (qs.get("sslmode", ["prefer"])[0] or "prefer").lower()

    kwargs = {
        "user":     u.username or "",
        "password": u.password or "",
        "host":     u.hostname or "localhost",
        "port":     int(u.port or 5432),
        "database": (u.path or "/")[1:] or None,
    }

    # SSL 处理：require/verify-* 生成简单的 SSLContext
    if sslmode in ("require", "verify-ca", "verify-full"):
        kwargs["ssl_context"] = ssl.create_default_context()
    elif sslmode in ("disable",):
        pass
    else:
        # prefer / allow 等，pg8000 默认即可
        pass

    return kwargs


def get_conn():
    if not DB_DSN:
        raise RuntimeError("DB_DSN is not set in Streamlit Secrets")

    # 优先 psycopg
    if psycopg is not None:
        return psycopg.connect(DB_DSN)

    # 兜底 pg8000
    if pg8000 is not None:
        kwargs = _parse_dsn_for_pg8000(DB_DSN)
        return pg8000.connect(**kwargs)

    raise RuntimeError("No database driver available. Please install pg8000 or psycopg.")
