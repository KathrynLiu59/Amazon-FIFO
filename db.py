# db.py  —— 统一的 Supabase 客户端
import os
from supabase import create_client, Client

def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY in Secrets")
    return create_client(url, key)

def upsert(table: str, rows: list[dict], on_conflict: list[str] | None = None):
    sb = get_client()
    q = sb.table(table).upsert(rows)
    if on_conflict:
        q = q.on_conflict(",".join(on_conflict))
    return q.execute()

def rpc(fn: str, args: dict | None = None):
    sb = get_client()
    return sb.rpc(fn, args or {}).execute()
