import os
import psycopg2
from psycopg2.extras import execute_values
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()
DB_DSN = os.environ.get("DB_DSN", "").strip()  # postgresql://postgres:YOURPASSWORD@HOST:6543/postgres

def get_pg():
    if not DB_DSN:
        raise RuntimeError("Missing DB_DSN secret")
    return psycopg2.connect(DB_DSN)

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- 数据库一键初始化/升级 ----
SCHEMA_SQL = r"""
do $$
begin
  perform 1;
  execute 'drop function if exists rebuild_lot_costs() cascade';
  execute 'drop function if exists summarize_month(text) cascade';
  execute 'drop function if exists normalize_month_sales(text) cascade';
  execute 'drop function if exists apply_fifo_for_month(text) cascade';
  execute 'drop view if exists v_sales_raw_orders cascade';
  execute 'drop table if exists month_summary cascade';
  execute 'drop table if exists sales_txn cascade';
  execute 'drop table if exists sales_raw cascade';
  execute 'drop table if exists sku_map cascade';
  execute 'drop table if exists lot_balance cascade';
  execute 'drop table if exists lot_cost cascade';
  execute 'drop table if exists inbound_tax_pool cascade';
  execute 'drop table if exists inbound_lot cascade';
  execute 'drop table if exists batch cascade';
exception when others then null;
end;
$$;

create extension if not exists "uuid-ossp";

create table batch (
  batch_id        text primary key,
  inbound_date    date not null,
  freight_total   numeric default 0,
  clearance_total numeric default 0,
  created_at      timestamptz default now()
);

create table inbound_lot (
  batch_id      text references batch(batch_id) on delete cascade,
  internal_sku  text not null,
  category      text not null,
  qty_in        integer not null check (qty_in >= 0),
  fob_unit      numeric,
  cbm_per_unit  numeric,
  created_at    timestamptz default now(),
  primary key (batch_id, internal_sku)
);

create table inbound_tax_pool (
  batch_id   text references batch(batch_id) on delete cascade,
  category   text not null,
  duty_total numeric not null default 0,
  created_at timestamptz default now(),
  primary key (batch_id, category)
);

create table lot_cost (
  batch_id           text not null,
  internal_sku       text not null,
  freight_per_unit   numeric default 0,
  clearance_per_unit numeric default 0,
  duty_per_unit      numeric default 0,
  updated_at         timestamptz default now(),
  primary key (batch_id, internal_sku)
);

create table lot_balance (
  batch_id     text not null,
  internal_sku text not null,
  qty_in       integer not null check (qty_in >= 0),
  qty_sold     integer not null default 0 check (qty_sold >= 0),
  primary key (batch_id, internal_sku)
);

create table sku_map (
  marketplace  text default 'US',
  amazon_sku   text not null,
  internal_sku text not null,
  ratio        numeric not null default 1,
  primary key (marketplace, amazon_sku, internal_sku)
);

create table sales_raw (
  raw_id       bigserial primary key,
  raw_time     timestamptz,
  raw_type     text,
  order_id     text,
  marketplace  text,
  amazon_sku   text,
  quantity     integer,
  raw_payload  jsonb,
  imported_at  timestamptz default now()
);

create table sales_txn (
  happened_at  timestamptz not null,
  order_id     text,
  marketplace  text default 'US',
  amazon_sku   text not null,
  qty          integer not null check (qty >= 0),
  created_at   timestamptz default now()
);

create table month_summary (
  ym             text primary key,
  fob_sum        numeric default 0,
  freight_sum    numeric default 0,
  duty_sum       numeric default 0,
  clearance_sum  numeric default 0,
  orders         integer default 0,
  updated_at     timestamptz default now()
);

create index idx_inbound_lot_cat on inbound_lot(category);
create index idx_lot_balance_sku on lot_balance(internal_sku);
create index idx_sales_txn_time on sales_txn(happened_at);
create index idx_sales_txn_sku on sales_txn(amazon_sku);
create index idx_sales_raw_time  on sales_raw(raw_time);
create index idx_sku_map_sku     on sku_map(amazon_sku);

create or replace view v_sales_raw_orders as
select
  raw_time      as happened_at,
  order_id,
  coalesce(nullif(marketplace,''),'US') as marketplace,
  amazon_sku,
  quantity      as qty
from sales_raw
where lower(coalesce(raw_type,'')) = 'order'
  and quantity is not null
  and quantity > 0;

create or replace function rebuild_lot_costs() returns void language plpgsql as $$
begin
  delete from lot_cost;
  insert into lot_cost (batch_id, internal_sku, freight_per_unit, clearance_per_unit, duty_per_unit)
  select
    il.batch_id,
    il.internal_sku,
    case when cbm_batch.sum_cbm > 0 then
      b.freight_total * (il.qty_in * coalesce(il.cbm_per_unit,0) / cbm_batch.sum_cbm) / nullif(il.qty_in,0)
    else 0 end,
    case when cbm_batch.sum_cbm > 0 then
      b.clearance_total * (il.qty_in * coalesce(il.cbm_per_unit,0) / cbm_batch.sum_cbm) / nullif(il.qty_in,0)
    else 0 end,
    case when fob_pool.sum_fob_cat > 0 then
      coalesce(tp.duty_total,0) * ((il.qty_in * coalesce(il.fob_unit,0)) / fob_pool.sum_fob_cat) / nullif(il.qty_in,0)
    else 0 end
  from inbound_lot il
  join batch b on b.batch_id = il.batch_id
  left join (
    select batch_id, sum(qty_in * coalesce(cbm_per_unit,0)) as sum_cbm
    from inbound_lot group by batch_id
  ) cbm_batch on cbm_batch.batch_id = il.batch_id
  left join (
    select batch_id, category, sum(qty_in * coalesce(fob_unit,0)) as sum_fob_cat
    from inbound_lot group by batch_id, category
  ) fob_pool on fob_pool.batch_id = il.batch_id and fob_pool.category = il.category
  left join inbound_tax_pool tp on tp.batch_id = il.batch_id and tp.category = il.category
  on conflict (batch_id, internal_sku) do update set
    freight_per_unit   = excluded.freight_per_unit,
    clearance_per_unit = excluded.clearance_per_unit,
    duty_per_unit      = excluded.duty_per_unit,
    updated_at         = now();

  insert into lot_balance (batch_id, internal_sku, qty_in, qty_sold)
  select batch_id, internal_sku, qty_in, 0
  from inbound_lot
  on conflict (batch_id, internal_sku) do update set
    qty_in = excluded.qty_in;
end $$;

create or replace function normalize_month_sales(ym text) returns void language sql as $$
  insert into sales_txn (happened_at, order_id, marketplace, amazon_sku, qty)
  select happened_at, order_id, marketplace, amazon_sku, qty
  from v_sales_raw_orders
  where to_char(happened_at, 'YYYY-MM') = ym
  on conflict do nothing;
$$;

create or replace function summarize_month(ym text) returns void language sql as $$
  insert into month_summary (ym, orders, updated_at)
  select ym,
         (select count(*) from sales_txn where to_char(happened_at, 'YYYY-MM') = ym),
         now()
  on conflict (ym) do update set
    orders = excluded.orders,
    updated_at = now();
$$;

create or replace function apply_fifo_for_month(ym text) returns void
language plpgsql as $$
declare
  r record;
  lot record;
  need numeric;
  take integer;
  v_fob numeric := 0;
  v_freight numeric := 0;
  v_clearance numeric := 0;
  v_duty numeric := 0;
begin
  drop table if exists _t_need;
  create temporary table _t_need (
    internal_sku text primary key,
    qty_needed numeric
  ) on commit drop;

  insert into _t_need (internal_sku, qty_needed)
  select sm.internal_sku, sum(t.qty * sm.ratio)
  from sales_txn t
  join sku_map sm
    on sm.marketplace = coalesce(nullif(t.marketplace,''),'US')
   and sm.amazon_sku  = t.amazon_sku
  where to_char(t.happened_at, 'YYYY-MM') = ym
  group by sm.internal_sku;

  for r in select internal_sku, qty_needed from _t_need loop
    need := r.qty_needed;

    for lot in
      select lb.batch_id, lb.internal_sku, lb.qty_in, lb.qty_sold,
             b.inbound_date,
             coalesce(lc.freight_per_unit,0)   as c_freight,
             coalesce(lc.clearance_per_unit,0) as c_clearance,
             coalesce(lc.duty_per_unit,0)      as c_duty,
             coalesce(il.fob_unit,0)           as c_fob
      from lot_balance lb
      join batch b        on b.batch_id = lb.batch_id
      left join lot_cost lc on lc.batch_id = lb.batch_id and lc.internal_sku = lb.internal_sku
      left join inbound_lot il on il.batch_id = lb.batch_id and il.internal_sku = lb.internal_sku
      where lb.internal_sku = r.internal_sku
      order by b.inbound_date asc, lb.batch_id asc
    loop
      exit when need <= 0;
      take := greatest(0, least(need, (lot.qty_in - lot.qty_sold)));
      if take > 0 then
        update lot_balance set qty_sold = qty_sold + take
        where batch_id = lot.batch_id and internal_sku = lot.internal_sku;

        v_freight   := v_freight   + lot.c_freight   * take;
        v_clearance := v_clearance + lot.c_clearance * take;
        v_duty      := v_duty      + lot.c_duty      * take;
        v_fob       := v_fob       + lot.c_fob       * take;

        need := need - take;
      end if;
    end loop;
  end loop;

  insert into month_summary (ym, fob_sum, freight_sum, clearance_sum, duty_sum, orders, updated_at)
  values (
    ym, v_fob, v_freight, v_clearance, v_duty,
    (select count(*) from sales_txn where to_char(happened_at,'YYYY-MM') = ym),
    now()
  )
  on conflict (ym) do update set
    fob_sum       = excluded.fob_sum,
    freight_sum   = excluded.freight_sum,
    clearance_sum = excluded.clearance_sum,
    duty_sum      = excluded.duty_sum,
    orders        = excluded.orders,
    updated_at    = now();
end $$;
"""

def ensure_schema():
    """如果核心表不存在，自动创建整套 schema。"""
    conn = get_pg()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        select count(*)
        from information_schema.tables
        where table_schema='public' and table_name='batch'
    """)
    exists = cur.fetchone()[0] > 0
    if not exists:
        cur.execute(SCHEMA_SQL)
    cur.close(); conn.close()

# ---- 数据写入工具 ----
def insert_rows(table, rows, columns):
    if not rows:
        return
    conn = get_pg()
    cur = conn.cursor()
    sql = f"insert into {table} ({', '.join(columns)}) values %s"
    execute_values(cur, sql, rows)
    conn.commit(); cur.close(); conn.close()

def call_sql(sql, params=None):
    conn = get_pg(); cur = conn.cursor()
    cur.execute(sql, params or ())
    conn.commit(); cur.close(); conn.close()
