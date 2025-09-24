create table if not exists raw_orders(
  id bigserial primary key,
  payload jsonb not null,
  ingested_at timestamptz default now()
);

create table if not exists orders_clean(
  order_id text primary key,
  customer_id text not null,
  amount numeric not null,
  currency text not null,
  created_at timestamptz not null,
  last_updated timestamptz not null
);

create table if not exists orders_quarantine(
  id bigserial primary key,
  reason text not null,
  payload jsonb not null,
  quarantined_at timestamptz default now()
);

create table if not exists audit_events(
  id bigserial primary key,
  stage text not null,          -- VALIDATE / DECIDE / REMEDIATE
  event text not null,          -- e.g., NULLS_EXCEEDED
  details jsonb,
  created_at timestamptz default now()
);

create table if not exists pipeline_state(
  id int primary key default 1,
  last_updated_iso text
);

insert into pipeline_state(id) values (1)
on conflict (id) do nothing;
