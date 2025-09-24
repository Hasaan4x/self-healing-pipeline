# app/storage.py
from sqlalchemy import create_engine, text
from .config import DB_URL
from sqlalchemy import text

engine = create_engine(DB_URL, future=True)

def _df_json_records(df):
    """Return a list of JSON-safe dicts: datetimes -> ISO strings, NaN -> None."""
    import pandas as pd
    import numpy as np

    df2 = df.copy()

    # 1) Make all columns object-type so None isn't coerced back to NaN
    df2 = df2.astype(object)

    # 2) Replace NaN/NaT with None
    df2 = df2.where(pd.notnull(df2), None)

    # 3) Coerce datetime-like columns to ISO strings
    for col in ("created_at", "last_updated"):
        if col in df2.columns:
            df2[col] = pd.to_datetime(df2[col], utc=True, errors="coerce").apply(
                lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if x is not None else None
            )

    # Now all values are JSON-serialisable Python types
    return df2.to_dict(orient="records")

def write_raw(df):
    import json
    rows = _df_json_records(df)
    if not rows:
        return

    # json.dumps with allow_nan=False to guarantee no NaN slips through
    params = {f"p{i}": json.dumps(r, allow_nan=False) for i, r in enumerate(rows)}
    values_sql = ",".join([f"((:p{i})::jsonb)" for i in range(len(rows))])

    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO raw_orders(payload) VALUES {values_sql}"), params)

def write_clean(df):
    rows = _df_json_records(df)
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            insert into orders_clean(order_id, customer_id, amount, currency, created_at, last_updated)
            values (:order_id, :customer_id, :amount, :currency, :created_at, :last_updated)
            on conflict (order_id) do update set
              customer_id=excluded.customer_id,
              amount=excluded.amount,
              currency=excluded.currency,
              created_at=excluded.created_at,
              last_updated=excluded.last_updated
        """), rows)

def quarantine(df, reason):
    import json
    rows = _df_json_records(df)
    if not rows:
        return
    params = [{"reason": reason, "payload": json.dumps(r, allow_nan=False)} for r in rows]
    with engine.begin() as conn:
        conn.execute(text("""
            insert into orders_quarantine(reason, payload)
            values (:reason, (:payload)::jsonb)
        """), params)

def audit(stage, event, details=None):
    import json
    with engine.begin() as conn:
        conn.execute(text("""
          insert into audit_events(stage, event, details)
          values (:s, :e, (:d)::jsonb)
        """), {"s": stage, "e": event, "d": json.dumps(details or {}, allow_nan=False)})



def get_watermark():
    with engine.begin() as c:
        return c.execute(text("select last_updated_iso from pipeline_state where id=1")).scalar()

def set_watermark(iso_str: str):
    with engine.begin() as c:
        c.execute(text("update pipeline_state set last_updated_iso=:w where id=1"), {"w": iso_str})
