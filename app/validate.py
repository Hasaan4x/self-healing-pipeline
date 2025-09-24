import pandas as pd
from datetime import datetime, timezone, timedelta
from .config import SLA_MINUTES, NULL_THRESH, DUP_THRESH

def validate(df: pd.DataFrame):
    issues = []

    # schema: required cols
    required = {"order_id","customer_id","amount","currency","created_at","last_updated"}
    missing = required - set(df.columns)
    if missing:
        issues.append(("SCHEMA_MISSING", {"cols": list(missing)}))

    # freshness: last_updated not too old
    if "last_updated" in df.columns and not df["last_updated"].empty:
        max_age = (datetime.now(timezone.utc) - df["last_updated"].max()).total_seconds()/60
        if max_age > SLA_MINUTES:
            issues.append(("FRESHNESS", {"max_age_min": max_age}))

    # nulls
    if "amount" in df.columns:
        null_rate = df["amount"].isna().mean()
        if null_rate > NULL_THRESH:
            issues.append(("NULLS_AMOUNT", {"rate": null_rate}))

    # duplicates
    if "order_id" in df.columns:
        dup_rate = df["order_id"].duplicated().mean()
        if dup_rate > DUP_THRESH:
            issues.append(("DUPLICATES", {"rate": dup_rate}))

    # schema drift: extra cols (just record, don’t fail)
    extra = set(df.columns) - required
    if extra:
        issues.append(("SCHEMA_EXTRA", {"cols": list(extra)}))

    return issues
