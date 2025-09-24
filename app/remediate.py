import pandas as pd
from .storage import quarantine, audit
from .notify import alert

def apply_actions(df, actions):
    df_out = df.copy()
    for act, reason in actions:
        if act == "BLOCK":
            audit("DECIDE", "BLOCK", {"reason": reason})
            alert("Pipeline blocked", {"reason": reason}, severity="critical")
            raise RuntimeError(f"Blocked: {reason}")

        if act == "DEDUPE":
            before = len(df_out)
            df_out = df_out.drop_duplicates(subset=["order_id"], keep="last")
            dropped = before - len(df_out)
            if dropped > 0:
                alert("Deduped rows", {"dropped": int(dropped)}, severity="warning")

        if act == "QUARANTINE_PARTIAL":
            bad = df_out[df_out["amount"].isna()]
            if not bad.empty:
                quarantine(bad, reason)
                alert("Quarantined bad rows", {"count": int(bad.shape[0])}, severity="warning")
            df_out = df_out.dropna(subset=["amount"])

        if act == "ALLOW_WITH_LOG":
            audit("DECIDE", "SCHEMA_EXTRA_ALLOWED", {})

        if act == "BACKFILL":
            audit("REMEDIATE", "REQUEST_BACKFILL", {"hours": 2})
            alert("Requested backfill", {"hours": 2}, severity="info")

    return df_out
