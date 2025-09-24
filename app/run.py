from . import storage
from .ingest import extract_batch
from .validate import validate
from .decide import decide
from .remediate import apply_actions
from .storage import write_clean, audit, write_raw

def main():
    wm = storage.get_watermark()
    df = extract_batch(since_iso=wm)
    if df.empty:
        audit("VALIDATE", "RESULTS", {"issues": []})
        print("No new data.")
        return

    write_raw(df)
    issues = validate(df)
    audit("VALIDATE", "RESULTS", {"issues": issues})

    actions = decide(issues)
    audit("DECIDE", "ACTIONS", {"actions": actions})

    df2 = apply_actions(df, actions)
    write_clean(df2)
    audit("REMEDIATE", "DONE", {"rows_clean": len(df2)})

    # update watermark to newest timestamp that actually landed clean
    if not df2.empty:
        latest = df2["last_updated"].max().strftime("%Y-%m-%dT%H:%M:%SZ")
        storage.set_watermark(latest)

    print(f"Ingested raw: {len(df)}, clean: {len(df2)}")

if __name__ == "__main__":
    main()
