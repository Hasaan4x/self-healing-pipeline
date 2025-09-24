def decide(issues):
    actions = []
    kinds = {k for k,_ in issues}

    if "SCHEMA_MISSING" in kinds:
        actions.append(("BLOCK", "Missing required columns"))

    if "FRESHNESS" in kinds:
        actions.append(("BACKFILL", "Recent window"))

    if "NULLS_AMOUNT" in kinds:
        actions.append(("QUARANTINE_PARTIAL", "High nulls in amount"))

    if "DUPLICATES" in kinds:
        actions.append(("DEDUPE", "Drop duplicate order_id"))

    if "SCHEMA_EXTRA" in kinds:
        actions.append(("ALLOW_WITH_LOG", "Extra columns ok (recorded)"))

    if not actions:
        actions.append(("LOAD_OK", "Proceed"))

    return actions
