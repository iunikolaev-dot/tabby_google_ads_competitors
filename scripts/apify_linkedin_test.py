#!/usr/bin/env python3
"""
scripts/apify_linkedin_test.py — small probe run of the LinkedIn Ad
Library Apify actor for a single Global competitor.

Goal: see what the actor actually returns (field names, image URLs,
ad copy, dates) so we can finalize the v2-row mapping in
scrapers/apify_linkedin.py before doing a full multi-competitor scrape.

Usage:
    python3 scripts/apify_linkedin_test.py                 # default: Wise, limit 50
    python3 scripts/apify_linkedin_test.py --who Klarna
    python3 scripts/apify_linkedin_test.py --who Wise --limit 100

Always uses country=ALL per project scope (Global competitors only).
Stages raw JSON output to staging/linkedin_<name>_<batch>.json.
Does NOT touch public/ads_data.js or data/ads.db — just an inspection run.
Records spend to metrics/spend_history.jsonl.

Requires:
  - APIFY_TOKEN in env
  - /tmp/tabby_approval_<YYYYMMDD>.token (P6 gate)
"""

from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import config  # noqa: E402
from scrapers import apify_linkedin  # noqa: E402
from pipeline.observability import record_spend  # noqa: E402


TODAY = datetime.date.today().isoformat()
BATCH_ID = f"linkedin_probe_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"


def main() -> int:
    who = "Wise"
    limit = 50
    args = sys.argv[1:]
    if "--who" in args:
        who = args[args.index("--who") + 1]
    if "--limit" in args:
        limit = int(args[args.index("--limit") + 1])

    # P6 approval token (per audit feedback rule)
    today_token = Path(f"/tmp/tabby_approval_{TODAY.replace('-','')}.token")
    if not today_token.exists() or not today_token.read_text().strip():
        print(f"FATAL: approval token missing at {today_token}", file=sys.stderr)
        print(f"       echo 'approved by Ilya — LinkedIn probe' > {today_token}",
              file=sys.stderr)
        return 1

    if not config.resolve_env("APIFY_TOKEN"):
        print("FATAL: APIFY_TOKEN not set", file=sys.stderr)
        return 1

    comp = next((c for c in config.COMPETITORS if c["name"] == who), None)
    if not comp:
        print(f"FATAL: '{who}' not in config.COMPETITORS", file=sys.stderr)
        return 1
    if comp.get("category") != "Global":
        print(f"FATAL: '{who}' is not Global — LinkedIn scrape is Global-only "
              f"per project scope", file=sys.stderr)
        return 1
    if not comp.get("linkedin_handle"):
        print(f"FATAL: '{who}' has no linkedin_handle in config", file=sys.stderr)
        return 1

    worst_case_usd = round(apify_linkedin.estimate_cost_usd(limit), 4)
    print(f"=== LinkedIn probe ===")
    print(f"  Competitor:    {who} (handle={comp['linkedin_handle']})")
    print(f"  Country:       ALL")
    print(f"  Results limit: {limit}")
    print(f"  Worst-case $:  ~${worst_case_usd}")
    print()

    result = apify_linkedin.scrape_competitor(
        comp, BATCH_ID, results_limit=limit, country="ALL",
        skip_details=False,
    )

    items = result["stats"]["items_fetched"]
    rows = len(result["rows"])
    cost = result["stats"]["estimated_cost_usd"]
    record_spend(BATCH_ID, "apify_linkedin/silva95gustavo",
                 competitor=who, items_fetched=items,
                 est_cost_usd=cost,
                 extra={"run_id": result["stats"]["run_id"],
                        "limit": limit, "errors": result["errors"][:3]})

    print(f"=== Result ===")
    print(f"  ok:           {result['ok']}")
    print(f"  items_fetched: {items}")
    print(f"  rows_built:    {rows}")
    print(f"  est cost USD:  ${cost}")
    if result["errors"]:
        print(f"  errors:        {result['errors'][:5]}")

    # Show shape of first 2 raw items so we can adjust the v2 mapping.
    if items > 0:
        raw_path = REPO / "staging" / f"linkedin_{who.replace(' ', '_')}_{BATCH_ID}.json"
        if raw_path.exists():
            raw = json.loads(raw_path.read_text())
            print()
            print("=== First raw item keys ===")
            print(sorted(raw[0].keys()))
            print()
            print("=== First raw item (truncated values) ===")
            sample = {k: (str(v)[:120] if v else v) for k, v in raw[0].items()}
            print(json.dumps(sample, indent=2, ensure_ascii=False))

    # Show first built v2 row for comparison
    if rows > 0:
        print()
        print("=== First built v2 row ===")
        first = {k: (str(v)[:120] if v else v) for k, v in result["rows"][0].items()}
        print(json.dumps(first, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
