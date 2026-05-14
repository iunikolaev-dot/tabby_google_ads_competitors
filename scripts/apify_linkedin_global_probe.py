#!/usr/bin/env python3
"""
scripts/apify_linkedin_global_probe.py — probe the LinkedIn Ad Library
scraper across all 5 Global competitors (Klarna, Wise, Monzo, Cash App,
Revolut) at a moderate limit, then merge the results into data/ads.db
so they show up in the production dashboard via /api/ads.

Country filter = ALL  (per project scope: LinkedIn Ads is Global-only).
JOB ads are rejected at the scraper level — see LINKEDIN_REJECTED_FORMATS.

Cost ceiling at the configured limit:
    5 competitors × limit × $0.004 (ad-with-details) + 5 × $0.00005 actor-start
    e.g. limit=50  →  ~$1.00 worst case.

Usage:
    python3 scripts/apify_linkedin_global_probe.py            # limit=50
    python3 scripts/apify_linkedin_global_probe.py --limit 100

Requires:
    APIFY_TOKEN in env
    /tmp/tabby_approval_<YYYYMMDD>.token  (P6 gate)
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
from pipeline import sqlite_store  # noqa: E402
from pipeline.observability import record_spend  # noqa: E402


TODAY = datetime.date.today().isoformat()
BATCH_ID = f"linkedin_probe_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _merge_history(rows: list[dict], conn) -> tuple[int, int]:
    """Set seen_in_batches / first_seen / last_seen on rows by looking up
    the existing row (if any) in ads.db. Returns (new_count, updated_count).
    """
    new_count = updated_count = 0
    for r in rows:
        cur = conn.execute(
            "SELECT seen_in_batches, first_seen_batch_id "
            "FROM ads WHERE platform=? AND creative_id=? AND region=?",
            (r["Platform"], r["Creative ID"], r.get("Region", "")),
        ).fetchone()
        r["last_seen_batch_id"] = BATCH_ID
        r["miss_streak"] = 0
        if cur is None:
            r["seen_in_batches"] = 1
            r["first_seen_batch_id"] = BATCH_ID
            r["New This Week"] = "NEW"
            new_count += 1
        else:
            r["seen_in_batches"] = (cur["seen_in_batches"] or 0) + 1
            r["first_seen_batch_id"] = cur["first_seen_batch_id"] or BATCH_ID
            updated_count += 1
    return new_count, updated_count


def main() -> int:
    args = sys.argv[1:]
    limit = 50
    if "--limit" in args:
        limit = int(args[args.index("--limit") + 1])

    # P6 gate
    today_token = Path(f"/tmp/tabby_approval_{TODAY.replace('-','')}.token")
    if not today_token.exists() or not today_token.read_text().strip():
        print(f"FATAL: approval token missing at {today_token}", file=sys.stderr)
        return 1

    if not config.resolve_env("APIFY_TOKEN"):
        print("FATAL: APIFY_TOKEN not set", file=sys.stderr)
        return 1

    targets = [
        c for c in config.COMPETITORS
        if c.get("category") == "Global" and c.get("linkedin_handle")
    ]
    print(f"=== LinkedIn probe — {len(targets)} Global competitors, "
          f"limit={limit} each ===")
    worst = len(targets) * (limit * apify_linkedin.COST_PER_AD_WITH_DETAILS_USD
                            + apify_linkedin.COST_ACTOR_START_USD)
    print(f"  worst-case cost: ~${worst:.2f}")
    print()

    all_rows: list[dict] = []
    total_items = 0
    total_cost = 0.0
    per_competitor = []

    for comp in targets:
        print(f"--- {comp['name']} ({comp['linkedin_handle']}) ---")
        result = apify_linkedin.scrape_competitor(
            comp, BATCH_ID, results_limit=limit,
            country="ALL", skip_details=False,
        )
        items = result["stats"]["items_fetched"]
        rows = result["rows"]
        cost = result["stats"]["estimated_cost_usd"]
        total_items += items
        total_cost += cost
        per_competitor.append({
            "name": comp["name"],
            "items_fetched": items,
            "rows_built": len(rows),
            "cost_usd": cost,
            "errors": result.get("errors", [])[:3],
        })
        record_spend(BATCH_ID, "apify_linkedin/silva95gustavo",
                     competitor=comp["name"],
                     items_fetched=items,
                     est_cost_usd=cost,
                     extra={"run_id": result["stats"]["run_id"],
                            "limit": limit,
                            "rows_kept": len(rows)})
        if result["errors"]:
            print(f"    errors: {result['errors'][:2]}")
        print(f"    items: {items} → rows kept: {len(rows)} (after JOB filter) "
              f"~${cost:.2f}")
        all_rows.extend(rows)

    print()
    print(f"=== Aggregated: {total_items} items, {len(all_rows)} rows kept, "
          f"${total_cost:.2f} spent ===")

    if not all_rows:
        print("No rows to merge. Done.")
        return 0

    # Merge into ads.db with history fields
    conn = sqlite_store.open_db()
    sqlite_store.init_db(conn)
    new_count, updated_count = _merge_history(all_rows, conn)
    n_written = sqlite_store.upsert_rows(conn, all_rows)
    # Checkpoint WAL so a clean commit has the merged data in the main .db
    conn.execute("PRAGMA wal_checkpoint(FULL)")
    conn.execute("VACUUM")
    conn.close()
    print()
    print(f"DB merge: {new_count} new + {updated_count} re-seen — "
          f"{n_written} rows written to data/ads.db")

    # Quick post-checks
    conn = sqlite_store.open_db()
    li_total = conn.execute(
        "SELECT COUNT(*) FROM ads WHERE platform='LinkedIn Ads'"
    ).fetchone()[0]
    by_comp = conn.execute(
        "SELECT competitor_name, COUNT(*) FROM ads "
        "WHERE platform='LinkedIn Ads' GROUP BY competitor_name"
    ).fetchall()
    print(f"DB now contains {li_total} LinkedIn Ads rows:")
    for r in by_comp:
        print(f"  {r[0]:10}  {r[1]}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
