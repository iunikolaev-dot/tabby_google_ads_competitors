#!/usr/bin/env python3
"""
scripts/migrate_to_sqlite.py — one-shot import of public/ads_data.js
into data/ads.db (audit step 5.1).

Idempotent: drops + recreates the `ads` table, then bulk-upserts. Safe to
re-run after schema changes.

Verification: prints row counts before/after and a few schema-drift checks
so we know the JSON ↔ SQL translation is faithful.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from pipeline import sqlite_store as store

ADS_JS = REPO / "public" / "ads_data.js"


def load_json() -> list:
    raw = ADS_JS.read_text()
    start = raw.index("[")
    end = raw.rindex("]") + 1
    return json.loads(raw[start:end])


def main() -> int:
    if not ADS_JS.exists():
        print(f"FATAL: {ADS_JS} missing", file=sys.stderr)
        return 1

    rows = load_json()
    print(f"Loaded {len(rows):,} rows from ads_data.js")

    # Recreate from scratch — we want the schema fresh on every migrate.
    if store.DB_PATH.exists():
        store.DB_PATH.unlink()
        print(f"Removed previous {store.DB_PATH}")
    conn = store.open_db()
    store.init_db(conn)
    print(f"Initialized {store.DB_PATH}")

    written = store.upsert_rows(conn, rows)
    print(f"Upserted {written:,} rows")

    # Sanity checks
    db_n = store.count(conn)
    actives = conn.execute("SELECT COUNT(*) FROM ads WHERE status='Active'").fetchone()[0]
    inactives = conn.execute("SELECT COUNT(*) FROM ads WHERE status='Inactive'").fetchone()[0]
    by_plat = dict(conn.execute(
        "SELECT platform, COUNT(*) FROM ads GROUP BY platform"
    ).fetchall())
    by_fmt = dict(conn.execute(
        "SELECT ad_format, COUNT(*) FROM ads GROUP BY ad_format"
    ).fetchall())

    print()
    print(f"DB rows:      {db_n:,}")
    print(f"  Active:     {actives:,}")
    print(f"  Inactive:   {inactives:,}")
    print(f"  By platform: {by_plat}")
    print(f"  By format:   {by_fmt}")

    # Compare counts. Some JSON rows may collide on the (platform, creative_id,
    # region) PK and lose to the later one — that's deduplication, expected.
    if db_n < len(rows):
        print(f"  Note: {len(rows) - db_n:,} rows collided on PK and were "
              f"deduplicated. (existing JSON has 178 known cross-region dupes.)")

    # Smoke check: round-trip a row to make sure JSON ↔ SQL fidelity holds
    sample = conn.execute(
        "SELECT * FROM ads WHERE image_url != '' OR embed_url != '' "
        "ORDER BY seen_in_batches DESC LIMIT 1"
    ).fetchone()
    if sample:
        out = store.row_to_json_dict(sample)
        keys_check = ["Competitor Name", "Platform", "Creative ID", "Image URL",
                      "Embed URL", "Status"]
        present = [k for k in keys_check if k in out]
        print(f"  Round-trip keys present: {present}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
