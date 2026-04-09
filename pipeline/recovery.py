"""
pipeline/recovery.py — implements PRD v2 §10.1 CDN recovery runbook.

Purpose: restore the 613 Global Google ads (Klarna, Wise, Monzo, Cash App,
Revolut) that were deleted from `public/ads_data.js` during the failed
Phase 3 rescrape on 2026-04-09.

This module treats recovery as the INTEGRATION TEST for pipeline/merge.py.
It calls `merge_rows()` just like the real scrapers will, exercising the
2-phase atomic write and postconditions on a known-safe, additive operation.

Data source: a snapshot of the Vercel CDN copy of ads_data.js saved to disk
BEFORE the CDN could be redeployed with the broken version. The snapshot
path is passed as an argument so we don't hardcode a filename that changes
with timestamps.

Recovery policy (PRD §10.1 steps 5-7):
    - Only rows where (Platform, Creative ID) is NOT already in the current
      SoT are eligible.
    - Only rows where Platform == "Google Ads" AND Competitor Name is in the
      5 Global Google competitors are recovered. This narrows the scope to
      exactly what the incident deleted.
    - Recovered rows are stamped with source_actor="recovered_from_cdn".
    - last_seen_batch_id is NOT set (stamp_last_seen=False in merge_rows).
      Rationale: these ads were not observed in the current batch; faking
      the last_seen would violate Invariant I4 ("Last Shown is set ONLY when
      a scrape successfully observes the ad").
    - preview_status is marked "unverified". The next live scrape or the
      preview validator will fix old fbcdn URLs that may have expired.
    - The total recovered count MUST be in [500, 800] per §10.1 step 6, or
      recovery aborts.

CLI usage:
    python -m pipeline.recovery --snapshot ~/tabby_recovery_snapshot_20260409_175604.js
    python -m pipeline.recovery --snapshot <path> --dry-run

Cross-references:
    PRD §10.1  Recovery runbook
    PRD §4.7   Invariant I4 (Last Shown discipline)
    PRD §4.8   Postconditions Q1-Q6
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import config
import safety_check
from pipeline import merge


# ─────────────────────────────────────────────────────────────────────────────
# Sanity thresholds (PRD §10.1 step 6)
# ─────────────────────────────────────────────────────────────────────────────

MIN_RECOVERABLE = 500
MAX_RECOVERABLE = 800


# ─────────────────────────────────────────────────────────────────────────────
# Core logic
# ─────────────────────────────────────────────────────────────────────────────

def compute_recoverable_rows(
    current_rows: list[dict],
    snapshot_rows: list[dict],
) -> list[dict]:
    """
    Return rows from snapshot_rows that:
      - are not already in current_rows (by Platform + Creative ID), AND
      - are Google Ads, AND
      - belong to one of the 5 Global Google competitors.

    Pure function: no I/O, no mutation of inputs.
    """
    current_keys = {(r.get("Platform", ""), r.get("Creative ID", ""))
                    for r in current_rows}

    recoverable: list[dict] = []
    for r in snapshot_rows:
        key = (r.get("Platform", ""), r.get("Creative ID", ""))
        if key in current_keys:
            continue
        if r.get("Platform") != "Google Ads":
            continue
        if r.get("Competitor Name") not in config.GLOBAL_GOOGLE_COMPETITOR_NAMES:
            continue
        recoverable.append(r)

    return recoverable


def stamp_recovery_metadata(row: dict) -> dict:
    """
    Apply recovery-specific field stamps to a row in place, then return it.

    - source_actor = "recovered_from_cdn" (preserved if already set)
    - preview_status = "unverified" (Check 1 and 2 of the preview validator
      are expected to flag many of these until a live rescrape refreshes).
    - preview_checked_at = now (ISO)
    - Status = derived from Last Shown vs today (Invariant I5)

    Note: we deliberately DO NOT set first_seen_batch_id or last_seen_batch_id
    here. pipeline.merge.merge_rows() handles those based on stamp_last_seen=False.
    """
    row.setdefault("source_actor", "recovered_from_cdn")
    row["preview_status"] = "unverified"
    row["preview_checked_at"] = datetime.now().isoformat(timespec="seconds")

    # Recompute Status from Last Shown (Invariant I5).
    last_shown = row.get("Last Shown", "")
    try:
        if last_shown:
            dt = datetime.strptime(last_shown, "%Y-%m-%d").date()
            row["Status"] = "Active" if (date.today() - dt).days <= 7 else "Inactive"
        else:
            row["Status"] = "Inactive"
    except ValueError:
        row["Status"] = "Inactive"

    return row


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run_recovery(snapshot_path: Path, dry_run: bool = False) -> int:
    """
    Full recovery workflow. Returns a Unix exit code (0 on success).
    """
    print(f"pipeline/recovery.py — snapshot={snapshot_path}")
    print("=" * 70)

    # ── Step 1: safety checks in recovery mode (P1, P2, P5) ───────────────
    print("\n[1/7] Preconditions (recovery mode)")
    ok, results = safety_check.run_checks("recovery")
    for r in results:
        print(f"    {r}")
    if not ok:
        print("ABORT: preconditions failed.")
        return 1

    # ── Step 2: acquire lock ───────────────────────────────────────────────
    print("\n[2/7] Acquiring lock")
    if config.LOCK_PATH.exists():
        print(f"ABORT: {config.LOCK_PATH} already exists.")
        return 1
    try:
        config.LOCK_PATH.write_text(str(datetime.now().isoformat()))
        print(f"    locked: {config.LOCK_PATH}")
    except Exception as e:
        print(f"ABORT: cannot acquire lock: {e}")
        return 1

    try:
        # ── Step 3: load SoT and snapshot ─────────────────────────────────
        print("\n[3/7] Loading SoT and snapshot")
        current_rows = merge.load_sot(config.SOT_PATH)
        print(f"    current SoT: {len(current_rows)} rows")

        if not snapshot_path.exists():
            print(f"ABORT: snapshot not found at {snapshot_path}")
            return 1
        if snapshot_path.stat().st_size < 1_000_000:
            print(f"ABORT: snapshot only "
                  f"{snapshot_path.stat().st_size} bytes (< 1 MB). "
                  f"Suspect CDN was already redeployed.")
            return 1

        snapshot_rows = merge.load_sot(snapshot_path)
        print(f"    snapshot:    {len(snapshot_rows)} rows")

        # ── Step 4: compute recoverable diff ──────────────────────────────
        print("\n[4/7] Computing recoverable rows")
        recoverable = compute_recoverable_rows(current_rows, snapshot_rows)
        print(f"    recoverable: {len(recoverable)} rows")

        # Per-competitor breakdown
        from collections import Counter
        per_comp = Counter(r.get("Competitor Name", "?") for r in recoverable)
        for comp, count in sorted(per_comp.items()):
            print(f"      {comp}: {count}")

        # ── Step 5: sanity check count (PRD §10.1 step 6) ─────────────────
        if not (MIN_RECOVERABLE <= len(recoverable) <= MAX_RECOVERABLE):
            print(f"ABORT: recoverable count {len(recoverable)} outside "
                  f"expected range [{MIN_RECOVERABLE}, {MAX_RECOVERABLE}]. "
                  f"Expected ~613.")
            return 1

        # ── Step 6: stamp metadata ────────────────────────────────────────
        print("\n[5/7] Stamping recovery metadata")
        for r in recoverable:
            stamp_recovery_metadata(r)
        print(f"    stamped {len(recoverable)} rows "
              f"with source_actor='recovered_from_cdn', "
              f"preview_status='unverified'")

        # ── Step 7: merge via the real pipeline ───────────────────────────
        print("\n[6/7] Running pipeline/merge.py")
        if dry_run:
            print("    DRY RUN — no file will be written.")
            print(f"    Would add {len(recoverable)} rows to "
                  f"{config.SOT_PATH}.")
            return 0

        batch_id = merge.make_batch_id("recovery")
        stats = merge.merge_rows(
            new_rows=recoverable,
            batch_id=batch_id,
            source_actor="recovered_from_cdn",
            stamp_last_seen=False,   # Invariant I4: don't fake observation
        )

        print(f"    batch_id:            {stats.batch_id}")
        print(f"    rows_before:         {stats.rows_before}")
        print(f"    rows_after:          {stats.rows_after}")
        print(f"    rows_added:          {stats.rows_added}")
        print(f"    rows_updated:        {stats.rows_updated}")
        print(f"    rows_rejected_no_cr: {stats.rows_rejected_no_cr}")
        print(f"    sha256_before:       {stats.sha256_before[:16]}...")
        print(f"    sha256_after:        {stats.sha256_after[:16]}...")

        if not stats.ok:
            print("\nPOSTCONDITION FAILURES:")
            for f in stats.postcondition_failures:
                print(f"    - {f}")
            print("\nABORT: merge rolled back. public/ads_data.js is untouched.")
            return 1

        # ── Step 8: write recovery log ────────────────────────────────────
        print("\n[7/7] Writing recovery log")
        config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        log_path = config.LOGS_DIR / f"recovery_{batch_id}.json"
        log_path.write_text(json.dumps({
            "batch_id": batch_id,
            "snapshot_path": str(snapshot_path),
            "stats": stats.as_dict(),
            "per_competitor": dict(per_comp),
        }, indent=2, ensure_ascii=False))
        print(f"    log: {log_path}")

        print("\n" + "=" * 70)
        print(f"RECOVERY SUCCESS — {stats.rows_added} ads restored "
              f"({stats.rows_before} → {stats.rows_after})")
        return 0

    finally:
        # Always release the lock, even on error.
        try:
            config.LOCK_PATH.unlink(missing_ok=True)
            print(f"    lock released: {config.LOCK_PATH}")
        except Exception as e:
            print(f"    WARNING: could not release lock: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore 613 Global Google ads from CDN snapshot (PRD §10.1).")
    parser.add_argument(
        "--snapshot",
        required=True,
        type=Path,
        help="Path to the CDN snapshot file (e.g. ~/tabby_recovery_snapshot_*.js)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all checks and compute the diff but do not write public/ads_data.js",
    )
    args = parser.parse_args()
    snapshot = args.snapshot.expanduser().resolve()
    return run_recovery(snapshot, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
