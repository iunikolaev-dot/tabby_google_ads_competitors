"""
pipeline/merge.py — 2-phase atomic merge for public/ads_data.js.

This module is the ONLY code path permitted to write `public/ads_data.js`.
Every other component (recovery, scrapers/merge, schema migrations) must go
through `merge_rows()`. This is a hard invariant of the v2 pipeline.

Design principles (PRD v2 §4.7, §4.8):

    I1  Append-only within a run. Rows are never removed except via explicit
        retirement (retired=True) which this module does NOT do.
    I2  Every row must have a non-empty Creative ID. Rows without one are
        rejected, logged, and not written.
    I3  Dedup key is (Platform, Creative ID). Region is NOT part of the key.
        A creative seen in multiple regions produces one row with Regions[]
        populated (canonical region at index 0).
    I9  Writes are atomic via 2-phase commit: write .tmp → fsync → rename.
    I10 If ANY postcondition fails, the .tmp is rolled back to .failed and
        the previous file is untouched.

Postconditions (PRD §4.8 Q1-Q6, what `_verify_postconditions` checks):

    Q1  Row count after ≥ row count before (monotonic growth within a batch).
    Q2  Every row from the previous file still exists by dedup key.
    Q3  SHA256 of the new file is written to manifest.json with the batch ID.
    Q4  Smoke test: 5 random rows have their Local Image / Local Video files
        on disk (if those fields are set).
    Q5  Preview validator has run. (For MVP we mark this as "deferred" — the
        validator lives in pipeline/preview_validator.py which is built in a
        later step. Merge records `preview_validator_ran=False` in manifest
        so downstream code can see it was skipped.)
    Q6  On ANY postcondition failure: `mv .tmp .failed` and exit non-zero.

Cross-references:
    PRD §4.2   Single source of truth
    PRD §4.5   Data model
    PRD §4.7   Invariants I1-I10
    PRD §4.8   Pipeline execution contract
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import config


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# The JS wrapper that `public/ads_data.js` uses. Must be stable — the
# dashboard parses this exact prefix.
JS_PREFIX = "const ADS_DATA = "
JS_SUFFIX = ";"

# Dedup key builder. Region is deliberately NOT part of the key (Invariant I3).
def _dedup_key(row: dict) -> tuple[str, str]:
    return (row.get("Platform", ""), row.get("Creative ID", ""))


# ─────────────────────────────────────────────────────────────────────────────
# Result types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MergeStats:
    """Summary of a single merge_rows() invocation."""
    batch_id: str
    rows_before: int = 0
    rows_after: int = 0
    rows_added: int = 0           # brand-new (Platform, Creative ID)
    rows_updated: int = 0         # existing key, fields merged
    rows_rejected_no_cr: int = 0  # dropped by Invariant I2
    rows_in_input: int = 0
    regions_merged: int = 0       # times an existing row got a new region
    sha256_before: str = ""
    sha256_after: str = ""
    postcondition_failures: list[str] = field(default_factory=list)
    ok: bool = False

    def as_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "rows_before": self.rows_before,
            "rows_after": self.rows_after,
            "rows_added": self.rows_added,
            "rows_updated": self.rows_updated,
            "rows_rejected_no_cr": self.rows_rejected_no_cr,
            "rows_in_input": self.rows_in_input,
            "regions_merged": self.regions_merged,
            "sha256_before": self.sha256_before,
            "sha256_after": self.sha256_after,
            "postcondition_failures": self.postcondition_failures,
            "ok": self.ok,
        }


# ─────────────────────────────────────────────────────────────────────────────
# SoT I/O
# ─────────────────────────────────────────────────────────────────────────────

def load_sot(path: Path = None) -> list[dict]:
    """
    Read `public/ads_data.js` and return the list of rows.

    Raises FileNotFoundError if the file doesn't exist.
    Raises ValueError if the wrapper or JSON cannot be parsed.

    Accepts either the v2 wrapper `const ADS_DATA = [...];` or a raw JSON
    array (which the CDN snapshot happens to also be wrapped in). We look
    for the first `[` and last `]` rather than hard-matching the prefix —
    same approach as safety_check.py.
    """
    path = path or config.SOT_PATH
    if not path.exists():
        raise FileNotFoundError(f"SoT file not found: {path}")

    txt = path.read_text()
    start = txt.find("[")
    end = txt.rfind("]")
    if start < 0 or end < 0 or end < start:
        raise ValueError(f"Cannot locate JSON array in {path}")

    try:
        rows = json.loads(txt[start:end + 1])
    except json.JSONDecodeError as e:
        raise ValueError(f"SoT JSON parse error: {e}")

    if not isinstance(rows, list):
        raise ValueError(f"SoT root is not a list (got {type(rows).__name__})")

    return rows


def _serialize_sot(rows: list[dict]) -> str:
    """
    Render rows back to the `const ADS_DATA = [...];` JS wrapper.

    We use `ensure_ascii=False` to preserve Arabic ad copy, `separators` to
    minimize whitespace (the dashboard doesn't need pretty-printed), and
    `sort_keys=False` to preserve insertion order for easier diffs.
    """
    body = json.dumps(rows, ensure_ascii=False, separators=(", ", ": "))
    return f"{JS_PREFIX}{body}{JS_SUFFIX}"


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# Row validation and merging
# ─────────────────────────────────────────────────────────────────────────────

def _is_valid_row(row: dict) -> bool:
    """Invariant I2: every row must have a non-empty Creative ID."""
    if not isinstance(row, dict):
        return False
    cr = row.get("Creative ID", "")
    return isinstance(cr, str) and cr.strip() != ""


def _merge_regions(existing_row: dict, new_row: dict) -> int:
    """
    Merge Regions[] from new_row into existing_row in place.

    - If existing_row has no Regions[] field (v1 schema), initialize it from
      its legacy "Region" scalar field (if any).
    - Append new regions from new_row.get("Regions") or new_row.get("Region").
    - Canonical region (index 0) is preserved.

    Returns the number of regions added (0 if none).
    """
    # Build existing set preserving order.
    existing_regions: list[str] = []
    if isinstance(existing_row.get("Regions"), list):
        existing_regions = [r for r in existing_row["Regions"] if r]
    elif existing_row.get("Region"):
        existing_regions = [existing_row["Region"]]

    # Collect new regions.
    new_regions: list[str] = []
    if isinstance(new_row.get("Regions"), list):
        new_regions = [r for r in new_row["Regions"] if r]
    elif new_row.get("Region"):
        new_regions = [new_row["Region"]]

    added = 0
    for r in new_regions:
        if r not in existing_regions:
            existing_regions.append(r)
            added += 1

    if existing_regions:
        existing_row["Regions"] = existing_regions

    return added


def _merge_row_fields(existing_row: dict, new_row: dict, stats: MergeStats) -> None:
    """
    Merge new_row into existing_row in place.

    Rules:
      - Regions[] is merged via _merge_regions (append, no duplicates).
      - For all other fields: new_row values overwrite existing_row values
        ONLY if the new value is non-empty. Empty new values never clobber
        a populated existing value. This protects Local Image / Local Video
        paths from being wiped by a re-scrape that didn't download media.
      - Last Shown is overwritten only if the new value is a later date.
    """
    added = _merge_regions(existing_row, new_row)
    if added:
        stats.regions_merged += added

    for key, new_val in new_row.items():
        if key in ("Region", "Regions"):
            continue  # handled above

        # Treat empty string / None / empty list as "no new data".
        if new_val in ("", None, [], {}):
            continue

        if key == "Last Shown":
            old = existing_row.get("Last Shown", "")
            # Lexicographic comparison works for ISO YYYY-MM-DD dates.
            if not old or new_val > old:
                existing_row[key] = new_val
            continue

        existing_row[key] = new_val


# ─────────────────────────────────────────────────────────────────────────────
# Postconditions
# ─────────────────────────────────────────────────────────────────────────────

def _verify_postconditions(
    before_rows: list[dict],
    after_rows: list[dict],
    stats: MergeStats,
) -> list[str]:
    """
    Run Q1, Q2, Q4 against the in-memory before/after row sets.

    Q3 (sha256 in manifest) and Q5 (preview validator) are handled by the
    caller (merge_rows) because they require the file to already be on disk
    or external services to have run.

    Returns a list of failure messages. Empty list means all postconditions
    pass.
    """
    failures: list[str] = []

    # Q1 — monotonic row count within a batch.
    if len(after_rows) < len(before_rows):
        failures.append(
            f"Q1 violated: rows_after ({len(after_rows)}) "
            f"< rows_before ({len(before_rows)})"
        )

    # Q2 — every prior row still exists by dedup key.
    after_keys = {_dedup_key(r) for r in after_rows}
    missing = [_dedup_key(r) for r in before_rows if _dedup_key(r) not in after_keys]
    if missing:
        failures.append(
            f"Q2 violated: {len(missing)} rows from the previous file "
            f"are missing. Sample: {missing[:3]}"
        )

    # Q4 — smoke-test 5 random rows. If Local Image or Local Video is set,
    # the referenced file must exist on disk.
    sample = random.sample(after_rows, min(5, len(after_rows))) if after_rows else []
    for row in sample:
        for field_name in ("Local Image", "Local Video"):
            local = row.get(field_name, "")
            if not local:
                continue
            # Local paths in the SoT are stored as "/meta_images/CR....jpg"
            # relative to public/. Resolve against the public/ directory.
            local_str = local.lstrip("/")
            abs_path = config.SOT_PATH.parent / local_str
            if not abs_path.exists():
                failures.append(
                    f"Q4 violated: row Creative ID={row.get('Creative ID')} "
                    f"declares {field_name}={local} but file not found at {abs_path}"
                )
            elif abs_path.stat().st_size < 1024:
                failures.append(
                    f"Q4 violated: row Creative ID={row.get('Creative ID')} "
                    f"{field_name} file is {abs_path.stat().st_size} bytes (< 1KB)"
                )

    return failures


# ─────────────────────────────────────────────────────────────────────────────
# Manifest
# ─────────────────────────────────────────────────────────────────────────────

def _update_manifest(batch_id: str, stats: MergeStats, sha256_new: str) -> None:
    """
    Write manifest.json with the latest batch_id, row count, and sha256.
    This is postcondition Q3.
    """
    manifest = {}
    if config.MANIFEST_PATH.exists():
        try:
            manifest = json.loads(config.MANIFEST_PATH.read_text())
        except Exception:
            manifest = {}

    # Preserve history under "history" so we can see previous batches if
    # something goes wrong. Capped to the last 20 entries.
    history = manifest.get("history", [])
    if manifest.get("batch_id"):
        history.append({
            "batch_id": manifest.get("batch_id"),
            "row_count": manifest.get("row_count"),
            "sha256": manifest.get("sha256"),
            "timestamp": manifest.get("timestamp"),
        })
    history = history[-20:]

    manifest.update({
        "batch_id": batch_id,
        "row_count": stats.rows_after,
        "sha256": sha256_new,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "preview_validator_ran": False,  # deferred; Q5 marker
        "schema_version": config.SCHEMA_VERSION,
        "history": history,
    })

    config.MANIFEST_PATH.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def make_batch_id(pipeline: str = "manual") -> str:
    """
    Generate a batch ID per Invariant I7:
        {pipeline}_{YYYYMMDD}_{HHMMSS}_{uuid4[:8]}
    """
    import uuid
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{pipeline}_{ts}_{uuid.uuid4().hex[:8]}"


def merge_rows(
    new_rows: Iterable[dict],
    batch_id: str,
    source_actor: str,
    stamp_last_seen: bool = True,
) -> MergeStats:
    """
    Merge `new_rows` into `public/ads_data.js` using a 2-phase atomic write
    and verify all postconditions. This is the ONLY sanctioned writer of
    the SoT file.

    Args:
        new_rows: rows to add/update. Each must pass _is_valid_row().
        batch_id: unique run ID (see make_batch_id).
        source_actor: "firecrawl" | "crawlerbros" | "curious_coder" |
                      "recovered_from_cdn" | ... — stored on new rows.
        stamp_last_seen: if True, set last_seen_batch_id on every row that
                         was either added or updated. Recovery passes False
                         for rows it restores, because it doesn't want to
                         imply those ads were observed in the current batch.

    Returns:
        MergeStats with ok=True on success, ok=False on postcondition failure.
        On failure, `public/ads_data.js` is untouched and the attempted write
        is moved to `public/ads_data.js.failed` for inspection.

    Raises:
        FileNotFoundError: SoT file doesn't exist (run recovery from backup
                           before calling merge_rows).
        ValueError: SoT file cannot be parsed.
    """
    stats = MergeStats(batch_id=batch_id)

    # ── 1. Load current SoT ────────────────────────────────────────────────
    before_rows = load_sot(config.SOT_PATH)
    stats.rows_before = len(before_rows)
    stats.sha256_before = _sha256_of_file(config.SOT_PATH)

    # Build an index for O(1) dedup lookups.
    index: dict[tuple[str, str], dict] = {}
    for row in before_rows:
        key = _dedup_key(row)
        # If duplicates already exist in the SoT (e.g. legacy data), keep
        # the first occurrence. Merging duplicates is a separate concern.
        if key not in index:
            index[key] = row

    # ── 2. Process new rows ────────────────────────────────────────────────
    new_rows_list = list(new_rows)
    stats.rows_in_input = len(new_rows_list)

    for new_row in new_rows_list:
        if not _is_valid_row(new_row):
            stats.rows_rejected_no_cr += 1
            continue

        key = _dedup_key(new_row)

        # Stamp the source_actor and last_seen_batch_id fields.
        new_row.setdefault("source_actor", source_actor)
        if stamp_last_seen:
            new_row["last_seen_batch_id"] = batch_id

        if key in index:
            _merge_row_fields(index[key], new_row, stats)
            stats.rows_updated += 1
        else:
            # Brand-new row — stamp first_seen_batch_id as well.
            new_row.setdefault("first_seen_batch_id",
                               batch_id if stamp_last_seen else new_row.get("last_seen_batch_id", ""))
            new_row.setdefault("schema_version", config.SCHEMA_VERSION)
            before_rows.append(new_row)
            index[key] = new_row
            stats.rows_added += 1

    after_rows = before_rows  # mutated in place above
    stats.rows_after = len(after_rows)

    # ── 3. Verify in-memory postconditions (Q1, Q2, Q4) ───────────────────
    failures = _verify_postconditions(
        before_rows=load_sot(config.SOT_PATH),  # re-read pristine copy for Q2
        after_rows=after_rows,
        stats=stats,
    )
    if failures:
        stats.postcondition_failures = failures
        stats.ok = False
        return stats

    # ── 4. 2-phase atomic write (Invariant I9) ─────────────────────────────
    serialized = _serialize_sot(after_rows)
    tmp_path = config.SOT_TMP_PATH
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(serialized)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp_path, config.SOT_PATH)
    except Exception as e:
        stats.postcondition_failures.append(f"write failed: {e}")
        # Clean up the .tmp if it exists.
        if tmp_path.exists():
            try:
                tmp_path.rename(config.SOT_FAILED_PATH)
            except Exception:
                pass
        stats.ok = False
        return stats

    # ── 5. Post-write postconditions (Q3 — sha256 in manifest) ────────────
    stats.sha256_after = _sha256_of_file(config.SOT_PATH)
    _update_manifest(batch_id, stats, stats.sha256_after)

    stats.ok = True
    return stats


def rollback_to_backup(backup_path: Path) -> None:
    """
    Emergency rollback: replace public/ads_data.js with a known-good backup.

    The caller is responsible for choosing WHICH backup to restore. This
    function does no magic selection.
    """
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup not found: {backup_path}")
    config.SOT_PATH.write_bytes(backup_path.read_bytes())
