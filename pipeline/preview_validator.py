"""
pipeline/preview_validator.py — post-merge preview integrity check (PRD §4.10).

After every merge, this validator answers one question for each row:
    "Will this preview actually render in the dashboard?"

It runs three checks per row, stopping at the first conclusive answer:

    Check 1 — Local file:
        If Local Image or Local Video is set, verify the file exists on
        disk and is > 1 KB. → 'ok' | 'broken'

    Check 2 — Remote HEAD:
        HTTP HEAD the Image URL / Video URL. Accept 200 with image/*,
        video/*, or text/html (YouTube embeds). → 'ok' | 'broken' | 'missing'

    Check 3 — Vision sample:
        For 5% of rows newly added in this batch (random sample, capped),
        download and pass to classify_preview(). → 'ok' | 'broken'

Every row gets `preview_status` ∈ {'ok', 'missing', 'broken', 'unverified'}
and `preview_checked_at`.

When a row fails Check 1 or 2, the validator logs to
`logs/preview_misses_{batch_id}.json` for human review.

This module MUTATES rows in place (sets preview_status and
preview_checked_at). Callers must re-serialize via pipeline.merge.

Cross-references:
    PRD §4.10  Preview validator contract
    PRD §4.5   preview_status field
    PRD §5.2   MAX_VISION_CALLS_PER_RUN cap
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

import requests

import config

log = logging.getLogger("pipeline.preview_validator")


# Cap on Vision calls inside a single validator run. The caller (orchestrator)
# is responsible for the per-RUN cap across all modules; this module's cap is
# a belt-and-suspenders local limit so a single call can't go runaway.
VISION_SAMPLE_RATE = 0.05
VISION_CALL_CAP = 100

# HEAD request config
HEAD_TIMEOUT_S = 10
VALID_CONTENT_TYPES = ("image/", "video/", "text/html")


# ─────────────────────────────────────────────────────────────────────────────
# Per-row checks
# ─────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _check_local_file(row: dict) -> Optional[str]:
    """
    Check 1: local file exists and is non-trivial.

    Returns 'ok' / 'broken', or None if no local file is referenced
    (so the next check can run).
    """
    for field_name in ("Local Image", "Local Video"):
        local = row.get(field_name, "")
        if not local:
            continue
        abs_path = config.SOT_PATH.parent / local.lstrip("/")
        if not abs_path.exists():
            return "broken"
        if abs_path.stat().st_size < 1024:
            return "broken"
        return "ok"
    return None  # no local file field — defer to check 2


def _check_remote_head(row: dict) -> Optional[str]:
    """
    Check 2: HEAD the Image URL / Video URL / Ad Preview URL.

    Returns 'ok' / 'broken' / 'missing'. 'missing' means no URL to check.
    """
    urls_to_try = []
    for field_name in ("Image URL", "Video URL", "Ad Preview URL"):
        url = row.get(field_name, "")
        if url:
            urls_to_try.append((field_name, url))

    if not urls_to_try:
        return "missing"

    # Try each URL in preference order. If any is ok, the row is ok.
    last_error: str = ""
    for _field, url in urls_to_try:
        try:
            resp = requests.head(url, timeout=HEAD_TIMEOUT_S, allow_redirects=True)
        except requests.RequestException as e:
            last_error = str(e)
            continue
        if resp.status_code != 200:
            last_error = f"HTTP {resp.status_code}"
            continue
        ct = resp.headers.get("Content-Type", "").lower()
        if any(ct.startswith(t) for t in VALID_CONTENT_TYPES):
            return "ok"
        last_error = f"unexpected content-type {ct}"
    # All URLs failed HEAD.
    log.debug(f"all HEADs failed for CR={row.get('Creative ID','')}: {last_error}")
    return "broken"


def _check_vision_sample(row: dict) -> str:
    """
    Check 3: call OpenAI Vision on the Image URL.

    Returns 'ok' on RENDERABLE, 'broken' otherwise.
    """
    from scrapers.vision_filter import classify_preview

    url = row.get("Image URL") or row.get("Video URL") or ""
    if not url:
        return "broken"
    label = classify_preview(url)
    return "ok" if label == "RENDERABLE" else "broken"


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def validate_rows(
    rows: list[dict],
    batch_id: str,
    newly_added_creative_ids: Optional[set[str]] = None,
    enable_vision: bool = True,
) -> dict:
    """
    Run checks 1-2 against every row in `rows` and check 3 against a 5%
    random sample of newly-added rows. Mutates rows in place.

    Args:
        rows: every row in the current SoT.
        batch_id: current run ID, used only for the log filename.
        newly_added_creative_ids: Creative IDs touched in this batch. Only
                                  these are candidates for the Vision sample.
                                  If None, no Vision calls are made.
        enable_vision: master switch for check 3.

    Returns:
        Summary stats dict.
    """
    stats = {
        "total_rows": len(rows),
        "ok": 0,
        "broken": 0,
        "missing": 0,
        "unverified": 0,
        "vision_calls": 0,
        "vision_budget_exhausted": False,
    }

    misses: list[dict] = []
    now = _now_iso()

    # Decide which creative IDs are eligible for the vision sample.
    newly_added_creative_ids = newly_added_creative_ids or set()
    sample_candidates = [
        r for r in rows
        if r.get("Creative ID") in newly_added_creative_ids
        and (r.get("Image URL") or r.get("Video URL"))
    ]
    sample_size = min(
        VISION_CALL_CAP,
        int(len(sample_candidates) * VISION_SAMPLE_RATE),
    )
    vision_sample_ids: set[str] = set()
    if enable_vision and sample_size > 0:
        sampled = random.sample(sample_candidates, sample_size)
        vision_sample_ids = {r["Creative ID"] for r in sampled}

    for row in rows:
        status = _check_local_file(row)
        if status is None:
            status = _check_remote_head(row)

        # Check 3: Vision sample (only for the picked subset).
        if (
            enable_vision
            and status == "ok"
            and row.get("Creative ID") in vision_sample_ids
            and stats["vision_calls"] < VISION_CALL_CAP
        ):
            vision_status = _check_vision_sample(row)
            stats["vision_calls"] += 1
            if vision_status != "ok":
                status = "broken"

        row["preview_status"] = status
        row["preview_checked_at"] = now
        stats[status] = stats.get(status, 0) + 1

        if status in ("broken", "missing"):
            misses.append({
                "Creative ID": row.get("Creative ID"),
                "Platform": row.get("Platform"),
                "Competitor Name": row.get("Competitor Name"),
                "Image URL": row.get("Image URL", ""),
                "Video URL": row.get("Video URL", ""),
                "Local Image": row.get("Local Image", ""),
                "Local Video": row.get("Local Video", ""),
                "preview_status": status,
            })

    if stats["vision_calls"] >= VISION_CALL_CAP:
        stats["vision_budget_exhausted"] = True

    # Write misses log if any.
    if misses:
        config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        log_path = config.LOGS_DIR / f"preview_misses_{batch_id}.json"
        log_path.write_text(json.dumps(
            {"batch_id": batch_id, "misses": misses, "stats": stats},
            ensure_ascii=False, indent=2,
        ))
        log.info(f"preview_validator: wrote {len(misses)} misses to {log_path.name}")

    return stats
