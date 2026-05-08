#!/usr/bin/env python3
"""
scripts/cleanup_broken_state.py — One-shot DB cleanup of broken state.

Three actions, all idempotent:
  1. Strip dead `Local Image` and `Local Video` fields from all rows.
     Dashboard already ignores them; the data field misleads any future
     code that re-introduces "prefer local". Vercel can't serve them anyway
     (public/meta_images is .gitignored).

  2. Clear `Image URL` on rows where it points at the now-empty R2 bucket
     (pub-...r2.dev). Those URLs return 404 since yesterday's bucket wipe.
     Dashboard falls back to placeholder until next Apify Meta scrape
     overwrites the field with a fresh FB CDN URL.

  3. Delete Inactive rows that have NO renderable source (no Image URL,
     no Embed URL, no Video URL). Pure dead history — no preview, no
     click-through value. Active rows are NEVER deleted regardless of
     preview state.

Backups:
  - Writes a timestamped backup to backups/ads_data_<timestamp>.js BEFORE
    any change. Restore via cp if anything goes sideways.

Usage:
  python3 scripts/cleanup_broken_state.py --dry-run    # show counts only
  python3 scripts/cleanup_broken_state.py              # apply changes
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ADS_JS = REPO / "public" / "ads_data.js"
BACKUPS = REPO / "backups"

R2_HOST = "r2.dev"

import re
import time as _time

_EXPIRY_RE = re.compile(r"oe=([0-9a-fA-F]+)")


def is_r2(url: str) -> bool:
    return R2_HOST in (url or "")


def is_expired_fb(url: str) -> bool:
    """FB CDN URLs sign with `oe=<hex unix timestamp>`. Return True if past."""
    m = _EXPIRY_RE.search(url or "")
    if not m:
        return False
    try:
        return int(m.group(1), 16) < int(_time.time())
    except ValueError:
        return False


def has_renderable(ad: dict) -> bool:
    """Will the dashboard show anything other than a placeholder?

    Considered broken: R2 URL (bucket wiped), expired FB CDN tokens.
    Video URL alone doesn't count — the dashboard uses it for click-through,
    not for the card thumbnail.
    """
    img = ad.get("Image URL", "") or ""
    if img and not is_r2(img) and not is_expired_fb(img):
        return True
    if ad.get("Embed URL"):
        return True
    if ad.get("Screenshot"):
        return True
    return False


def load() -> list:
    raw = ADS_JS.read_text()
    start = raw.index("[")
    end = raw.rindex("]") + 1
    return json.loads(raw[start:end])


def save(data: list) -> None:
    new_raw = "const ADS_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
    ADS_JS.write_text(new_raw)


def backup() -> Path:
    BACKUPS.mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    dst = BACKUPS / f"ads_data_pre_cleanup_{ts}.js"
    shutil.copy2(ADS_JS, dst)
    return dst


def main() -> int:
    dry = "--dry-run" in sys.argv
    data = load()
    n0 = len(data)
    print(f"Loaded {n0} rows from {ADS_JS.name}")

    # Counters
    stripped_local_img = 0
    stripped_local_vid = 0
    cleared_r2_image = 0
    deleted_inactive = 0

    # Pass 1: strip Local Image / Local Video, clear R2 Image URLs
    for ad in data:
        if "Local Image" in ad and ad["Local Image"]:
            stripped_local_img += 1
        if "Local Video" in ad and ad["Local Video"]:
            stripped_local_vid += 1
        if not dry:
            ad.pop("Local Image", None)
            ad.pop("Local Video", None)
        if is_r2(ad.get("Image URL", "")):
            cleared_r2_image += 1
            if not dry:
                ad["Image URL"] = ""

    # Pass 2: delete dead Inactive rows
    def keep(ad: dict) -> bool:
        nonlocal deleted_inactive
        if (ad.get("Status") or "Active") == "Active":
            return True
        if has_renderable(ad):
            return True
        deleted_inactive += 1
        return False

    if dry:
        # Just count without filtering
        for ad in data:
            if (ad.get("Status") or "Active") != "Active" and not has_renderable(ad):
                deleted_inactive += 1
        new_data = data
    else:
        new_data = [ad for ad in data if keep(ad)]

    n1 = len(new_data)
    print()
    print(f"  stripped Local Image field on   {stripped_local_img:>5} rows")
    print(f"  stripped Local Video field on   {stripped_local_vid:>5} rows")
    print(f"  cleared R2 Image URL on         {cleared_r2_image:>5} active rows")
    print(f"  deleted Inactive dead rows:     {deleted_inactive:>5}")
    print(f"  rows: {n0:,} -> {n1:,}  (delta {n1 - n0:+,})")

    if dry:
        print("\n[DRY RUN] no changes written.")
        return 0

    bk = backup()
    print(f"\nBackup written: {bk}")
    save(new_data)
    print(f"Saved {ADS_JS}")

    # Quick post-conditions
    A = [a for a in new_data if (a.get("Status") or "Active") == "Active"]
    I = [a for a in new_data if a.get("Status") == "Inactive"]
    leftover_r2 = sum(1 for a in A if is_r2(a.get("Image URL", "")))
    leftover_local = sum(1 for a in new_data if a.get("Local Image") or a.get("Local Video"))
    leftover_dead_inact = sum(1 for a in I if not has_renderable(a))
    print()
    print(f"Post-checks (should all be 0): r2_active={leftover_r2}, "
          f"local_fields={leftover_local}, dead_inactive={leftover_dead_inact}")
    print(f"Active: {len(A):,}  Inactive: {len(I):,}  Total: {len(new_data):,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
