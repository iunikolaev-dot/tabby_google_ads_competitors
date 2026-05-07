#!/usr/bin/env python3
"""
scripts/backfill_embed_urls.py — One-shot: populate Embed URL on existing
Google Ads rows by replaying cached staging/google_*.json files.

Why: scrapers/apify_google.py historically dropped crawlerbros's previewUrl
into the wrong field for VIDEO ads — it ended up in Video URL (which the
dashboard tries as <img src> and fails). Result: ~680 Google Video cards
display placeholders even though crawlerbros gave us a renderable URL.

This script back-fills WITHOUT re-scraping (zero Apify cost) by:
    1. Reading every staging/google_*.json file we have on disk
    2. Building a {creative_id: previewUrl} index (latest entry wins)
    3. For each Google Ads row in public/ads_data.js, populating:
       - Embed URL (when previewUrl is a displayads-formats JS embed,
         swapped from /content.js to /content.html)
       - Image URL (when previewUrl is a direct CDN image and Image URL
         is currently empty)
    4. Writing public/ads_data.js back in place

Usage:  python3 scripts/backfill_embed_urls.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ADS_JS = REPO / "public" / "ads_data.js"
STAGING = REPO / "staging"

JS_EMBED_HOST = "displayads-formats.googleusercontent.com"


def categorize_preview(preview_url: str) -> tuple[str, str]:
    """Return (image_url, embed_url) — exactly one of them is non-empty."""
    if not preview_url:
        return "", ""
    if JS_EMBED_HOST in preview_url:
        # JS render → matched HTML page on same parameters
        return "", preview_url.replace("/preview/content.js", "/preview/content.html")
    # Otherwise treat as direct image CDN (simgad / 2mdn / lh3 / etc.)
    return preview_url, ""


def build_index() -> dict[str, dict]:
    """Build {creative_id: {image, embed, video}} from all staging files (latest wins)."""
    idx: dict[str, dict] = {}
    if not STAGING.is_dir():
        print(f"WARN: no staging dir at {STAGING}", file=sys.stderr)
        return idx
    files = sorted(STAGING.glob("google_*.json"))  # alphabetical → batch_id sort
    for path in files:
        try:
            items = json.loads(path.read_text())
        except Exception as e:
            print(f"  skip {path.name}: {e}", file=sys.stderr)
            continue
        if not isinstance(items, list):
            continue
        for it in items:
            cid = (it.get("creativeId") or it.get("creative_id") or "").strip()
            if not cid:
                continue
            preview = it.get("previewUrl") or it.get("preview_url") or ""
            img_from_preview, embed_from_preview = categorize_preview(preview)
            idx[cid] = {
                "image": it.get("imageUrl") or it.get("image_url") or img_from_preview,
                "embed": embed_from_preview,
                "video": it.get("videoUrl") or it.get("video_url") or "",
            }
    print(f"Indexed {len(idx)} Creative IDs from {len(files)} staging files")
    return idx


def main() -> int:
    if not ADS_JS.exists():
        print(f"FATAL: {ADS_JS} missing", file=sys.stderr)
        return 1

    raw = ADS_JS.read_text()
    start = raw.index("[")
    end = raw.rindex("]") + 1
    data = json.loads(raw[start:end])
    print(f"Loaded {len(data)} ads from {ADS_JS.name}")

    idx = build_index()

    embed_added = img_added = unchanged = nomatch = 0
    for ad in data:
        if ad.get("Platform") != "Google Ads":
            continue
        cid = ad.get("Creative ID", "")
        entry = idx.get(cid)
        if not entry:
            nomatch += 1
            continue
        changed = False
        if entry["embed"] and not ad.get("Embed URL"):
            ad["Embed URL"] = entry["embed"]
            embed_added += 1
            changed = True
        if entry["image"] and not ad.get("Image URL"):
            ad["Image URL"] = entry["image"]
            img_added += 1
            changed = True
        if not changed:
            unchanged += 1

    print(f"Backfill: +{embed_added} Embed URL, +{img_added} Image URL, "
          f"{unchanged} unchanged, {nomatch} no staging match")

    # Preserve the file's original "const ADS_DATA = [...];" wrapper.
    new_raw = "const ADS_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
    ADS_JS.write_text(new_raw)
    print(f"Wrote {ADS_JS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
