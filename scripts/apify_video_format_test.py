#!/usr/bin/env python3
"""
scripts/apify_video_format_test.py — Targeted Apify Google scrape using the
`?format=VIDEO` URL variant for advertisers that return 0 items on the
default URL.

Stage 1 (probe): runs ONE competitor (default: Cash App) with format=VIDEO
to confirm the variant returns the ads we know exist. Cost: ~$0.30.

Stage 2 (full): pass `--all` to extend to Klarna + Cash App + Tamara + Ziina.

Writes results into public/ads_data.js using the same merge keys as
run_weekly.py's merge_and_generate, so existing rows update in place.

Usage:
    python3 scripts/apify_video_format_test.py            # probe Cash App
    python3 scripts/apify_video_format_test.py --all       # all 4 broken
    python3 scripts/apify_video_format_test.py --who Klarna,Tamara

Requires: APIFY_TOKEN in env, /tmp/tabby_approval_<YYYYMMDD>.token present.
"""

from __future__ import annotations

import datetime
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import config  # noqa: E402
from scrapers import apify_google  # noqa: E402

ADS_JS = REPO / "public" / "ads_data.js"

DEFAULT_TARGETS = ["Cash App"]
ALL_TARGETS = ["Klarna", "Cash App", "Tamara", "Ziina"]

TODAY = datetime.date.today().isoformat()
BATCH_ID = f"video_format_probe_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"


def load_data() -> list:
    raw = ADS_JS.read_text()
    start = raw.index("[")
    end = raw.rindex("]") + 1
    return json.loads(raw[start:end])


def write_data(data: list) -> None:
    new_raw = "const ADS_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
    ADS_JS.write_text(new_raw)


def merge_v2_rows(data: list, v2_rows: list, competitor_name: str,
                  is_global: bool) -> tuple[int, int]:
    """Merge v2-shaped rows into ads_data.js with the same key as run_weekly."""
    region_v1 = "Global" if is_global else (
        next((c.get("google_region") for c in config.COMPETITORS
              if c["name"] == competitor_name), "") or ""
    )
    cat_v1 = "Global" if is_global else "GCC"
    name_override = {"Al Rajhi Bank": "Rajhi Bank"}
    display_name = name_override.get(competitor_name, competitor_name)

    by_key = {}
    for ad in data:
        if ad.get("Platform") != "Google Ads":
            continue
        cid = ad.get("Creative ID", "")
        reg = ad.get("Region", "")
        if cid:
            by_key[f"Google Ads|{cid}|{reg}"] = ad

    new_count = updated_count = 0
    for row in v2_rows:
        cid = row.get("Creative ID", "")
        if not cid:
            continue
        key = f"Google Ads|{cid}|{region_v1}"
        existing = by_key.get(key)
        ad_preview = (
            f"https://adstransparency.google.com/advertiser/"
            f"{row.get('Advertiser ID', '')}/creative/{cid}"
        )
        if existing:
            existing.update({
                "Competitor Name": display_name,
                "Category": cat_v1,
                "Region": region_v1,
                "Ad Format": row.get("Ad Format", "Video"),
                "Image URL": row.get("Image URL") or existing.get("Image URL", ""),
                "Video URL": row.get("Video URL") or existing.get("Video URL", ""),
                "Embed URL": row.get("Embed URL") or existing.get("Embed URL", ""),
                "Ad Preview URL": ad_preview,
                "Last Shown": TODAY,
                "Date Collected": TODAY,
                "Status": "Active",
            })
            updated_count += 1
        else:
            data.append({
                "Competitor Name": display_name,
                "Competitor Website": "",
                "Category": cat_v1,
                "Region": region_v1,
                "Advertiser ID": row.get("Advertiser ID", ""),
                "Advertiser Name (Transparency Center)": row.get(
                    "Advertiser Name (Transparency Center)", ""),
                "Creative ID": cid,
                "Ad Format": row.get("Ad Format", "Video"),
                "Image URL": row.get("Image URL", ""),
                "Video URL": row.get("Video URL", ""),
                "Embed URL": row.get("Embed URL", ""),
                "Ad Preview URL": ad_preview,
                "Last Shown": TODAY,
                "Started Running": row.get("First Shown") or TODAY,
                "Date Collected": TODAY,
                "Platform": "Google Ads",
                "Status": "Active",
                "New This Week": "NEW",
                "Scrape Batch ID": BATCH_ID,
            })
            new_count += 1
    return new_count, updated_count


def main() -> int:
    args = sys.argv[1:]
    if "--all" in args:
        targets = ALL_TARGETS
    elif "--who" in args:
        i = args.index("--who")
        targets = args[i + 1].split(",")
    else:
        targets = DEFAULT_TARGETS
    print(f"Targets: {targets}")

    if not config.resolve_env("APIFY_TOKEN"):
        print("FATAL: APIFY_TOKEN not set", file=sys.stderr)
        return 1

    today_token = Path(f"/tmp/tabby_approval_{TODAY.replace('-','')}.token")
    if not today_token.exists() or not today_token.read_text().strip():
        print(f"FATAL: approval token missing at {today_token}", file=sys.stderr)
        return 1

    data = load_data()
    print(f"Loaded {len(data)} ads")

    total_new = total_updated = total_cost = 0
    for name in targets:
        comp = next((c for c in config.COMPETITORS if c["name"] == name), None)
        if not comp:
            print(f"  ✗ {name} not in config.COMPETITORS — skipping")
            continue
        print(f"\n=== {name} (advertiser_ids={comp.get('google_advertiser_ids')}, "
              f"region={comp.get('google_region')}) ===")
        result = apify_google.scrape_competitor(
            comp, BATCH_ID, results_limit=200,
            format_filters=("VIDEO",),  # explicitly request the VIDEO variant
        )
        if result["errors"]:
            print(f"  errors: {result['errors'][:2]}")
        rows = result.get("rows", [])
        cost = result["stats"].get("estimated_cost_usd", 0)
        total_cost += cost
        print(f"  rows built: {len(rows)} (~${cost:.2f})")

        if rows:
            is_global = comp.get("category") == "Global"
            n, u = merge_v2_rows(data, rows, name, is_global)
            total_new += n
            total_updated += u
            print(f"  merged: {n} new, {u} updated")
        # Save after each competitor — protect progress
        write_data(data)

    print(f"\n=== Done ===")
    print(f"Total: {total_new} new, {total_updated} updated, "
          f"~${total_cost:.2f} Apify cost")
    print(f"Data file: {ADS_JS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
