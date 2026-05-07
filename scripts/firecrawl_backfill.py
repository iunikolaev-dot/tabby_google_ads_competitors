#!/usr/bin/env python3
"""
scripts/firecrawl_backfill.py — One-shot: backfill Embed URL on Google Video
ads still showing placeholders.

After scripts/backfill_embed_urls.py ran on cached staging files, ~190 video
ads still had no Image URL / Embed URL — those are older rows whose Creative
IDs we don't have a staging match for. This script hits each one's
Transparency Center creative page via FireCrawl (which renders JS), parses
the resulting HTML for the displayads-formats iframe URL or a direct image,
and writes back into public/ads_data.js.

Cost: 0$ (FireCrawl free tier, ~190 pages of 500/month).
Time: ~2-3 min depending on FireCrawl latency.

Usage:  python3 scripts/firecrawl_backfill.py [--dry-run] [--limit N]

Env: FIRECRAWL_API_KEY (read from .env via the project's existing pattern)
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parent.parent
ADS_JS = REPO / "public" / "ads_data.js"
ENV_PATH = REPO / ".env"


def load_firecrawl_key() -> str:
    if k := os.environ.get("FIRECRAWL_API_KEY"):
        return k
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if line.startswith("FIRECRAWL_API_KEY="):
                return line.split("=", 1)[1].strip()
    return ""


def load_data() -> list:
    raw = ADS_JS.read_text()
    start = raw.index("[")
    end = raw.rindex("]") + 1
    return json.loads(raw[start:end])


def write_data(data: list) -> None:
    new_raw = "const ADS_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
    ADS_JS.write_text(new_raw)


def firecrawl_scrape(url: str, key: str) -> dict | None:
    """Scrape one URL with FireCrawl. Returns the data dict or None."""
    payload = {
        "url": url,
        "formats": ["html"],
        "waitFor": 4000,  # let the SPA render the creative
        "actions": [
            {"type": "wait", "milliseconds": 3000},
        ],
    }
    try:
        resp = requests.post(
            "https://api.firecrawl.dev/v2/scrape",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
            timeout=90,
        )
    except requests.RequestException as e:
        return None
    if resp.status_code != 200:
        return None
    j = resp.json()
    if not j.get("success"):
        return None
    return j.get("data", {})


# Patterns we accept as a renderable preview source.
EMBED_HOST = "displayads-formats.googleusercontent.com"
DIRECT_IMG_HOSTS = (
    "tpc.googlesyndication.com/archive/simgad/",
    "s0.2mdn.net/",
    "lh3.googleusercontent.com/",
    "i.ytimg.com/vi/",
)


def extract_preview(html: str) -> tuple[str, str]:
    """Return (image_url, embed_url) — at most one is non-empty."""
    if not html:
        return "", ""

    # 1. Direct iframe to displayads-formats (preferred — renders the actual ad)
    iframe_match = re.search(
        r'<iframe[^>]+src="(https://' + re.escape(EMBED_HOST) + r'/[^"]+)"',
        html,
    )
    if iframe_match:
        url = iframe_match.group(1).replace("&amp;", "&")
        # Match what apify_google.py does: prefer .html over .js
        url = url.replace("/preview/content.js", "/preview/content.html")
        return "", url

    # 2. Inline displayads URL anywhere in the HTML (the SPA may not always
    #    iframe it — sometimes it's inside a JS string we can still grep)
    embed_match = re.search(
        r'https?://' + re.escape(EMBED_HOST) + r'/ads/preview/content\.[hj]t?ml?[^\s"<>\\]+',
        html,
    )
    if embed_match:
        url = embed_match.group(0).replace("&amp;", "&")
        url = url.replace("/preview/content.js", "/preview/content.html")
        return "", url

    # 3. Direct image URL (simgad/2mdn/lh3/ytimg)
    for host in DIRECT_IMG_HOSTS:
        m = re.search(r'https?://' + re.escape(host) + r'[^\s"<>\\]+\.(?:jpg|jpeg|png|gif|webp)',
                      html, re.IGNORECASE)
        if m:
            return m.group(0).replace("&amp;", "&"), ""
        # ytimg may not have an extension
        if "ytimg" in host:
            m2 = re.search(r'https?://i\.ytimg\.com/vi/[A-Za-z0-9_-]+/(?:hqdefault|maxresdefault|mqdefault)\.jpg', html)
            if m2:
                return m2.group(0), ""

    return "", ""


def main() -> int:
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    limit = None
    if "--limit" in args:
        i = args.index("--limit")
        limit = int(args[i + 1])

    key = load_firecrawl_key()
    if not key:
        print("FATAL: FIRECRAWL_API_KEY not set", file=sys.stderr)
        return 1

    data = load_data()
    print(f"Loaded {len(data)} ads")

    # Targets: active Google Video ads with no Image URL AND no Embed URL
    targets = [
        a for a in data
        if a.get("Platform") == "Google Ads"
        and a.get("Ad Format") == "Video"
        and (a.get("Status") or "Active") == "Active"
        and not a.get("Image URL")
        and not a.get("Embed URL")
        and a.get("Ad Preview URL")
    ]
    print(f"Targets (active Google Video without preview): {len(targets)}")
    if limit:
        targets = targets[:limit]
        print(f"Limited to: {len(targets)}")

    if not targets:
        print("Nothing to do.")
        return 0

    if dry_run:
        print("DRY RUN — printing first 3 target URLs:")
        for a in targets[:3]:
            print("  ", a["Ad Preview URL"])
        return 0

    embed_added = img_added = failed = 0
    by_competitor = {}
    for i, ad in enumerate(targets, 1):
        url = ad["Ad Preview URL"]
        comp = ad.get("Competitor Name", "?")
        by_competitor.setdefault(comp, {"ok": 0, "fail": 0})
        result = firecrawl_scrape(url, key)
        if not result:
            failed += 1
            by_competitor[comp]["fail"] += 1
            print(f"  [{i}/{len(targets)}] {comp[:12]:12} FAIL")
            time.sleep(0.5)
            continue
        img, embed = extract_preview(result.get("html", ""))
        if embed:
            ad["Embed URL"] = embed
            embed_added += 1
            by_competitor[comp]["ok"] += 1
            print(f"  [{i}/{len(targets)}] {comp[:12]:12} embed")
        elif img:
            ad["Image URL"] = img
            img_added += 1
            by_competitor[comp]["ok"] += 1
            print(f"  [{i}/{len(targets)}] {comp[:12]:12} image")
        else:
            failed += 1
            by_competitor[comp]["fail"] += 1
            print(f"  [{i}/{len(targets)}] {comp[:12]:12} no match")
        time.sleep(0.3)  # be polite to FireCrawl
        # Periodic write — protect progress against ctrl-C
        if i % 25 == 0:
            write_data(data)

    write_data(data)

    print()
    print(f"Done: +{embed_added} Embed URL, +{img_added} Image URL, "
          f"{failed} failed/no-match (of {len(targets)} attempted)")
    print("By competitor:")
    for c, s in sorted(by_competitor.items(), key=lambda x: -x[1]["ok"]):
        print(f"  {c:15} ok={s['ok']:3}  fail={s['fail']:3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
