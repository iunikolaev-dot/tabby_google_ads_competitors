"""
scrapers/firecrawl_google.py — v2 FireCrawl Google Transparency Center scraper.

This is the DEFAULT Google scraper (PRD §4.3, §4.3.2). FireCrawl is free
within 500 pages/month and returns clean markdown with image-creative
pairings for the Transparency Center.

Key lessons applied from v1:
    1. waitFor=10000 (not 5000). The v1 value caused intermittent 60s
       server-side timeouts because Google's React hydration wasn't complete.
       Validated on 2026-04-09 against Rajhi Bank, EmiratesNBD, Tamara.
    2. formats=["html","markdown"]. HTML is the fallback when markdown
       card pattern misses a creative.
    3. URL must include ?region={code} — advertisers return "not found in
       your region" for the default US bucket if canonical region differs.

Scraper contract (PRD §4.8 C1):
    scrape_competitor(competitor_config) -> {
        "ok": bool,
        "rows": list[dict],    # v2-schema rows ready for merge_rows()
        "stats": {...},
        "errors": list[str],
    }

This function NEVER raises. All errors are structured.

Cost contract (PRD §4.3, §5.2):
    - ≤ MAX_FIRECRAWL_PAGES_PER_RUN pages total per run (enforced by caller)
    - Each advertiser = 1-2 pages (main + optional video filter)
    - Zero monetary cost within free tier

Cross-references:
    PRD §4.3    FireCrawl cost contract
    PRD §4.3.2  FireCrawl-first fallback rule
    PRD §4.5    v2 data model
    PRD §4.8 C1 Scraper return shape
    PRD §10.2   waitFor root cause
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import date
from typing import Optional

import requests

import config

log = logging.getLogger("scrapers.firecrawl_google")

FIRECRAWL_SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP layer
# ─────────────────────────────────────────────────────────────────────────────

def _firecrawl_scrape(url: str, api_key: str) -> tuple[bool, dict, str]:
    """
    POST a single URL to FireCrawl v2/scrape.

    Returns (ok, data, error). `data` is the inner `data` dict on success,
    empty on failure. `error` is a human-readable string on failure.
    """
    payload = {
        "url": url,
        "formats": config.FIRECRAWL_FORMATS,
        "waitFor": config.FIRECRAWL_WAIT_FOR_MS,
    }
    try:
        resp = requests.post(
            FIRECRAWL_SCRAPE_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=config.FIRECRAWL_TIMEOUT_SECONDS,
        )
    except requests.RequestException as e:
        return False, {}, f"request exception: {e}"

    if resp.status_code != 200:
        return False, {}, f"HTTP {resp.status_code}: {resp.text[:300]}"

    try:
        result = resp.json()
    except ValueError as e:
        return False, {}, f"JSON parse: {e}"

    if not result.get("success"):
        return False, {}, f"success=false: {result.get('error', result)}"

    return True, result.get("data", {}) or {}, ""


def _build_advertiser_url(
    advertiser_id: str,
    region: str,
    format_filter: Optional[str] = None,
) -> str:
    """
    Construct the Transparency Center URL with required query params.

    We deliberately omit preset-date. v1 set it to "Last 7 days" but that
    limits inactive-ad visibility — the dashboard needs the full history
    for the Status=Inactive split to work. The SoT's incremental merge
    handles retention; we want everything FireCrawl will return.
    """
    url = f"https://adstransparency.google.com/advertiser/{advertiser_id}"
    params = [f"region={region}"] if region else []
    if format_filter:
        params.append(f"format={format_filter}")
    if params:
        url += "?" + "&".join(params)
    return url


# ─────────────────────────────────────────────────────────────────────────────
# Parsing (ported from v1 firecrawl_scraper.py with v2 schema output)
# ─────────────────────────────────────────────────────────────────────────────

# Host whitelist for ad creative images.
_GOOGLE_IMAGE_HOSTS = (
    "googlesyndication.com",
    "googleusercontent.com",
    "ytimg.com",
    "gstatic.com",
)

# Card pattern: [![](IMG_URL)CONTENT](…creative/CR…)
_CARD_RE = re.compile(
    r'\[!\[\]\(([^)]+)\)'
    r'(.*?)'
    r'\]\('
    r'[^)]*creative/(CR\d+)'
    r'[^)]*\)',
    re.DOTALL,
)

# Text-only ad: [Advertisement (N of M)](…creative/CR…)
_TEXT_RE = re.compile(
    r'\[Advertisement \(\d+ of \d+\)\]'
    r'\([^)]*creative/(CR\d+)[^)]*\)'
)

_CR_RE = re.compile(r"creative/(CR\d+)")


def _normalize_image_url(url: str) -> str:
    if url.startswith("//"):
        return "https:" + url
    return url


def _parse_markdown_cards(
    markdown: str,
    html: str,
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    """
    Parse a FireCrawl response body into:
      - creative_image_map: cid → thumbnail URL (may be "")
      - creative_format_map: cid → "Image" | "Video" | "Text"
      - creative_ids: ordered, deduped list of all creative IDs in the page

    HTML-proximity fallback runs for cids that the markdown card pattern
    missed.
    """
    content = markdown + "\n" + html
    creative_ids = list(dict.fromkeys(_CR_RE.findall(content)))

    image_map: dict[str, str] = {}
    format_map: dict[str, str] = {}

    for m in _CARD_RE.finditer(markdown):
        img_url = _normalize_image_url(m.group(1))
        between = m.group(2) or ""
        cid = m.group(3)

        if any(host in img_url for host in _GOOGLE_IMAGE_HOSTS):
            image_map[cid] = img_url

        if "_videocam_" in between:
            format_map[cid] = "Video"
        elif cid not in format_map:
            format_map[cid] = "Image"

    for m in _TEXT_RE.finditer(markdown):
        cid = m.group(1)
        format_map.setdefault(cid, "Text")
        image_map.setdefault(cid, "")

    # HTML-proximity fallback for cids not found in markdown cards.
    for cid in creative_ids:
        if cid in image_map:
            continue
        proximity_re = rf'(?s)(.{{0,2000}}creative/{re.escape(cid)}.{{0,500}})'
        m = re.search(proximity_re, html)
        if not m:
            continue
        context = m.group(1)
        for img in re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', context):
            img = _normalize_image_url(img)
            if any(host in img for host in _GOOGLE_IMAGE_HOSTS):
                image_map[cid] = img
                break

    return image_map, format_map, creative_ids


def _build_v2_row(
    cid: str,
    image_url: str,
    ad_format: str,
    competitor_name: str,
    category: str,
    advertiser_id: str,
    canonical_region: str,
    batch_id: str,
) -> dict | None:
    """Construct a single v2-schema row from parsed fields. Returns None for rejected formats."""
    # Rule: reject HTML5 rich-media by URL indicator.
    if any(ind in image_url for ind in config.HTML5_REJECT_INDICATORS):
        return None

    # Infer format from image URL if not set.
    if not ad_format and image_url:
        ad_format = "Video" if "ytimg" in image_url else "Image"

    return {
        "schema_version": config.SCHEMA_VERSION,
        "Competitor Name": competitor_name,
        "Category": category,
        "Platform": "Google Ads",
        "Advertiser ID": advertiser_id,
        "Advertiser Name (Transparency Center)": "",
        "Creative ID": cid,
        "Ad Format": ad_format or "Image",
        "Image URL": image_url,
        "Video URL": "",
        "Local Image": "",
        "Local Video": "",
        "Ad Preview URL": (
            f"https://adstransparency.google.com/advertiser/"
            f"{advertiser_id}/creative/{cid}"
        ),
        "Landing Page": "",
        "Regions": [canonical_region] if canonical_region else [],
        "First Shown": "",
        "Last Shown": date.today().isoformat(),
        "Date Collected": date.today().isoformat(),
        "first_seen_batch_id": batch_id,
        "last_seen_batch_id": batch_id,
        "source_actor": "firecrawl",
        "retired": False,
        "retired_reason": "",
        "preview_status": "unverified",
        "preview_checked_at": "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def scrape_competitor(competitor: dict, batch_id: str) -> dict:
    """
    Scrape all advertiser IDs for a competitor via FireCrawl and return
    v2-schema rows.

    Args:
        competitor: dict from config.COMPETITORS
        batch_id:   current run batch ID

    Returns:
        {"ok": bool, "rows": list[dict], "stats": {...}, "errors": list[str]}
        Never raises.
    """
    name = competitor.get("name", "?")
    category = competitor.get("category", "Regional")
    region = competitor.get("google_region") or ""
    advertiser_ids = competitor.get("google_advertiser_ids") or []

    result = {
        "ok": False,
        "rows": [],
        "stats": {
            "competitor": name,
            "advertiser_count": len(advertiser_ids),
            "pages_scraped": 0,
            "creative_ids_found": 0,
            "creatives_with_preview": 0,
            "preview_coverage": 0.0,
        },
        "errors": [],
    }

    if not advertiser_ids:
        result["errors"].append(f"{name}: no google_advertiser_ids configured")
        return result

    api_key = config.resolve_env("FIRECRAWL_KEY")
    if not api_key:
        result["errors"].append("FIRECRAWL_KEY not set in env")
        return result

    seen_cids: set[str] = set()
    rows: list[dict] = []

    for adv_id in advertiser_ids:
        url = _build_advertiser_url(adv_id, region)
        log.info(f"FireCrawl: {name} / {adv_id} / region={region or '-'}")
        ok, data, err = _firecrawl_scrape(url, api_key)
        result["stats"]["pages_scraped"] += 1

        if not ok:
            result["errors"].append(f"{name}/{adv_id}: {err}")
            continue

        image_map, format_map, creative_ids = _parse_markdown_cards(
            data.get("markdown", "") or "",
            data.get("html", "") or "",
        )
        log.info(f"  → {len(creative_ids)} unique creative IDs")

        for cid in creative_ids:
            if cid in seen_cids:
                continue  # dedup across multiple advertiser IDs (e.g. Rajhi)
            seen_cids.add(cid)

            img = image_map.get(cid, "")
            fmt = format_map.get(cid, "")

            # Skip pure text ads — no visual value, per v1 behavior.
            if fmt == "Text":
                continue

            row = _build_v2_row(
                cid=cid,
                image_url=img,
                ad_format=fmt,
                competitor_name=name,
                category=category,
                advertiser_id=adv_id,
                canonical_region=region,
                batch_id=batch_id,
            )
            if row is not None:
                rows.append(row)

    result["stats"]["creative_ids_found"] = len(rows)
    with_preview = sum(1 for r in rows if r.get("Image URL"))
    result["stats"]["creatives_with_preview"] = with_preview
    result["stats"]["preview_coverage"] = (
        with_preview / len(rows) if rows else 0.0
    )
    result["rows"] = rows
    result["ok"] = len(rows) > 0 or not result["errors"]
    return result


def is_firecrawl_healthy_for(result: dict) -> bool:
    """
    Implements the PRD §4.3.2 coverage threshold:
      "FireCrawl returns N rows AND ≥ 70% of those rows have a non-empty
       preview/image/video URL → mark this competitor as 'FireCrawl OK'."

    An empty result set is NOT healthy — it means the advertiser page
    hydrated without loading ads, which is the failure mode we're guarding
    against. The caller should add such competitors to the fallback list.
    """
    if not result.get("ok"):
        return False
    stats = result.get("stats", {})
    if stats.get("creative_ids_found", 0) == 0:
        return False
    return stats.get("preview_coverage", 0.0) >= config.FIRECRAWL_MIN_PREVIEW_COVERAGE
