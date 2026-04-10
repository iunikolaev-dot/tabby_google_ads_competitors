"""
scrapers/apify_meta.py — v2 Meta Ad Library scraper via Apify.

PER-COMPETITOR BATCHING (PRD §4.3.1):
    v1 ran `curious_coder/facebook-ads-library-scraper` ONCE for all 13
    competitors in a single actor invocation. One failed page → zero data
    for everyone that week. v2 runs one actor invocation per competitor.
    Cost is identical (pricing is per-result, not per-run; start fees are
    de minimis).

Each call's raw dataset is written to `staging/meta_{competitor}_{batch_id}.json`
before being merged, so we can inspect what was returned if postconditions
later reject the merge.

Scraper contract (PRD §4.8 C1):
    scrape_competitor(competitor_config, batch_id) -> {
        "ok": bool, "rows": list[dict], "stats": {...}, "errors": list[str],
    }

Cost contract (PRD §4.3):
    $0.00075 per ad. Hard cap of $3.00 per run ENFORCED BY CALLER
    (this scraper does not know the cumulative run budget).

Cross-references:
    PRD §4.3    Apify Meta cost contract
    PRD §4.3.1  Per-competitor batching rationale
    PRD §4.5    v2 data model
    PRD §4.8 C1 Scraper return shape
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

import requests

import config

log = logging.getLogger("scrapers.apify_meta")

APIFY_API = "https://api.apify.com/v2"

POLL_INTERVAL_S = 15
MAX_POLL_S = 600  # 10 min ceiling per run


# ─────────────────────────────────────────────────────────────────────────────
# Apify HTTP layer
# ─────────────────────────────────────────────────────────────────────────────

def _actor_slug(actor: str) -> str:
    return actor.replace("/", "~")


def _start_run(token: str, actor_input: dict) -> tuple[bool, str, str]:
    """Start an Apify actor run. Returns (ok, run_id, error)."""
    url = f"{APIFY_API}/acts/{_actor_slug(config.APIFY_META_ACTOR)}/runs"
    try:
        resp = requests.post(
            url,
            params={"token": token},
            json=actor_input,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
    except requests.RequestException as e:
        return False, "", f"request exception: {e}"
    if resp.status_code >= 400:
        return False, "", f"HTTP {resp.status_code}: {resp.text[:300]}"
    try:
        run_id = resp.json()["data"]["id"]
    except (KeyError, ValueError) as e:
        return False, "", f"malformed response: {e}"
    return True, run_id, ""


def _wait_for_run(token: str, run_id: str) -> tuple[bool, dict, str]:
    """Poll until an Apify run finishes. Returns (ok, run_data, error)."""
    url = f"{APIFY_API}/actor-runs/{run_id}"
    elapsed = 0
    while elapsed < MAX_POLL_S:
        try:
            resp = requests.get(url, params={"token": token}, timeout=15)
        except requests.RequestException as e:
            return False, {}, f"poll exception: {e}"
        if resp.status_code >= 400:
            return False, {}, f"poll HTTP {resp.status_code}: {resp.text[:200]}"
        data = resp.json().get("data", {})
        status = data.get("status", "")
        if status == "SUCCEEDED":
            return True, data, ""
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return False, data, f"run ended with status {status}"
        time.sleep(POLL_INTERVAL_S)
        elapsed += POLL_INTERVAL_S
    return False, {}, f"run did not finish within {MAX_POLL_S}s"


def _fetch_dataset(token: str, dataset_id: str) -> tuple[bool, list[dict], str]:
    """Page through an Apify dataset. Returns (ok, items, error)."""
    items: list[dict] = []
    offset = 0
    limit = 100
    while True:
        url = f"{APIFY_API}/datasets/{dataset_id}/items"
        try:
            resp = requests.get(
                url,
                params={"token": token, "offset": offset,
                        "limit": limit, "format": "json"},
                timeout=30,
            )
        except requests.RequestException as e:
            return False, items, f"fetch exception: {e}"
        if resp.status_code >= 400:
            return False, items, f"fetch HTTP {resp.status_code}"
        batch = resp.json() or []
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return True, items, ""


# ─────────────────────────────────────────────────────────────────────────────
# Field extraction (ported from v1 meta_scraper.py)
# ─────────────────────────────────────────────────────────────────────────────

def _detect_format(item: dict) -> str:
    snap = item.get("snapshot", {}) or {}
    if snap.get("videos"):
        return "Video"
    for card in snap.get("cards", []) or []:
        if card.get("video_hd_url") or card.get("video_sd_url"):
            return "Video"
    if snap.get("images"):
        return "Image"
    for card in snap.get("cards", []) or []:
        if card.get("original_image_url") or card.get("resized_image_url"):
            return "Image"
    display_fmt = (snap.get("display_format") or "").upper()
    if "VIDEO" in display_fmt:
        return "Video"
    if "IMAGE" in display_fmt or "CAROUSEL" in display_fmt:
        return "Image"
    return "Image" if snap.get("cards") else "Text"


def _extract_image_url(item: dict) -> str:
    snap = item.get("snapshot", {}) or {}
    for img in snap.get("images", []) or []:
        if isinstance(img, dict):
            url = img.get("original_image_url") or img.get("resized_image_url") or ""
            if url:
                return url
        elif isinstance(img, str):
            return img
    for card in snap.get("cards", []) or []:
        url = card.get("original_image_url") or card.get("resized_image_url") or ""
        if url:
            return url
        url = card.get("video_preview_image_url") or ""
        if url:
            return url
    for vid in snap.get("videos", []) or []:
        url = vid.get("video_preview_image_url") or ""
        if url:
            return url
    return ""


def _extract_video_url(item: dict) -> str:
    snap = item.get("snapshot", {}) or {}
    for vid in snap.get("videos", []) or []:
        url = vid.get("video_hd_url") or vid.get("video_sd_url") or ""
        if url:
            return url
    for card in snap.get("cards", []) or []:
        url = card.get("video_hd_url") or card.get("video_sd_url") or ""
        if url:
            return url
    return ""


def _extract_landing_page(item: dict) -> str:
    snap = item.get("snapshot", {}) or {}
    link = snap.get("link_url") or ""
    if link:
        return link
    for card in snap.get("cards", []) or []:
        link = card.get("link_url") or ""
        if link:
            return link
    return ""


def _extract_dates(item: dict) -> tuple[str, str]:
    """Return (first_shown, last_shown) as YYYY-MM-DD strings or ''."""
    def _clean(s: Optional[str]) -> str:
        return (s or "").split(" ")[0] if s else ""

    start_fmt = _clean(item.get("start_date_formatted"))
    end_fmt = _clean(item.get("end_date_formatted"))
    last = end_fmt or start_fmt
    return start_fmt, last


def _build_v2_row(
    item: dict,
    competitor: dict,
    batch_id: str,
    today: str,
) -> Optional[dict]:
    """Transform a single Apify item into a v2-schema row."""
    creative_id = str(item.get("ad_archive_id") or "").strip()
    if not creative_id:
        return None  # Invariant I2 — will be filtered by merge anyway

    first_shown, last_shown = _extract_dates(item)
    page_name = (
        item.get("page_name")
        or (item.get("snapshot", {}) or {}).get("page_name", "")
        or ""
    )

    return {
        "schema_version": config.SCHEMA_VERSION,
        "Competitor Name": competitor["name"],
        "Category": competitor.get("category", "Regional"),
        "Platform": "Meta Ads",
        "Advertiser ID": "",
        "Advertiser Name (Transparency Center)": page_name,
        "Page ID": competitor.get("meta_page_id", ""),
        "Creative ID": creative_id,
        "Ad Format": _detect_format(item),
        "Image URL": _extract_image_url(item),
        "Video URL": _extract_video_url(item),
        "Local Image": "",
        "Local Video": "",
        "Ad Preview URL": item.get("ad_library_url", "") or "",
        "Landing Page": _extract_landing_page(item),
        "Regions": [],  # Meta's publisher_platform list is not a region
        "First Shown": first_shown,
        "Last Shown": last_shown or today,
        "Date Collected": today,
        "first_seen_batch_id": batch_id,
        "last_seen_batch_id": batch_id,
        "source_actor": "curious_coder",
        "retired": False,
        "retired_reason": "",
        "preview_status": "unverified",
        "preview_checked_at": "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def _build_actor_input(page_id: str, is_cash_app: bool) -> dict:
    """
    Build the actor input for a single Facebook page.

    We preserve the v1 quirk of restricting Cash App to `media_type=video`
    because the image path returns too many cross-brand (Square, BitKey)
    results. The v2 Vision filter (scrapers/vision_filter.py) handles the
    remainder.
    """
    media_type = "video" if is_cash_app else "all"
    url = (
        "https://www.facebook.com/ads/library/?"
        "active_status=active&ad_type=all&country=ALL"
        "&is_targeted_country=false"
        f"&media_type={media_type}&search_type=page"
        f"&view_all_page_id={page_id}"
    )
    return {
        "scrapeAdDetails": True,
        "scrapePageAds.activeStatus": "all",
        "urls": [{"url": url, "method": "GET"}],
    }


def scrape_competitor(competitor: dict, batch_id: str) -> dict:
    """
    Run the Apify Meta actor for a single competitor and return v2 rows.

    Rows are also persisted to staging/meta_{competitor}_{batch_id}.json
    before return, so a postcondition failure downstream doesn't lose
    the scrape.

    Never raises.
    """
    from datetime import date

    name = competitor.get("name", "?")
    page_id = competitor.get("meta_page_id")

    result = {
        "ok": False,
        "rows": [],
        "stats": {
            "competitor": name,
            "page_id": page_id,
            "items_fetched": 0,
            "rows_built": 0,
            "run_id": "",
            "dataset_id": "",
        },
        "errors": [],
    }

    if not page_id:
        result["errors"].append(f"{name}: no meta_page_id configured")
        return result

    token = config.resolve_env("APIFY_TOKEN")
    if not token:
        result["errors"].append("APIFY_TOKEN not set in env")
        return result

    actor_input = _build_actor_input(
        page_id=page_id,
        is_cash_app=(name == "Cash App"),
    )

    log.info(f"Apify Meta: {name} (page_id={page_id}) — starting run")
    ok, run_id, err = _start_run(token, actor_input)
    if not ok:
        result["errors"].append(f"{name}: start failed: {err}")
        return result
    result["stats"]["run_id"] = run_id

    ok, run_data, err = _wait_for_run(token, run_id)
    if not ok:
        result["errors"].append(f"{name}: run failed: {err}")
        return result

    dataset_id = run_data.get("defaultDatasetId", "")
    result["stats"]["dataset_id"] = dataset_id
    if not dataset_id:
        result["errors"].append(f"{name}: no dataset_id on finished run")
        return result

    ok, items, err = _fetch_dataset(token, dataset_id)
    if not ok:
        result["errors"].append(f"{name}: dataset fetch failed: {err}")
        return result

    result["stats"]["items_fetched"] = len(items)
    log.info(f"  {name}: fetched {len(items)} items")

    # Persist raw items to staging/ before transforming — cheap insurance.
    config.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = name.replace(" ", "_").replace("/", "_")
    staging_path = config.STAGING_DIR / f"meta_{safe_name}_{batch_id}.json"
    staging_path.write_text(json.dumps(items, ensure_ascii=False, indent=2))

    today = date.today().isoformat()
    rows: list[dict] = []
    for item in items:
        row = _build_v2_row(item, competitor, batch_id, today)
        if row:
            rows.append(row)

    result["stats"]["rows_built"] = len(rows)
    result["rows"] = rows
    result["ok"] = True
    return result


def estimate_cost_usd(items_count: int) -> float:
    """
    Cost model from PRD §4.3: $0.00075 per ad.
    """
    return items_count * 0.00075
