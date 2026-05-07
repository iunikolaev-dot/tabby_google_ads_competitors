"""
scrapers/apify_google.py — Google Transparency Center fallback via
`crawlerbros/google-ads-scraper`.

This scraper is a FALLBACK only (PRD §4.3, §4.3.2). The orchestrator calls
scrape_competitor() for a specific competitor ONLY when FireCrawl failed to
return ≥ FIRECRAWL_MIN_PREVIEW_COVERAGE for that advertiser in the same run.
It is never called unconditionally, and never called with human approval
implied — run_weekly.py always pauses and asks before invoking this.

Why crawlerbros:
    - `experthasan/google-ads-transparency-api` went into maintenance
      on 2026-04-09 and is structurally unreliable.
    - `crawlerbros` is maintained, 5.0 rating, ~7× cheaper
      ($0.70 per 1,000 ads vs experthasan's start+per-result+per-detail).
    - Returns previewUrl, imageUrl, videoUrl, firstShown, lastShown, format.
    - CAVEAT: videoUrl is YouTube-hosted only, NOT googlevideo.com MP4s.
      The v1 local MP4 pipeline does not apply to new crawlerbros results.
      Pre-existing MP4s in public/google_videos/ are preserved (§4.3.3).

Scraper contract (PRD §4.8 C1):
    scrape_competitor(competitor_config, batch_id) -> {
        "ok": bool, "rows": list[dict], "stats": {...}, "errors": list[str],
    }

Cost contract (PRD §4.3):
    $0.70 per 1,000 ads = $0.0007/ad
    Hard cap $2.00 per run ENFORCED BY CALLER (run_weekly.py).

Cross-references:
    PRD §4.3     crawlerbros cost contract
    PRD §4.3.2   FireCrawl-first fallback rule
    PRD §4.3.3   YouTube-only video URL regression
    PRD §4.5     v2 data model
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date
from typing import Optional

import requests

import config

log = logging.getLogger("scrapers.apify_google")

APIFY_API = "https://api.apify.com/v2"

POLL_INTERVAL_S = 15
MAX_POLL_S = 600

# Default result cap per advertiser — PRD §4.3 uses 200 as the typical value.
DEFAULT_RESULTS_LIMIT = 200


# ─────────────────────────────────────────────────────────────────────────────
# Apify I/O (same shape as apify_meta.py — duplicated intentionally rather
# than extracted, because the two scrapers have different actor slugs,
# different input schemas, and different cost models. Extracting a shared
# helper would couple them in a way that makes future divergence harder.)
# ─────────────────────────────────────────────────────────────────────────────

def _actor_slug(actor: str) -> str:
    return actor.replace("/", "~")


def _start_run(token: str, actor_input: dict) -> tuple[bool, str, str]:
    url = f"{APIFY_API}/acts/{_actor_slug(config.APIFY_GOOGLE_ACTOR)}/runs"
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
# Field extraction
# ─────────────────────────────────────────────────────────────────────────────

def _build_v2_row(
    item: dict,
    competitor: dict,
    batch_id: str,
    today: str,
) -> Optional[dict]:
    """
    Transform a single crawlerbros item into a v2-schema row.

    Expected crawlerbros fields (from actor docs):
        advertiserId, creativeId, previewUrl, imageUrl, videoUrl,
        format, firstShown, lastShown, region
    """
    creative_id = (
        item.get("creativeId")
        or item.get("creative_id")
        or ""
    ).strip()
    if not creative_id:
        return None

    advertiser_id = (
        item.get("advertiserId")
        or item.get("advertiser_id")
        or ""
    )
    image_url = item.get("imageUrl") or item.get("image_url") or ""
    video_url = item.get("videoUrl") or item.get("video_url") or ""
    preview_url = (
        item.get("previewUrl")
        or item.get("preview_url")
        or (f"https://adstransparency.google.com/advertiser/"
            f"{advertiser_id}/creative/{creative_id}" if advertiser_id else "")
    )

    raw_fmt = (item.get("format") or "").upper()

    # Rule 1: Reject formats in GOOGLE_REJECTED_FORMATS (Invariant I3 — TEXT).
    if raw_fmt in config.GOOGLE_REJECTED_FORMATS:
        return None

    # Rule 2: Reject HTML5 rich-media bundles by URL indicator.
    for url_field in (preview_url, image_url, video_url):
        if any(ind in url_field for ind in config.HTML5_REJECT_INDICATORS):
            return None

    # Rule 3: Categorize previewUrl by host. crawlerbros returns previewUrl
    # for nearly every ad, but in two distinct shapes:
    #   - Direct image CDN: tpc.googlesyndication.com/archive/simgad/... ,
    #     s0.2mdn.net/..., lh3.googleusercontent.com/... → use as Image URL
    #   - JS-render embed:  displayads-formats.googleusercontent.com/ads/preview/
    #     content.js?... → swap content.js → content.html (same params) and
    #     store in Embed URL so dashboard.html iframes it
    # Confirmed 2026-05-01: imageUrl is null for ~50–100% of global advertisers'
    # video ads (Revolut/Wise/Monzo/Klarna). The previewUrl is what carries
    # the renderable creative for those.
    embed_url = ""
    if preview_url and "displayads-formats.googleusercontent.com" in preview_url:
        # JS embed → matched HTML page on same parameters
        embed_url = preview_url.replace("/preview/content.js", "/preview/content.html")
    elif preview_url and not image_url:
        # Direct image (simgad/2mdn/lh3/etc.) used as Image URL fallback for
        # both IMAGE-format and VIDEO-format ads when imageUrl is null.
        image_url = preview_url

    fmt = raw_fmt.capitalize() or ("Video" if video_url else "Image")

    first_shown = item.get("firstShown") or item.get("first_shown") or ""
    last_shown = item.get("lastShown") or item.get("last_shown") or today

    # crawlerbros may or may not echo back the requested region. Trust the
    # canonical region from config first; include any extra regions the
    # actor reports.
    canonical_region = competitor.get("google_region") or ""
    regions = [canonical_region] if canonical_region else []
    item_region = item.get("region") or ""
    if item_region and item_region not in regions:
        regions.append(item_region)

    return {
        "schema_version": config.SCHEMA_VERSION,
        "Competitor Name": competitor["name"],
        "Category": competitor.get("category", "Global"),
        "Platform": "Google Ads",
        "Advertiser ID": advertiser_id,
        "Advertiser Name (Transparency Center)": item.get("advertiserName", "") or "",
        "Creative ID": creative_id,
        "Ad Format": fmt,
        "Image URL": image_url,
        "Video URL": video_url,   # YouTube URL, not MP4 (§4.3.3)
        "Embed URL": embed_url,   # iframeable when previewUrl is a JS render
        "Local Image": "",
        "Local Video": "",        # never populated for new crawlerbros results
        "Ad Preview URL": preview_url,
        "Landing Page": item.get("landingPage") or item.get("landing_page") or "",
        "Regions": regions,
        "First Shown": first_shown,
        "Last Shown": last_shown,
        "Date Collected": today,
        "first_seen_batch_id": batch_id,
        "last_seen_batch_id": batch_id,
        "source_actor": "crawlerbros",
        "retired": False,
        "retired_reason": "",
        "preview_status": "unverified",
        "preview_checked_at": "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def _build_actor_input(
    advertiser_id: str,
    region: str,
    results_limit: int,
    format_filter: str = "",
) -> dict:
    """
    Build crawlerbros input for a SINGLE advertiser.

    IMPORTANT: crawlerbros silently fails on multi-URL `startUrls` arrays.
    Empirically (2026-05-01), passing 4–5 URLs returns 0 items OR only the
    first URL's items, with no error. The actor reports SUCCEEDED.
    `scrape_competitor` therefore loops one URL per actor run.

    The region MUST be embedded in the URL itself (`?region=anywhere` or
    `?region=SA`). Passing `region` as a top-level input field also fails
    silently — same root cause, same date.

    `format_filter`: optional, "VIDEO" or "IMAGE" or "TEXT". Some advertisers
    (Klarna, Cash App, Tamara, Ziina as of 2026-05-06) return 0 items on the
    default URL but populated results when `&format=VIDEO` is appended.
    """
    region_param = region or "anywhere"
    url = (
        f"https://adstransparency.google.com/advertiser/{advertiser_id}"
        f"?region={region_param}"
    )
    if format_filter:
        url += f"&format={format_filter}"
    return {
        "startUrls": [{"url": url}],
        "resultsLimit": results_limit,
        "skipDetails": False,  # we need imageUrl/videoUrl/previewUrl details
    }


def scrape_competitor(
    competitor: dict,
    batch_id: str,
    results_limit: int = DEFAULT_RESULTS_LIMIT,
    format_filters: tuple[str, ...] = ("",),
) -> dict:
    """
    Run crawlerbros for a single competitor's advertiser IDs.

    Writes raw items to staging/google_{competitor}_{batch_id}.json before
    returning. Never raises.

    `format_filters`: tuple of format strings to scrape. Default is `("",)`
    which fires one actor run per advertiser with the plain URL. Pass
    `("", "VIDEO")` or `("VIDEO",)` to also (or only) hit `&format=VIDEO`,
    which is required for advertisers whose default page returns 0 ads
    (Klarna/Cash App/Tamara/Ziina as of 2026-05-06). Items are deduped by
    creativeId across format runs.
    """
    name = competitor.get("name", "?")
    advertiser_ids = competitor.get("google_advertiser_ids") or []
    region = competitor.get("google_region") or ""

    result = {
        "ok": False,
        "rows": [],
        "stats": {
            "competitor": name,
            "advertiser_count": len(advertiser_ids),
            "items_fetched": 0,
            "rows_built": 0,
            "run_id": "",
            "dataset_id": "",
            "estimated_cost_usd": 0.0,
        },
        "errors": [],
    }

    if not advertiser_ids:
        result["errors"].append(f"{name}: no google_advertiser_ids configured")
        return result

    token = config.resolve_env("APIFY_TOKEN")
    if not token:
        result["errors"].append("APIFY_TOKEN not set in env")
        return result

    # crawlerbros silently fails on multi-URL inputs (confirmed 2026-05-01).
    # Loop one advertiser × format per actor run; aggregate stats and items.
    log.info(f"Apify Google: {name} / {len(advertiser_ids)} advertiser(s) "
             f"× {len(format_filters)} format(s) / limit={results_limit} "
             f"/ region={region or 'anywhere'}")

    all_items: list[dict] = []
    run_ids: list[str] = []
    dataset_ids: list[str] = []
    seen_creative_ids: set[str] = set()

    for aid in advertiser_ids:
        for fmt in format_filters:
            actor_input = _build_actor_input(aid, region, results_limit, fmt)
            ok, run_id, err = _start_run(token, actor_input)
            if not ok:
                result["errors"].append(f"{name}/{aid}/{fmt or 'all'}: start failed: {err}")
                continue
            run_ids.append(run_id)

            ok, run_data, err = _wait_for_run(token, run_id)
            if not ok:
                result["errors"].append(f"{name}/{aid}/{fmt or 'all'}: run failed: {err}")
                continue

            dataset_id = run_data.get("defaultDatasetId", "")
            if not dataset_id:
                result["errors"].append(f"{name}/{aid}/{fmt or 'all'}: no dataset_id")
                continue
            dataset_ids.append(dataset_id)

            ok, items, err = _fetch_dataset(token, dataset_id)
            if not ok:
                result["errors"].append(
                    f"{name}/{aid}/{fmt or 'all'}: dataset fetch failed: {err}"
                )
                continue

            # Dedupe across format runs by creativeId.
            new_items = []
            for it in items:
                cid = (it.get("creativeId") or it.get("creative_id") or "").strip()
                if cid and cid in seen_creative_ids:
                    continue
                if cid:
                    seen_creative_ids.add(cid)
                new_items.append(it)
            log.info(f"  {aid}/{fmt or 'all'}: {len(items)} items "
                     f"({len(new_items)} new after dedupe)")
            all_items.extend(new_items)

    result["stats"]["run_id"] = ",".join(run_ids)
    result["stats"]["dataset_id"] = ",".join(dataset_ids)
    result["stats"]["items_fetched"] = len(all_items)
    result["stats"]["estimated_cost_usd"] = estimate_cost_usd(len(all_items))

    config.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    safe = name.replace(" ", "_").replace("/", "_")
    (config.STAGING_DIR / f"google_{safe}_{batch_id}.json").write_text(
        json.dumps(all_items, ensure_ascii=False, indent=2)
    )

    today = date.today().isoformat()
    rows: list[dict] = []
    for item in all_items:
        row = _build_v2_row(item, competitor, batch_id, today)
        if row:
            rows.append(row)

    result["stats"]["rows_built"] = len(rows)
    result["rows"] = rows
    # ok=True iff at least one advertiser succeeded
    result["ok"] = len(all_items) > 0 or not advertiser_ids
    return result


def estimate_cost_usd(items_count: int) -> float:
    """crawlerbros: $0.70 per 1,000 ads."""
    return items_count * 0.0007
