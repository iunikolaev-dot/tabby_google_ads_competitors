"""
scrapers/apify_linkedin.py — LinkedIn Ad Library scraper via Apify actor
`silva95gustavo/linkedin-ad-library-scraper` (id: AdwCDVyhFcWXQF9tg).

Scope (per user 2026-05-14): GLOBAL competitors only, country filter = ALL.

Pricing model (2026-01-19 onwards): PAY_PER_EVENT
    apify-actor-start       $0.00005 (one-off per run, depends on memory GB)
    ad-without-details      $0.002 / ad      (FREE tier)
    ad-with-details         $0.004 / ad      (FREE tier)

We always use skipDetails=False — we want copy + media URLs + CTAs to
mirror what Meta and Google return. Test budget worst case: 5 Global
competitors × 300 ads × $0.004 = $6.00 (300 is the actor's free-trial cap).

Scraper contract — matches apify_google.py / apify_meta.py:
    scrape_competitor(competitor, batch_id, results_limit=200, ...) -> {
        "ok": bool, "rows": list[dict], "stats": {...}, "errors": list[str],
    }

v2 row shape includes Platform="LinkedIn Ads" so the dashboard's existing
filter dropdown picks it up (one-line dashboard tweak in phase B if we ship).

Cross-references:
    PRD §4.3     paid-actor cost ceiling
    PRD §4.8 C1  scraper contract
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date
from typing import Optional
from urllib.parse import urlencode

import requests

import config

log = logging.getLogger("scrapers.apify_linkedin")

APIFY_API = "https://api.apify.com/v2"
ACTOR_ID = "AdwCDVyhFcWXQF9tg"  # silva95gustavo/linkedin-ad-library-scraper

POLL_INTERVAL_S = 15
MAX_POLL_S = 900   # LinkedIn pagination can be slow; allow 15 min ceiling.

DEFAULT_RESULTS_LIMIT = 200

def build_url(linkedin_handle: str, country: str = "ALL") -> str:
    """Return the actor's expected start URL.

    The actor accepts two input formats:
      1. Company URL — `https://www.linkedin.com/company/<handle>/` —
         simple slug form, no numeric companyId required, but the actor's
         own docs note pagination is limited on this form.
      2. Ad Library search — `…/ad-library/search?accountOwner=<numericId>
         &country=ALL` — requires LinkedIn's internal numeric company ID
         which we don't have configured yet.

    For now we use form (1). If we want to filter by country at the URL
    level later we'll need to migrate to form (2) with numeric IDs. The
    `country` kwarg here is kept for that future API stability — currently
    unused since company URLs return all geographies by default (effectively
    country=ALL, which matches the project scope).
    """
    return f"https://www.linkedin.com/company/{linkedin_handle}/"


# Estimated cost per item (we run with skipDetails=False)
COST_PER_AD_WITH_DETAILS_USD = 0.004
COST_ACTOR_START_USD = 0.00005


# ─────────────────────────────────────────────────────────────────────────────
# Apify I/O — same shape as the other apify_* modules
# ─────────────────────────────────────────────────────────────────────────────

def _start_run(token: str, actor_input: dict) -> tuple[bool, str, str]:
    url = f"{APIFY_API}/acts/{ACTOR_ID}/runs"
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
    last_status = ""
    while elapsed < MAX_POLL_S:
        try:
            resp = requests.get(url, params={"token": token}, timeout=15)
        except requests.RequestException as e:
            return False, {}, f"poll exception: {e}"
        if resp.status_code >= 400:
            return False, {}, f"poll HTTP {resp.status_code}: {resp.text[:200]}"
        data = resp.json().get("data", {})
        status = data.get("status", "")
        if status != last_status:
            log.info(f"  run {run_id}: status={status} elapsed={elapsed}s")
            last_status = status
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
# v2-row mapping (best-effort — actor returns LinkedIn-specific fields and
# the shape will evolve once we see real output).
# ─────────────────────────────────────────────────────────────────────────────

import ast as _ast
import re as _re


def _extract_company_id(advertiser_url: str) -> str:
    """`https://www.linkedin.com/company/9471107` → `9471107`."""
    m = _re.search(r"/company/(\d+)", advertiser_url or "")
    return m.group(1) if m else ""


def _parse_ctas(raw) -> list[str]:
    """Actor returns ctas inconsistently — list[str], Python-repr str, or empty."""
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    if isinstance(raw, str) and raw.strip():
        try:
            v = _ast.literal_eval(raw)
            if isinstance(v, list):
                return [str(x) for x in v if x]
        except (ValueError, SyntaxError):
            return [raw]
    return []


def _parse_availability(raw) -> tuple[str, str]:
    """Actor's `availability` field is sometimes a dict-as-string like
    `"{'end': '2026-05-14', 'start': '2026-04-10'}"`. Returns (start, end)."""
    if isinstance(raw, dict):
        return raw.get("start", "") or "", raw.get("end", "") or ""
    if isinstance(raw, str) and raw.strip():
        try:
            v = _ast.literal_eval(raw)
            if isinstance(v, dict):
                return v.get("start", "") or "", v.get("end", "") or ""
        except (ValueError, SyntaxError):
            pass
    return "", ""


# LinkedIn format → dashboard "Ad Format" bucket.
# Anything we don't explicitly handle still gets stored under its raw LinkedIn
# label in `linkedin_format` for later analysis.
_FORMAT_MAP = {
    "SINGLE_IMAGE": "Image",
    "CAROUSEL": "Image",
    "SPOTLIGHT": "Image",
    "FOLLOWER": "Image",
    "VIDEO": "Video",
    # JOB is REJECTED below — kept here as a comment so the mapping table is
    # complete documentation of formats we've observed: JOB → would-be-Image
}

# Formats we filter out before the merge. Listed by raw LinkedIn label.
LINKEDIN_REJECTED_FORMATS = frozenset({
    "JOB",       # job-postings, not paid creatives — user excluded 2026-05-14
})


def _build_v2_row(item: dict, competitor: dict, batch_id: str, today: str) -> Optional[dict]:
    """Translate a single LinkedIn Ad Library item into a v2-schema row.

    Field mapping derived from the actor's actual output (probed 2026-05-14):
        adId             → Creative ID
        adLibraryUrl     → Ad Preview URL
        advertiserName   → Advertiser Name (Transparency Center)
        advertiserUrl    → Advertiser ID  (parsed numeric companyId)
        body             → Ad Copy        (full text of the ad)
        ctas             → CTAs           (list of CTA labels)
        format           → Ad Format      (mapped to Image/Video buckets)
        imageUrl         → Image URL
        videoUrl         → Video URL
        clickUrl         → Landing Page   (JOB ads only)
        availability     → First/Last Shown (JOB ads only; others lack dates)
        impressions      → Impressions Bucket (e.g. "50k-100k", JOB ads only)
        impressionsPerCountry → kept raw; geographic reach signal
        targeting        → kept raw; coarse targeting description
        paidBy           → Paid By        (legal payer entity)
        advertiserLogo   → Advertiser Logo URL
    """
    ad_id = str(item.get("adId") or item.get("ad_id") or "").strip()
    if not ad_id:
        return None

    raw_fmt = str(item.get("format") or "").upper()

    # Reject job-promotion ads — they're recruiting, not paid creatives. The
    # user explicitly requested filtering these out (2026-05-14). Other
    # unwanted formats can be added to LINKEDIN_REJECTED_FORMATS over time.
    if raw_fmt in LINKEDIN_REJECTED_FORMATS:
        return None

    advertiser_url = item.get("advertiserUrl") or ""
    company_id = _extract_company_id(advertiser_url)

    fmt = _FORMAT_MAP.get(raw_fmt, "Image")  # safe default

    image_url = item.get("imageUrl") or ""
    video_url = item.get("videoUrl") or ""

    ctas = _parse_ctas(item.get("ctas"))
    start_date, end_date = _parse_availability(item.get("availability"))

    return {
        "schema_version": config.SCHEMA_VERSION,
        "Competitor Name": competitor["name"],
        "Category": competitor.get("category", "Global"),
        "Platform": "LinkedIn Ads",
        "Status": "Active",   # LinkedIn ads in the library ARE running ads.
        "Advertiser ID": company_id,
        "Advertiser Name (Transparency Center)": item.get("advertiserName") or "",
        "Creative ID": ad_id,
        "Ad Format": fmt,
        "Image URL": image_url,
        "Video URL": video_url,
        "Embed URL": "",
        "Ad Preview URL": item.get("adLibraryUrl") or "",
        "Landing Page": item.get("clickUrl") or "",
        # Region = "Global" matches the project's dashboard convention for
        # global competitors (Meta + Google both use this). LinkedIn's
        # per-country impression breakdown lives in `impressions_per_country`
        # for downstream analysis if we want it.
        "Region": "Global",
        "Regions": [],
        "First Shown": start_date,
        "Last Shown": end_date or today,
        "Date Collected": today,
        "first_seen_batch_id": batch_id,
        "last_seen_batch_id": batch_id,
        "source_actor": "silva95gustavo/linkedin-ad-library-scraper",
        "retired": False,
        "retired_reason": "",
        "preview_status": "unverified",
        "preview_checked_at": "",
        # LinkedIn-specific extras — dashboard ignores unknown keys
        "linkedin_format": raw_fmt,
        "ad_copy": item.get("body") or "",
        "ctas": ctas,
        "paid_by": item.get("paidBy") or "",
        "advertiser_logo": item.get("advertiserLogo") or "",
        "impressions_bucket": item.get("impressions") or "",
        "impressions_per_country": item.get("impressionsPerCountry") or "",
        "targeting": item.get("targeting") or "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def estimate_cost_usd(items_count: int) -> float:
    """Estimated dollar cost for `items_count` ads (with details)."""
    return COST_ACTOR_START_USD + items_count * COST_PER_AD_WITH_DETAILS_USD


def scrape_competitor(
    competitor: dict,
    batch_id: str,
    results_limit: int = DEFAULT_RESULTS_LIMIT,
    country: str = "ALL",
    skip_details: bool = False,
) -> dict:
    """
    Run the LinkedIn Ad Library scraper for a single competitor.

    Writes raw items to staging/linkedin_{name}_{batch_id}.json before
    returning. Never raises.
    """
    name = competitor.get("name", "?")
    handle = competitor.get("linkedin_handle") or competitor.get("linkedin") or ""

    result: dict = {
        "ok": False,
        "rows": [],
        "stats": {
            "competitor": name,
            "items_fetched": 0,
            "rows_built": 0,
            "run_id": "",
            "dataset_id": "",
            "estimated_cost_usd": 0.0,
            "country": country,
            "results_limit": results_limit,
        },
        "errors": [],
    }

    if not handle:
        result["errors"].append(f"{name}: no linkedin_handle configured")
        return result

    token = config.resolve_env("APIFY_TOKEN")
    if not token:
        result["errors"].append("APIFY_TOKEN not set in env")
        return result

    actor_input = {
        "startUrls": [{"url": build_url(handle, country)}],
        "resultsLimit": results_limit,
        "skipDetails": skip_details,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
    }

    log.info(f"Apify LinkedIn: {name} / handle={handle} / country={country} / "
             f"limit={results_limit} / details={'no' if skip_details else 'yes'}")

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

    log.info(f"  {name}: {len(items)} items")
    result["stats"]["items_fetched"] = len(items)
    result["stats"]["estimated_cost_usd"] = round(estimate_cost_usd(len(items)), 4)

    # Stage raw output for offline replay (matches apify_google.py pattern)
    config.STAGING_DIR.mkdir(parents=True, exist_ok=True)
    safe = name.replace(" ", "_").replace("/", "_")
    (config.STAGING_DIR / f"linkedin_{safe}_{batch_id}.json").write_text(
        json.dumps(items, ensure_ascii=False, indent=2)
    )

    today = date.today().isoformat()
    rows: list[dict] = []
    for item in items:
        row = _build_v2_row(item, competitor, batch_id, today)
        if row:
            rows.append(row)

    result["stats"]["rows_built"] = len(rows)
    result["rows"] = rows
    result["ok"] = len(items) > 0
    return result
