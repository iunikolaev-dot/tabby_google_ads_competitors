#!/usr/bin/env python3
"""
Unified Weekly Ad Intelligence Pipeline

Runs the full scraping pipeline in one command:
  1. Google Ads via FireCrawl (preset-date=Last+7+days)
  2. OpenAI Vision filter for Cash App (remove Square/BitKey)
  3. Meta Ads via Apify
  4. Merge & generate dashboard
  5. Deploy to Vercel

Usage:
    python3 run_weekly.py

Budget per run:
    FireCrawl: ~20 pages (free plan: 500/month → supports 25 runs)
    Apify:     1 actor run (free tier)
    OpenAI:    ~50 image classifications (~$0.01)
"""

import json
import datetime
import os
import re
import time
import logging
import subprocess
import sys
from typing import Optional

import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("weekly")

# ── Load .env ────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, ".env")

ENV = {}
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                ENV[k.strip()] = v.strip()

FIRECRAWL_API_KEY = ENV.get("FIRECRAWL_API_KEY", "")
APIFY_TOKEN = ENV.get("APIFY_TOKEN", "")
OPENAI_API_KEY = ENV.get("OPENAI_API_KEY", "")

# ── Constants ────────────────────────────────────────────────────────────────
PUBLIC_DIR = os.path.join(SCRIPT_DIR, "public")
ADS_JS_PATH = os.path.join(PUBLIC_DIR, "ads_data.js")
ADS_JSON_PATH = os.path.join(SCRIPT_DIR, "ads_data.json")
META_IMAGES_DIR = os.path.join(PUBLIC_DIR, "meta_images")

TODAY = datetime.date.today().isoformat()
BATCH_ID = f"weekly_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ═════════════════════════════════════════════════════════════════════════════
# STEP 1: GOOGLE ADS VIA FIRECRAWL
# ═════════════════════════════════════════════════════════════════════════════

GOOGLE_COMPETITORS = [
    # Global competitors — scraped with NO region filter (all regions).
    # Do NOT add region-specific entries for global brands like Revolut/Monzo/
    # Cash App/Wise/Klarna — they target worldwide.
    {"name": "Revolut", "region": "Global", "cat": "Global", "web": "https://www.revolut.com/",
     "adv_id": "AR07098428377224183809", "adv_name": "Revolut Ltd"},
    {"name": "Monzo", "region": "Global", "cat": "Global", "web": "https://monzo.com/",
     "adv_id": "AR07289389941828616193", "adv_name": "MONZO BANK LIMITED"},
    {"name": "Cash App", "region": "Global", "cat": "Global", "web": "https://cash.app/",
     "adv_id": "AR14896030700992987137", "adv_name": "Block, Inc."},
    {"name": "Wise", "region": "Global", "cat": "Global", "web": "https://wise.com/",
     "adv_id": "AR14378710480124379137", "adv_name": "Wise Payments Limited"},
    {"name": "Klarna", "region": "Global", "cat": "Global", "web": "https://www.klarna.com/",
     "adv_id": "AR03841049863391281153", "adv_name": "Klarna AB"},
    {"name": "Tamara", "region": "SA", "cat": "GCC", "web": "https://tamara.co/",
     "adv_id": "AR02766979019476566017", "adv_name": "Tamara"},
    {"name": "Tamara", "region": "AE", "cat": "GCC", "web": "https://tamara.co/",
     "adv_id": "AR02766979019476566017", "adv_name": "Tamara"},
    {"name": "Rajhi Bank", "region": "SA", "cat": "GCC", "web": "https://www.alrajhibank.com.sa/",
     "adv_id": "AR07393135804576432129", "adv_name": "Al Rajhi Banking & Investment Corp."},
    {"name": "Rajhi Bank", "region": "SA", "cat": "GCC", "web": "https://www.alrajhibank.com.sa/",
     "adv_id": "AR17149597601662763009", "adv_name": "Al Rajhi Banking and Investment Corporation"},
    {"name": "EmiratesNBD", "region": "AE", "cat": "GCC", "web": "https://www.emiratesnbd.com/",
     "adv_id": "AR11606100870541869057", "adv_name": "EMIRATES NBD (P.J.S.C)"},
    {"name": "Ziina", "region": "AE", "cat": "GCC", "web": "https://ziina.com/",
     "adv_id": "AR06959610023805796353", "adv_name": "Ziina"},
]


def firecrawl_scrape(url: str) -> Optional[dict]:
    """Scrape a URL via FireCrawl with scroll actions to trigger lazy-loading.

    Google Transparency Center lazy-loads ad thumbnails as you scroll.
    We scroll down multiple times with waits to force all thumbnails to render.
    """
    payload = {
        "url": url,
        "formats": ["html", "markdown"],
        "waitFor": 3000,
        "actions": [
            {"type": "wait", "milliseconds": 3000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 2000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 2000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 2000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 2000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 3000},
        ],
    }
    try:
        resp = requests.post(
            "https://api.firecrawl.dev/v2/scrape",
            headers={"Authorization": f"Bearer {FIRECRAWL_API_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=180,
        )
        if resp.status_code != 200:
            log.warning(f"  FireCrawl HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        result = resp.json()
        if not result.get("success"):
            log.warning(f"  FireCrawl error: {str(result.get('error', ''))[:200]}")
            return None
        return result.get("data", {})
    except Exception as e:
        log.warning(f"  FireCrawl error: {e}")
        return None


def parse_google_ads(data: dict, adv_id: str, adv_name: str) -> list:
    """Parse ad creative IDs and image URLs from FireCrawl scrape.

    Strategy:
    1. Get unique creative IDs from markdown+html
    2. Try markdown card pattern first (image ads)
    3. For video ads and missing cards, walk the HTML linearly:
       find every ad thumbnail URL and pair it with the NEXT creative/CR
       that appears after it in the DOM. This handles the fact that Google
       Transparency Center renders thumbnails as CSS background-image URLs,
       followed ~6000 chars later by the creative link.
    """
    md = data.get("markdown", "")
    html = data.get("html", "")
    content = md + "\n" + html

    cids = list(dict.fromkeys(re.findall(r"creative/(CR\d+)", content)))
    if not cids:
        return []

    img_map = {}
    fmt_map = {}

    # ── Pass 1: markdown card pattern ([![](url)](creative/CR)) ──────
    card_re = re.compile(
        r"\[!\[\]\(([^)]+)\)(.*?)\]\([^)]*creative/(CR\d+)[^)]*\)", re.DOTALL
    )
    for m in card_re.finditer(md):
        img, between, cid = m.group(1), m.group(2), m.group(3)
        if img.startswith("//"):
            img = "https:" + img
        if any(d in img for d in ["googlesyndication", "googleusercontent", "ytimg", "gstatic"]):
            img_map[cid] = img
        fmt_map[cid] = "Video" if "_videocam_" in between else "Image"

    # ── NO HTML walker ──────────────────────────────────────────────
    # Previous versions tried to walk the HTML to find video thumbnails.
    # This does not work: the ytimg URLs live inside a separate
    # `all-video-container` DOM element (a video carousel) that has NO
    # connection to the ad card grid. The creative IDs appear 150k+ chars
    # later in the DOM. Any positional pairing is wrong 100% of the time.
    #
    # Video ads without a markdown thumbnail will show a placeholder
    # "▶ Video Ad → View on Google" card. This is honest and correct.

    # ── Pass 3: detect format from markdown _videocam_ markers ──────
    # For ads where markdown shows videocam but we got image from HTML
    videocam_re = re.compile(
        r"_videocam_[^[]*\[Advertisement[^\]]*\]\([^)]*creative/(CR\d+)"
    )
    for m in videocam_re.finditer(md):
        fmt_map[m.group(1)] = "Video"

    # Also: any creative with an ytimg URL is definitely video
    for cid, img in img_map.items():
        if "ytimg" in img:
            fmt_map[cid] = "Video"

    ads = []
    for cid in cids:
        img = img_map.get(cid, "")
        fmt = fmt_map.get(cid, "")
        if not fmt and img:
            fmt = "Video" if "ytimg" in img else "Image"
        if fmt == "Text":
            continue  # Skip text ads only
        # Keep ads without thumbnails — they'll show a video placeholder
        ads.append({
            "cid": cid, "img": img, "fmt": fmt or "Video",
            "adv_id": adv_id, "adv_name": adv_name,
        })
    return ads


def build_url(adv_id: str, region: str, fmt: str = "") -> str:
    """Build Transparency Center URL with Last 7 days filter."""
    url = f"https://adstransparency.google.com/advertiser/{adv_id}"
    params = []
    if region not in ("Global", "anywhere"):
        params.append(f"region={region}")
    if fmt:
        params.append(f"format={fmt}")
    params.append("preset-date=Last+7+days")
    return url + "?" + "&".join(params)


def scrape_google_ads() -> list:
    """Step 1: Scrape all Google Ads competitors via FireCrawl."""
    log.info("=" * 60)
    log.info("STEP 1: Google Ads via FireCrawl")
    log.info("=" * 60)

    if not FIRECRAWL_API_KEY:
        log.error("FIRECRAWL_API_KEY not set in .env — skipping Google Ads")
        return []

    all_ads = []
    pages_used = 0
    scraped_advs = set()  # Avoid scraping same advertiser+region twice

    for comp in GOOGLE_COMPETITORS:
        adv_id = comp["adv_id"]
        region = comp["region"]
        key = f"{adv_id}_{region}"

        if key in scraped_advs:
            log.info(f"  Reusing {comp['name']} ({region}) — already scraped")
            # Copy ads from previous scrape of same advertiser
            for ad in all_ads:
                if ad["adv_id"] == adv_id and ad["_region"] == region and ad["_name"] != comp["name"]:
                    all_ads.append({**ad, "_name": comp["name"], "_web": comp["web"], "_cat": comp["cat"]})
            continue

        # Default page (images)
        url = build_url(adv_id, region)
        log.info(f"  {comp['name']} ({region}): scraping...")
        data = firecrawl_scrape(url)
        pages_used += 1
        ads = parse_google_ads(data, adv_id, comp["adv_name"]) if data else []
        log.info(f"    Default: {len(ads)} ads")

        # VIDEO page
        time.sleep(3)
        url_v = build_url(adv_id, region, fmt="VIDEO")
        data_v = firecrawl_scrape(url_v)
        pages_used += 1
        if data_v:
            vads = parse_google_ads(data_v, adv_id, comp["adv_name"])
            existing = {a["cid"] for a in ads}
            new_v = [a for a in vads if a["cid"] not in existing]
            # Mark existing ads as Video if found on video page
            v_cids = {a["cid"] for a in vads}
            for a in ads:
                if a["cid"] in v_cids:
                    a["fmt"] = "Video"
            ads.extend(new_v)
            log.info(f"    Video: +{len(new_v)} ads")

        # Tag with competitor metadata
        for a in ads:
            a["_name"] = comp["name"]
            a["_web"] = comp["web"]
            a["_cat"] = comp["cat"]
            a["_region"] = region

        all_ads.extend(ads)
        scraped_advs.add(key)
        log.info(f"    Total: {len(ads)} ads")
        time.sleep(3)

    log.info(f"  Google Ads done: {len(all_ads)} ads, {pages_used} FireCrawl pages used")
    return all_ads


def scrape_google_ads_apify() -> list:
    """Step 1 (preferred): Google Ads via Apify (crawlerbros).

    Replaces FireCrawl as the primary Google scraper. FireCrawl was unreliable —
    multiple 180s timeouts per run, silent zero-ad returns for whole competitors.
    Apify (crawlerbros) returns reliably in ~30s/advertiser at $0.70/1k ads.

    Returns ads in the v1-shaped dict consumed by merge_and_generate. Hard caps
    per-run cost at $2.00 (PRD §4.3).
    """
    log.info("=" * 60)
    log.info("STEP 1: Google Ads via Apify (crawlerbros)")
    log.info("=" * 60)

    if not APIFY_TOKEN:
        log.error("APIFY_TOKEN not set in .env — skipping Google Ads")
        return []

    # Lazy import to avoid coupling run_weekly to v2 modules at import time.
    import config as cfg
    from scrapers import apify_google

    competitors = [c for c in cfg.COMPETITORS if c.get("google_advertiser_ids")]
    log.info(f"  {len(competitors)} competitors with Google ads")

    all_ads: list = []
    total_cost = 0.0
    COST_CAP_USD = 2.00

    for comp in competitors:
        if total_cost >= COST_CAP_USD:
            log.warning(f"  Cost cap ${COST_CAP_USD} reached — skipping rest")
            break
        region_label = comp.get("google_region") or "anywhere"
        log.info(f"  {comp['name']} ({region_label}): scraping...")
        try:
            result = apify_google.scrape_competitor(comp, BATCH_ID, results_limit=200)
        except Exception as e:
            log.warning(f"    Exception: {e}")
            continue
        if not result["ok"]:
            log.warning(f"    Failed: {'; '.join(result['errors'][:2])}")
            continue

        rows = result["rows"]
        cost = result["stats"]["estimated_cost_usd"]
        total_cost += cost
        log.info(f"    Got {len(rows)} ads (~${cost:.2f})")

        is_global = comp.get("category") == "Global"
        region_v1 = "Global" if is_global else (comp.get("google_region") or "")
        cat_v1 = "Global" if is_global else "GCC"
        display_name = _NAME_OVERRIDE.get(comp["name"], comp["name"])

        for row in rows:
            all_ads.append({
                "cid": row["Creative ID"],
                "adv_id": row["Advertiser ID"],
                "adv_name": row.get("Advertiser Name (Transparency Center)") or "",
                "fmt": row["Ad Format"],
                "img": row.get("Image URL", "") or "",
                "embed": row.get("Embed URL", "") or "",
                "vid": row.get("Video URL", "") or "",
                "_name": display_name,
                "_web": _WEBSITES.get(display_name, ""),
                "_cat": cat_v1,
                "_region": region_v1,
            })

    log.info(f"  Google Ads done: {len(all_ads)} ads, est. cost: ${total_cost:.2f}")
    return all_ads


# ═════════════════════════════════════════════════════════════════════════════
# STEP 2: OPENAI VISION FILTER (Cash App → remove Square/BitKey)
# ═════════════════════════════════════════════════════════════════════════════

def filter_cash_app_ads(google_ads: list) -> list:
    """Step 2: Use OpenAI Vision to remove Square/BitKey from Cash App ads."""
    log.info("=" * 60)
    log.info("STEP 2: OpenAI Vision filter for Cash App")
    log.info("=" * 60)

    if not OPENAI_API_KEY:
        log.warning("OPENAI_API_KEY not set — skipping Vision filter")
        return google_ads

    cash_ads = [a for a in google_ads if a["_name"] == "Cash App"]
    if not cash_ads:
        log.info("  No Cash App ads to filter")
        return google_ads

    # Deduplicate by image URL
    url_to_cids = {}
    for a in cash_ads:
        img = a["img"]
        if img not in url_to_cids:
            url_to_cids[img] = []
        url_to_cids[img].append(a["cid"])

    log.info(f"  Scanning {len(url_to_cids)} unique Cash App images...")

    square_cids = set()
    for i, (url, cids) in enumerate(url_to_cids.items()):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": [
                        {"type": "text", "text": "Which brand is this ad for? Reply with EXACTLY one word: CASHAPP, SQUARE, BITKEY, or UNKNOWN."},
                        {"type": "image_url", "image_url": {"url": url}},
                    ]}],
                    "max_tokens": 10,
                },
                timeout=15,
            )
            answer = resp.json()["choices"][0]["message"]["content"].strip().upper()
        except Exception:
            answer = "UNKNOWN"

        if answer in ("SQUARE", "BITKEY"):
            square_cids.update(cids)
            log.info(f"    [{i+1}/{len(url_to_cids)}] {answer} — removing {len(cids)} ads")

        time.sleep(0.3)

    if square_cids:
        before = len(google_ads)
        google_ads = [a for a in google_ads if a["cid"] not in square_cids]
        log.info(f"  Removed {before - len(google_ads)} Square/BitKey ads")
    else:
        log.info("  No Square/BitKey ads found")

    return google_ads


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3: META ADS VIA APIFY
# ═════════════════════════════════════════════════════════════════════════════

# Display-name and website overrides keyed by config.COMPETITORS["name"].
# Dashboard rows historically used "Rajhi Bank"; config canonical is "Al Rajhi
# Bank". config.COMPETITORS doesn't carry a website field, so we keep the map
# here. Any new competitor added to config also needs an entry here.
_NAME_OVERRIDE = {"Al Rajhi Bank": "Rajhi Bank"}
_WEBSITES = {
    "Klarna": "https://www.klarna.com/",
    "Wise": "https://wise.com/",
    "Monzo": "https://monzo.com/",
    "Cash App": "https://cash.app/",
    "Revolut": "https://www.revolut.com/",
    "Tamara": "https://www.tamara.co/",
    "EmiratesNBD": "https://www.emiratesnbd.com/",
    "Rajhi Bank": "https://www.alrajhibank.com.sa/",
    "Ziina": "https://ziina.com/",
    "Tiqmo": "https://tiqmo.com/",
    "D360 Bank": "https://www.d360.bank/",
    "Barq": "https://usebarq.com/",
    "Wio Bank": "https://wio.io/",
    "STC Bank": "https://www.stcbank.com.sa/",
    "HALA Payment": "https://hala.com/",
    "Alaan": "https://www.alaan.com/",
}


def _build_meta_page_map() -> dict:
    """Derive META_PAGE_MAP from config.COMPETITORS (single source of truth).

    Previously hardcoded — fell out of sync with config and silently skipped
    Wise + Revolut. Pulling from config catches new competitors automatically.
    """
    import config as cfg
    m: dict = {}
    for c in cfg.COMPETITORS:
        pid = c.get("meta_page_id")
        if not pid:
            continue
        display = _NAME_OVERRIDE.get(c["name"], c["name"])
        # Dashboard convention: "Global" stays, "Regional" → "GCC"
        cat = "Global" if c.get("category") == "Global" else "GCC"
        m[pid] = (display, cat, _WEBSITES.get(display, ""))
    return m


META_PAGE_MAP = _build_meta_page_map()

APIFY_API = "https://api.apify.com/v2"


def scrape_meta_ads() -> list:
    """Step 3: Scrape Meta Ads via Apify."""
    log.info("=" * 60)
    log.info("STEP 3: Meta Ads via Apify")
    log.info("=" * 60)

    if not APIFY_TOKEN:
        log.error("APIFY_TOKEN not set in .env — skipping Meta Ads")
        return []

    # Build actor input
    actor_input = {
        "scrapeAdDetails": True,
        "scrapePageAds.activeStatus": "all",
        "urls": [
            {"url": f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type={'video' if pid == '888799511134149' else 'all'}&search_type=page&view_all_page_id={pid}", "method": "GET"}
            for pid in META_PAGE_MAP
        ],
    }

    # Start actor
    log.info(f"  Starting Apify actor for {len(META_PAGE_MAP)} pages...")
    try:
        resp = requests.post(
            f"{APIFY_API}/acts/curious_coder~facebook-ads-library-scraper/runs",
            params={"token": APIFY_TOKEN},
            json=actor_input,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["data"]["id"]
        log.info(f"  Actor run started: {run_id}")
    except Exception as e:
        log.error(f"  Failed to start Apify actor: {e}")
        return []

    # Poll for completion
    elapsed = 0
    max_wait = 600
    while elapsed < max_wait:
        try:
            r = requests.get(f"{APIFY_API}/actor-runs/{run_id}", params={"token": APIFY_TOKEN}, timeout=15)
            status = r.json()["data"]["status"]
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                log.info(f"  Actor {status} ({elapsed}s)")
                if status != "SUCCEEDED":
                    return []
                dataset_id = r.json()["data"]["defaultDatasetId"]
                break
            log.info(f"  Waiting... ({status}, {elapsed}s)")
            time.sleep(30)
            elapsed += 30
        except Exception as e:
            log.warning(f"  Poll error: {e}")
            time.sleep(30)
            elapsed += 30
    else:
        log.error("  Apify actor timed out")
        return []

    # Fetch results
    items = []
    offset = 0
    while True:
        r = requests.get(
            f"{APIFY_API}/datasets/{dataset_id}/items",
            params={"token": APIFY_TOKEN, "offset": offset, "limit": 100, "format": "json"},
            timeout=30,
        )
        batch = r.json()
        if not batch:
            break
        items.extend(batch)
        if len(batch) < 100:
            break
        offset += 100

    log.info(f"  Fetched {len(items)} items from Apify")

    # Transform to our format
    meta_ads = []
    for item in items:
        page_id = str(item.get("page_id", ""))
        if page_id not in META_PAGE_MAP:
            continue

        name, category, website = META_PAGE_MAP[page_id]
        snapshot = item.get("snapshot", {})

        # Detect format
        videos = snapshot.get("videos", [])
        cards = snapshot.get("cards", [])
        if videos or any(c.get("video_hd_url") or c.get("video_sd_url") for c in cards):
            fmt = "Video"
        elif snapshot.get("images") or any(c.get("original_image_url") for c in cards):
            fmt = "Image"
        else:
            fmt = "Image"

        # Extract image URL
        img = ""
        for i in snapshot.get("images", []):
            if isinstance(i, dict):
                img = i.get("original_image_url") or i.get("resized_image_url", "")
            elif isinstance(i, str):
                img = i
            if img:
                break
        if not img:
            for c in cards:
                img = c.get("original_image_url") or c.get("resized_image_url") or c.get("video_preview_image_url", "")
                if img:
                    break
        if not img:
            for v in videos:
                img = v.get("video_preview_image_url", "")
                if img:
                    break

        # Extract video URL
        vid = ""
        for v in videos:
            vid = v.get("video_hd_url") or v.get("video_sd_url", "")
            if vid:
                break
        if not vid:
            for c in cards:
                vid = c.get("video_hd_url") or c.get("video_sd_url", "")
                if vid:
                    break

        # Landing page
        landing = snapshot.get("link_url", "")
        if not landing:
            for c in cards:
                landing = c.get("link_url", "")
                if landing:
                    break

        # Dates
        end_fmt = item.get("end_date_formatted", "")
        start_fmt = item.get("start_date_formatted", "")
        last_shown = (end_fmt or start_fmt or "").split(" ")[0]
        started = (start_fmt or "").split(" ")[0]

        # Publisher platforms as region
        platforms = item.get("publisher_platform", [])
        region = ", ".join(platforms) if platforms else "Meta"

        creative_id = str(item.get("ad_archive_id", ""))
        page_name = item.get("page_name", "") or snapshot.get("page_name", "")

        meta_ads.append({
            "Competitor Name": name,
            "Competitor Website": website,
            "Category": category,
            "Region": region,
            "Advertiser ID": page_id,
            "Advertiser Name (Transparency Center)": page_name,
            "Creative ID": creative_id,
            "Ad Format": fmt,
            "Last Shown": last_shown,
            "Started Running": started,
            "Ad Preview URL": item.get("ad_library_url", ""),
            "Landing Page / Destination URL": landing,
            "Image URL": img,
            "Video URL": vid,
            "Date Collected": TODAY,
            "New This Week": "",
            "Scrape Batch ID": BATCH_ID,
            "Platform": "Meta Ads",
            "Status": "Active",
        })

    log.info(f"  Transformed {len(meta_ads)} Meta ads")
    # Image URLs come straight from Apify (Meta CDN). They expire in 5-14
    # days, so this pipeline must run weekly to keep previews fresh.
    return meta_ads


# ═════════════════════════════════════════════════════════════════════════════
# STEP 4: MERGE & GENERATE DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════

def merge_and_generate(google_ads: list, meta_ads: list):
    """Step 4: True incremental merge — preserves history, updates status.

    Rules:
    - New ad (not seen before) → add with Status=Active, New This Week=NEW
    - Existing ad seen again → update Last Shown=today, Status=Active, clear NEW flag
    - Existing ad NOT seen this run → keep in DB, recompute Status based on Last Shown
    - Nothing is ever deleted. History accumulates forever.
    """
    log.info("=" * 60)
    log.info("STEP 4: Incremental merge & generate dashboard")
    log.info("=" * 60)

    # Load ALL existing data (both platforms)
    existing = []
    if os.path.exists(ADS_JS_PATH):
        try:
            with open(ADS_JS_PATH) as f:
                raw = f.read()
            start = raw.index("[")
            existing = json.loads(raw[start:raw.rindex("]") + 1])
            log.info(f"  Loaded {len(existing)} existing ads")
        except Exception as e:
            log.warning(f"  Could not load existing data: {e}")

    # Composite key: platform + creative_id + region
    def make_key(d):
        platform = d.get("Platform", "Google Ads")
        cid = d.get("Creative ID", "")
        region = d.get("Region", "")
        return f"{platform}|{cid}|{region}"

    existing_map = {make_key(d): d for d in existing}

    # Track which keys were seen this run
    seen_this_run = set()

    # ── Merge Google Ads (incremental) ───────────────────────────────
    new_google = 0
    updated_google = 0
    for ad in google_ads:
        key = f"Google Ads|{ad['cid']}|{ad['_region']}"
        seen_this_run.add(key)
        d = existing_map.get(key, {})
        is_new = key not in existing_map

        # Update fields; preserve existing assets if new scrape didn't find them
        new_img = ad.get("img") or d.get("Image URL", "")
        new_embed = ad.get("embed") or d.get("Embed URL", "")
        new_vid = ad.get("vid") or d.get("Video URL", "")
        d.update({
            "Competitor Name": ad["_name"],
            "Competitor Website": ad["_web"],
            "Category": ad["_cat"],
            "Region": ad["_region"],
            "Advertiser ID": ad["adv_id"],
            "Advertiser Name (Transparency Center)": ad["adv_name"],
            "Creative ID": ad["cid"],
            "Ad Format": ad["fmt"],
            "Image URL": new_img,
            "Video URL": new_vid,
            "Embed URL": new_embed,
            "Ad Preview URL": f"https://adstransparency.google.com/advertiser/{ad['adv_id']}/creative/{ad['cid']}",
            "Last Shown": TODAY,
            "Date Collected": TODAY,
            "Platform": "Google Ads",
            "Status": "Active",
        })
        if is_new:
            d["New This Week"] = "NEW"
            d["Started Running"] = d.get("Started Running") or TODAY
            new_google += 1
        else:
            d["New This Week"] = ""
            updated_google += 1
        existing_map[key] = d

    # ── Merge Meta Ads (incremental) ─────────────────────────────────
    new_meta = 0
    updated_meta = 0
    for ad in meta_ads:
        key = f"Meta Ads|{ad['Creative ID']}|{ad.get('Region', '')}"
        seen_this_run.add(key)
        is_new = key not in existing_map
        d = existing_map.get(key, {})

        # Preserve Local Image if it exists on disk; prefer new Image URL
        existing_local = d.get("Local Image", "")
        d.update(ad)
        if ad.get("Local Image"):
            d["Local Image"] = ad["Local Image"]
        elif existing_local:
            d["Local Image"] = existing_local

        d["Status"] = "Active"
        if is_new:
            d["New This Week"] = "NEW"
            new_meta += 1
        else:
            d["New This Week"] = ""
            updated_meta += 1
        existing_map[key] = d

    # ── Recompute Status for ads NOT seen this run ───────────────────
    today_date = datetime.date.fromisoformat(TODAY)
    inactive_count = 0
    still_active_count = 0
    for key, d in existing_map.items():
        if key in seen_this_run:
            continue
        # Not seen this run — recompute status from Last Shown
        last_shown = d.get("Last Shown", "")
        d["New This Week"] = ""
        if last_shown:
            try:
                ls_date = datetime.date.fromisoformat(last_shown.split(" ")[0])
                days_ago = (today_date - ls_date).days
                if days_ago <= 7:
                    d["Status"] = "Active"
                    still_active_count += 1
                else:
                    d["Status"] = "Inactive"
                    inactive_count += 1
            except (ValueError, TypeError):
                d["Status"] = "Inactive"
                inactive_count += 1
        else:
            d["Status"] = "Inactive"
            inactive_count += 1

    log.info(f"  Google Ads: {new_google} new, {updated_google} updated (re-seen)")
    log.info(f"  Meta Ads: {new_meta} new, {updated_meta} updated (re-seen)")
    log.info(f"  Not seen this run: {still_active_count} still active, {inactive_count} marked inactive")

    # Final list — nothing dropped, full history preserved
    all_data = list(existing_map.values())
    all_data.sort(key=lambda x: x.get("Last Shown", ""), reverse=True)

    total_google = sum(1 for d in all_data if d.get("Platform") == "Google Ads")
    total_meta = sum(1 for d in all_data if d.get("Platform") == "Meta Ads")
    total_active = sum(1 for d in all_data if d.get("Status") == "Active")
    log.info(f"  Total: {total_google} Google + {total_meta} Meta = {len(all_data)} ads ({total_active} active)")

    # Write files
    os.makedirs(PUBLIC_DIR, exist_ok=True)
    js_content = "const ADS_DATA = " + json.dumps(all_data, ensure_ascii=False) + ";"
    with open(ADS_JS_PATH, "w") as f:
        f.write(js_content)
    with open(os.path.join(SCRIPT_DIR, "ads_data.js"), "w") as f:
        f.write(js_content)
    with open(ADS_JSON_PATH, "w") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    log.info(f"  Dashboard files written")
    return all_data


# ═════════════════════════════════════════════════════════════════════════════
# STEP 5: DEPLOY TO VERCEL
# ═════════════════════════════════════════════════════════════════════════════

def deploy_vercel():
    """Step 6: Deploy to Vercel."""
    log.info("=" * 60)
    log.info("STEP 5: Deploy to Vercel")
    log.info("=" * 60)

    # Ensure node/npx are on PATH (needed when run from cron/LaunchAgent)
    env = os.environ.copy()
    extra_paths = [
        "/opt/homebrew/bin",
        "/usr/local/bin",
        os.path.expanduser("~/.nvm/versions/node/v22.20.0/bin"),
    ]
    env["PATH"] = ":".join(extra_paths) + ":" + env.get("PATH", "")

    try:
        result = subprocess.run(
            ["npx", "vercel", "--prod"],
            cwd=SCRIPT_DIR,
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
        )
        if result.returncode == 0:
            # Extract URL from output
            for line in result.stdout.split("\n"):
                if "tabby-ad-intelligence" in line and "vercel.app" in line:
                    log.info(f"  Deployed: {line.strip()}")
            log.info("  Deploy successful")
        else:
            log.warning(f"  Deploy failed: {result.stderr[:200]}")
    except Exception as e:
        log.warning(f"  Deploy error: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def main():
    start_time = time.time()
    log.info(f"{'=' * 60}")
    log.info(f"WEEKLY AD INTELLIGENCE PIPELINE — {BATCH_ID}")
    log.info(f"{'=' * 60}")

    # Step 1: Google Ads (via Apify; FireCrawl path kept as scrape_google_ads for fallback)
    google_ads = scrape_google_ads_apify()

    # Step 2: Filter Cash App
    google_ads = filter_cash_app_ads(google_ads)

    # Step 3: Meta Ads
    meta_ads = scrape_meta_ads()

    # Step 4: Merge & generate (writes public/ads_data.js)
    all_data = merge_and_generate(google_ads, meta_ads)

    # Step 5: Deploy is handled by `git push` → Vercel auto-deploy.
    # The legacy `npx vercel --prod` step uploaded the 1.6 GB gitignored
    # meta_images/ working tree and timed out at 180s. Removed.

    # Summary
    elapsed = time.time() - start_time
    log.info(f"{'=' * 60}")
    log.info(f"DONE in {elapsed:.0f}s — {len(all_data)} total ads")
    log.info(f"  Google Ads: {sum(1 for d in all_data if d.get('Platform') == 'Google Ads')}")
    log.info(f"  Meta Ads: {sum(1 for d in all_data if d.get('Platform') == 'Meta Ads')}")
    log.info(f"  Next: git add public/ads_data.js && git commit && git push")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
