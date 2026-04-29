#!/usr/bin/env python3
"""
Meta Ads Library Scraper
Scrapes competitor ads from Meta (Facebook/Instagram) Ad Library using Apify.
Reads page IDs from a local config, calls the Apify actor, and exports results
to the same Google Sheet + dashboard used by the Google Ads scraper.
"""

import json
import datetime
import time
import logging
import os
import re
from typing import Optional
import requests

from google.auth import default
import gspread

# ── Config ────────────────────────────────────────────────────────────────────
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
APIFY_ACTOR = "curious_coder/facebook-ads-library-scraper"
OUTPUT_SHEET_NAME = "Google Ads Transparency - Competitor Ads"

# Apify API base
APIFY_API = "https://api.apify.com/v2"

# Poll interval when waiting for actor run to finish
POLL_INTERVAL = 15  # seconds
MAX_POLL_TIME = 600  # 10 minutes max wait

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("meta_scraper")

# ── Competitor page mapping ───────────────────────────────────────────────────
# Facebook page_id → (display_name, category, website)
PAGE_MAP = {
    "888799511134149": ("Cash App", "Global", "https://cash.app/"),
    "107593894218382": ("Tamara", "GCC", "https://www.tamara.co/"),
    "105245002169048": ("Tiqmo", "GCC", "https://tiqmo.com/"),
    "100238958486269": ("D360 Bank", "GCC", "https://www.d360.bank/"),
    "370543246139130": ("Barq", "GCC", "https://usebarq.com/"),
    "102791935482897": ("Wio Bank", "GCC", "https://wio.io/"),
    "141270813154032": ("STC Bank", "GCC", "https://www.stcbank.com.sa/"),
    "379823329174805": ("HALA Payment", "GCC", "https://hala.com/"),
    "102701872367080": ("Alaan", "GCC", "https://www.alaan.com/"),
    "390926061079580": ("Klarna", "Global", "https://www.klarna.com/"),
    "113612035651775": ("Monzo", "Global", "https://monzo.com/"),
    "116206531782887": ("Wise", "Global", "https://wise.com/"),
    "335642513253333": ("Revolut", "Global", "https://www.revolut.com/"),
}

# Input URLs for the Apify actor
SCRAPER_INPUT = {
    "scrapeAdDetails": True,
    "scrapePageAds.activeStatus": "all",
    "urls": [
        {"url": f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type={'video' if pid == '888799511134149' else 'all'}&search_type=page&view_all_page_id={pid}", "method": "GET"}
        for pid in PAGE_MAP
    ],
}

# ── Output columns (same as Google Ads scraper + Platform) ────────────────────
HEADERS = [
    "Competitor Name",
    "Competitor Website",
    "Category",
    "Region",
    "Advertiser ID",
    "Advertiser Name (Transparency Center)",
    "Creative ID",
    "Ad Format",
    "Last Shown",
    "Ad Preview URL",
    "Landing Page / Destination URL",
    "Image URL",
    "Image Preview",
    "Video URL",
    "Video Preview",
    "Date Collected",
    "New This Week",
    "Scrape Batch ID",
    "Platform",
    "Started Running",
]


# ── Apify API helpers ─────────────────────────────────────────────────────────

def run_actor(token: str, actor_input: dict) -> str:
    """Start the Apify actor and return the run ID."""
    actor_slug = APIFY_ACTOR.replace("/", "~")
    url = f"{APIFY_API}/acts/{actor_slug}/runs"
    resp = requests.post(
        url,
        params={"token": token},
        json=actor_input,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    run_id = data["id"]
    log.info(f"Actor run started: {run_id}")
    return run_id


def wait_for_run(token: str, run_id: str) -> dict:
    """Poll until the actor run finishes. Returns run data."""
    url = f"{APIFY_API}/actor-runs/{run_id}"
    elapsed = 0
    while elapsed < MAX_POLL_TIME:
        resp = requests.get(url, params={"token": token}, timeout=15)
        resp.raise_for_status()
        data = resp.json()["data"]
        status = data["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            log.info(f"Actor run {status} (took ~{elapsed}s)")
            return data
        log.info(f"  Run status: {status} ({elapsed}s elapsed)...")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    raise TimeoutError(f"Actor run did not finish within {MAX_POLL_TIME}s")


def fetch_dataset(token: str, dataset_id: str) -> list[dict]:
    """Fetch all items from an Apify dataset."""
    items = []
    offset = 0
    limit = 100
    while True:
        url = f"{APIFY_API}/datasets/{dataset_id}/items"
        resp = requests.get(
            url,
            params={"token": token, "offset": offset, "limit": limit, "format": "json"},
            timeout=30,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    log.info(f"Fetched {len(items)} items from dataset {dataset_id}")
    return items


# ── Transform Meta ad data to our row format ──────────────────────────────────

def _detect_format(item: dict) -> str:
    """Detect ad format from Meta ad item."""
    snapshot = item.get("snapshot", {})
    # Check for video content
    videos = snapshot.get("videos", [])
    if videos:
        return "Video"
    cards = snapshot.get("cards", [])
    for card in cards:
        if card.get("video_hd_url") or card.get("video_sd_url"):
            return "Video"
    # Check for image content
    images = snapshot.get("images", [])
    if images:
        return "Image"
    for card in cards:
        if card.get("original_image_url") or card.get("resized_image_url"):
            return "Image"
    # Check display_format hint
    display_fmt = snapshot.get("display_format", "")
    if "VIDEO" in display_fmt.upper():
        return "Video"
    if "IMAGE" in display_fmt.upper() or "CAROUSEL" in display_fmt.upper():
        return "Image"
    # Default: if there's body text, it's at minimum a text ad
    return "Image" if cards else "Text"


def _extract_image_url(item: dict) -> str:
    """Extract the best image URL from a Meta ad item."""
    snapshot = item.get("snapshot", {})
    # From images array
    for img in snapshot.get("images", []):
        if isinstance(img, dict):
            url = img.get("original_image_url") or img.get("resized_image_url", "")
            if url:
                return url
        elif isinstance(img, str):
            return img
    # From cards
    for card in snapshot.get("cards", []):
        url = card.get("original_image_url") or card.get("resized_image_url", "")
        if url:
            return url
        # Video preview image as fallback for video ads
        url = card.get("video_preview_image_url", "")
        if url:
            return url
    # From videos (preview image)
    for vid in snapshot.get("videos", []):
        url = vid.get("video_preview_image_url", "")
        if url:
            return url
    return ""


def _extract_video_url(item: dict) -> str:
    """Extract the best video URL from a Meta ad item."""
    snapshot = item.get("snapshot", {})
    # From videos array
    for vid in snapshot.get("videos", []):
        url = vid.get("video_hd_url") or vid.get("video_sd_url", "")
        if url:
            return url
    # From cards
    for card in snapshot.get("cards", []):
        url = card.get("video_hd_url") or card.get("video_sd_url", "")
        if url:
            return url
    return ""


def _extract_landing_page(item: dict) -> str:
    """Extract landing page URL from a Meta ad item."""
    snapshot = item.get("snapshot", {})
    link = snapshot.get("link_url", "")
    if link:
        return link
    for card in snapshot.get("cards", []):
        link = card.get("link_url", "")
        if link:
            return link
    return ""


def _extract_ad_text(item: dict) -> str:
    """Extract ad body text."""
    snapshot = item.get("snapshot", {})
    body = snapshot.get("body", {})
    if isinstance(body, dict):
        return body.get("text", "")
    if isinstance(body, str):
        return body
    return ""


def transform_item(item: dict, batch_id: str, today: str) -> Optional[list]:
    """Transform a single Apify result item into our standard row format."""
    page_id = str(item.get("page_id", ""))
    if page_id not in PAGE_MAP:
        return None

    name, category, website = PAGE_MAP[page_id]

    ad_format = _detect_format(item)
    image_url = _extract_image_url(item)
    video_url = _extract_video_url(item)
    landing_page = _extract_landing_page(item)
    ad_library_url = item.get("ad_library_url", "")
    creative_id = str(item.get("ad_archive_id", ""))

    # Dates
    last_shown = ""
    started_running = ""
    end_fmt = item.get("end_date_formatted", "")
    start_fmt = item.get("start_date_formatted", "")
    if end_fmt:
        last_shown = end_fmt.split(" ")[0]  # "2026-03-22 07:00:00" → "2026-03-22"
    elif start_fmt:
        last_shown = start_fmt.split(" ")[0]
    if start_fmt:
        started_running = start_fmt.split(" ")[0]

    # Publisher platforms
    platforms = item.get("publisher_platform", [])
    region_hint = ", ".join(platforms) if platforms else "Meta"

    # Page name from the API (advertiser name equivalent)
    page_name = item.get("page_name", "") or item.get("snapshot", {}).get("page_name", "")

    image_preview = f'=IMAGE("{image_url}")' if image_url else ""

    row = [
        name,                   # Competitor Name
        website,                # Competitor Website
        category,               # Category (GCC / Global)
        region_hint,            # Region (publisher platforms)
        page_id,                # Advertiser ID (page_id)
        page_name,              # Advertiser Name
        creative_id,            # Creative ID (ad_archive_id)
        ad_format,              # Ad Format
        last_shown,             # Last Shown
        ad_library_url,         # Ad Preview URL
        landing_page,           # Landing Page
        image_url,              # Image URL
        image_preview,          # Image Preview
        video_url,              # Video URL
        video_url,              # Video Preview
        today,                  # Date Collected
        "",                     # New This Week (filled later)
        batch_id,               # Scrape Batch ID
        "Meta Ads",             # Platform
        started_running,        # Started Running
    ]
    return row


def _download_images_for_rows(all_rows: list[list]):
    """Download images locally from fbcdn URLs before they expire."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    images_dir = os.path.join(script_dir, "public", "meta_images")
    os.makedirs(images_dir, exist_ok=True)

    img_idx = HEADERS.index("Image URL")
    cid_idx = HEADERS.index("Creative ID")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    downloaded = 0
    for row in all_rows:
        url = row[img_idx] if len(row) > img_idx else ""
        cid = row[cid_idx] if len(row) > cid_idx else ""
        if not url or not cid:
            continue

        ext = "png" if ".png" in url.lower() else "jpg"
        filename = f"{cid}.{ext}"
        filepath = os.path.join(images_dir, filename)

        # Skip if already downloaded
        if os.path.exists(filepath) and os.path.getsize(filepath) > 500:
            downloaded += 1
            continue

        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 200 and len(resp.content) > 500:
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                downloaded += 1
            time.sleep(0.2)
        except Exception:
            pass

    log.info(f"  Downloaded {downloaded}/{len(all_rows)} images to {images_dir}")


# ── Google Sheet output ───────────────────────────────────────────────────────

def get_gspread_client():
    """Authenticate and return gspread client."""
    creds, _ = default(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
    )
    return gspread.authorize(creds)


def get_or_create_output_sheet(gc) -> gspread.Spreadsheet:
    """Get existing output sheet or create a new one."""
    try:
        sh = gc.open(OUTPUT_SHEET_NAME)
        return sh
    except gspread.SpreadsheetNotFound:
        sh = gc.create(OUTPUT_SHEET_NAME)
        sh.share("", perm_type="anyone", role="writer")
        return sh


def write_results(gc, all_rows: list[list], batch_id: str):
    """Write results to the output Google Sheet (Meta Ads Data tab)."""
    sh = get_or_create_output_sheet(gc)

    tab_name = "Meta Ads Data"
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")

    # Load existing creative IDs for dedup
    existing_ids = set()
    try:
        all_values = ws.get_all_values()
        if len(all_values) > 1:
            cid_idx = HEADERS.index("Creative ID")
            existing_ids = {row[cid_idx] for row in all_values[1:] if len(row) > cid_idx and row[cid_idx]}
    except Exception:
        pass

    # Filter duplicates
    cid_idx = HEADERS.index("Creative ID")
    new_rows = [row for row in all_rows if row[cid_idx] not in existing_ids]
    skipped = len(all_rows) - len(new_rows)
    log.info(f"New ads to add: {len(new_rows)}, duplicates skipped: {skipped}")

    if not new_rows:
        log.info("No new ads to add.")
        return sh.url

    # Append in batches
    BATCH_SIZE = 50
    for i in range(0, len(new_rows), BATCH_SIZE):
        batch = new_rows[i : i + BATCH_SIZE]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        log.info(f"  Written {min(i + BATCH_SIZE, len(new_rows))}/{len(new_rows)} rows")
        time.sleep(1)

    log.info(f"Output sheet URL: {sh.url}")
    return sh.url


# ── Dashboard generation ──────────────────────────────────────────────────────

def generate_dashboard(meta_rows: list[list]):
    """
    Merge Meta ads with existing Google Ads data and regenerate ads_data.js.
    Adds a 'Platform' field to all entries.

    IMPORTANT: Load from public/ads_data.js (the deployed source of truth),
    NOT from ads_data.json. The JS file is what Vercel serves and what the
    dashboard reads. Loading from the stale JSON file caused us to lose
    Local Video mappings and ytimg cleanups.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Load existing data from the SINGLE SOURCE OF TRUTH: public/ads_data.js
    existing_data = []
    public_js_path = os.path.join(script_dir, "public", "ads_data.js")
    json_path = os.path.join(script_dir, "ads_data.json")

    if os.path.exists(public_js_path):
        try:
            with open(public_js_path, "r") as f:
                raw = f.read()
            start = raw.index("[")
            existing_data = json.loads(raw[start : raw.rindex("]") + 1])
            log.info(f"Loaded {len(existing_data)} existing ads from public/ads_data.js")
        except Exception as e:
            log.warning(f"Could not parse public/ads_data.js: {e}")
            # Fallback to ads_data.json only if .js fails
            if os.path.exists(json_path):
                with open(json_path, "r") as f:
                    existing_data = json.load(f)
    elif os.path.exists(json_path):
        with open(json_path, "r") as f:
            existing_data = json.load(f)

    # Ensure all existing entries have Platform field
    for d in existing_data:
        if "Platform" not in d:
            d["Platform"] = "Google Ads"

    # Convert meta rows to dicts + add local image paths
    images_dir = os.path.join(script_dir, "public", "meta_images")
    meta_new_items = []
    for row in meta_rows:
        d = {}
        for i, h in enumerate(HEADERS):
            d[h] = row[i] if i < len(row) else ""
        # Check if local image exists
        cid = d.get("Creative ID", "")
        if cid:
            for ext in ("jpg", "png"):
                local_path = os.path.join(images_dir, f"{cid}.{ext}")
                if os.path.exists(local_path):
                    d["Local Image"] = f"/meta_images/{cid}.{ext}"
                    break
        meta_new_items.append(d)

    # ── TRUE INCREMENTAL MERGE ─────────────────────────────────────
    # Preserve ALL historical Meta ads. Update the ones seen this run.
    # Mark the ones not seen as Inactive. Never delete anything.
    today = datetime.date.today().isoformat()
    today_date = datetime.date.today()

    # Index existing ads by Platform|Creative ID|Region
    def make_key(ad):
        platform = ad.get("Platform", "Google Ads")
        cid = ad.get("Creative ID", "")
        region = ad.get("Region", "")
        return f"{platform}|{cid}|{region}"

    existing_map = {make_key(d): d for d in existing_data}
    seen_meta_keys = set()

    new_count = 0
    updated_count = 0
    for d in meta_new_items:
        key = make_key(d)
        seen_meta_keys.add(key)
        if key in existing_map:
            # Merge: update everything but preserve Local Image if disk still has it
            old = existing_map[key]
            preserved_local = old.get("Local Image", "")
            existing_map[key].update(d)
            if not d.get("Local Image") and preserved_local:
                existing_map[key]["Local Image"] = preserved_local
            existing_map[key]["Status"] = "Active"
            existing_map[key]["Last Shown"] = d.get("Last Shown") or today
            existing_map[key]["New This Week"] = ""
            updated_count += 1
        else:
            d["Status"] = "Active"
            d["New This Week"] = "NEW"
            if not d.get("Last Shown"):
                d["Last Shown"] = today
            existing_map[key] = d
            new_count += 1

    # Mark old Meta ads (not seen this run) as Inactive — never delete
    inactive_count = 0
    for key, d in existing_map.items():
        if d.get("Platform") != "Meta Ads":
            continue
        if key in seen_meta_keys:
            continue
        d["New This Week"] = ""
        # Compute status from Last Shown
        last_shown = d.get("Last Shown", "")
        if last_shown:
            try:
                ls = datetime.date.fromisoformat(str(last_shown)[:10])
                days = (today_date - ls).days
                d["Status"] = "Active" if days <= 7 else "Inactive"
            except (ValueError, TypeError):
                d["Status"] = "Inactive"
        else:
            d["Status"] = "Inactive"
        if d["Status"] == "Inactive":
            inactive_count += 1

    # Also ensure Google Ads Status is sane (set if missing)
    for d in existing_map.values():
        if d.get("Platform") == "Google Ads" and not d.get("Status"):
            last_shown = d.get("Last Shown", "")
            if last_shown:
                try:
                    ls = datetime.date.fromisoformat(str(last_shown)[:10])
                    days = (today_date - ls).days
                    d["Status"] = "Active" if days <= 7 else "Inactive"
                except (ValueError, TypeError):
                    d["Status"] = "Inactive"
            else:
                d["Status"] = "Inactive"

    all_data = list(existing_map.values())
    all_data.sort(key=lambda x: str(x.get("Last Shown", "")), reverse=True)

    total_google = sum(1 for d in all_data if d.get("Platform") == "Google Ads")
    total_meta = sum(1 for d in all_data if d.get("Platform") == "Meta Ads")

    # Write JS data file (root copy — legacy)
    js_path = os.path.join(script_dir, "ads_data.js")
    with open(js_path, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(all_data, f, ensure_ascii=False)
        f.write(";")

    # Write JSON
    with open(json_path, "w") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    # public/ads_data.js — the single source of truth
    public_js = os.path.join(script_dir, "public", "ads_data.js")
    os.makedirs(os.path.dirname(public_js), exist_ok=True)
    with open(public_js, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(all_data, f, ensure_ascii=False)
        f.write(";")

    log.info(
        f"Meta merge: {new_count} new, {updated_count} updated, {inactive_count} marked Inactive"
    )
    log.info(
        f"Dashboard written: {total_google} Google + {total_meta} Meta = {len(all_data)} total"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    batch_id = f"meta_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    today = datetime.date.today().isoformat()
    log.info(f"=== Meta Ads Library Scraper — {batch_id} ===")

    if not APIFY_TOKEN:
        log.error("APIFY_TOKEN environment variable is not set.")
        log.error("Set it with: export APIFY_TOKEN='your_token_here'")
        return []

    # Step 1: Run the Apify actor
    log.info(f"Starting Apify actor: {APIFY_ACTOR}")
    log.info(f"Scraping {len(PAGE_MAP)} competitor pages...")
    run_id = run_actor(APIFY_TOKEN, SCRAPER_INPUT)

    # Step 2: Wait for completion
    run_data = wait_for_run(APIFY_TOKEN, run_id)
    if run_data["status"] != "SUCCEEDED":
        log.error(f"Actor run failed with status: {run_data['status']}")
        return []

    # Step 3: Fetch results
    dataset_id = run_data["defaultDatasetId"]
    items = fetch_dataset(APIFY_TOKEN, dataset_id)
    log.info(f"Total items from Apify: {len(items)}")

    # Step 4: Transform to our format + download images locally
    all_rows = []
    for item in items:
        row = transform_item(item, batch_id, today)
        if row:
            all_rows.append(row)

    log.info(f"Transformed {len(all_rows)} ads from {len(PAGE_MAP)} pages")

    # Step 4.5: Download images locally (fbcdn URLs expire!)
    log.info("Downloading ad images locally...")
    _download_images_for_rows(all_rows)

    # Step 5: Write to Google Sheet
    try:
        gc = get_gspread_client()
        sheet_url = write_results(gc, all_rows, batch_id)
        log.info(f"Sheet: {sheet_url}")
    except Exception as e:
        log.warning(f"Could not write to Google Sheet: {e}")
        log.warning("Continuing with dashboard generation...")

    # Step 6: Generate dashboard data
    generate_dashboard(all_rows)

    log.info(f"\n=== Done! {len(all_rows)} Meta ads collected ===")
    return all_rows


if __name__ == "__main__":
    main()
