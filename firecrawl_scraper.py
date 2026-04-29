#!/usr/bin/env python3
"""
Google Ads Transparency Center Scraper — FireCrawl Edition

Replaces the GoogleAds library with FireCrawl web scraping.
Budget: ~20 FireCrawl pages per weekly run (free plan: 500 pages total).

Usage:
    python firecrawl_scraper.py
"""

import json
import datetime
import os
import re
import logging
import time
import requests

from google.auth import default
import gspread

# ── Config ────────────────────────────────────────────────────────────────────
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
FIRECRAWL_URL = "https://api.firecrawl.dev/v2/scrape"

# Load from .env if not in environment
if not FIRECRAWL_API_KEY:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("FIRECRAWL_API_KEY="):
                    FIRECRAWL_API_KEY = line.split("=", 1)[1].strip()

INPUT_SHEET_ID = "1xP3ivxuECvba9Z9zahbPLsiSLSSpUt6DvTHClYkJ-Go"
OUTPUT_SHEET_NAME = "Google Ads Transparency - Competitor Ads"

# ── Competitors ───────────────────────────────────────────────────────────────
# Each entry: 1 FireCrawl page per advertiser_id per region.
# Competitors without known IDs are discovered via a lightweight API call (free).
COMPETITORS = [
    # ── GCC — Saudi Arabia ────────────────────────────────────────────────
    {
        "name": "Rajhi Bank", "region": "SA", "category": "GCC",
        "website": "https://www.alrajhibank.com.sa/",
        "search_terms": ["Al Rajhi Bank", "Al Rajhi Banking"],
        "advertiser_ids": [
            {"id": "AR07393135804576432129", "name": "Al Rajhi Banking & Investment Corp."},
            {"id": "AR17149597601662763009", "name": "Al Rajhi Banking and Investment Corporation"},
        ],
    },
    {
        "name": "STC Pay", "region": "SA", "category": "GCC",
        "website": "https://www.stcpay.com.sa/",
        "search_terms": ["stc pay", "STC", "Saudi Telecom"],
        "advertiser_ids": [],
    },
    {
        "name": "STCBank", "region": "SA", "category": "GCC",
        "website": "https://www.stcbank.com.sa/",
        "search_terms": ["stc bank", "STC Bank", "Saudi Telecom"],
        "advertiser_ids": [],
    },
    {
        "name": "Saudi National Bank", "region": "SA", "category": "GCC",
        "website": "https://www.snb.com/",
        "search_terms": ["Saudi National Bank", "The Saudi National Bank"],
        "advertiser_ids": [],  # TODO: add known ID once discovered correctly
    },
    {
        "name": "Tamara", "region": "SA", "category": "GCC",
        "website": "https://tamara.co/",
        "search_terms": ["Tamara", "Tamara Company", "Tamara Financing"],
        "advertiser_ids": [],
    },
    # ── GCC — UAE ─────────────────────────────────────────────────────────
    {
        "name": "EmiratesNBD", "region": "AE", "category": "GCC",
        "website": "https://www.emiratesnbd.com/",
        "search_terms": ["Emirates NBD"],
        "advertiser_ids": [
            {"id": "AR11606100870541869057", "name": "EMIRATES NBD (P.J.S.C)"},
        ],
    },
    {
        "name": "Liv (owned by EmiratesNBD)", "region": "AE", "category": "GCC",
        "website": "https://www.liv.me/",
        "search_terms": ["Emirates NBD", "Liv bank"],
        "advertiser_ids": [
            {"id": "AR11606100870541869057", "name": "EMIRATES NBD (P.J.S.C)"},
        ],
    },
    {
        "name": "Tamara", "region": "AE", "category": "GCC",
        "website": "https://tamara.co/",
        "search_terms": ["Tamara", "Tamara Company"],
        "advertiser_ids": [],
    },
    {
        "name": "Mamo", "region": "AE", "category": "GCC",
        "website": "https://www.mamopay.com/",
        "search_terms": ["Mamo Pay", "mamopay", "Mamo"],
        "advertiser_ids": [],
    },
    {
        "name": "Wio", "region": "AE", "category": "GCC",
        "website": "https://www.wio.io/",
        "search_terms": ["Wio Bank", "Wio bank PJSC"],
        "advertiser_ids": [],
    },
    {
        "name": "Revolut", "region": "AE", "category": "GCC",
        "website": "https://www.revolut.com/",
        "search_terms": ["Revolut"],
        "advertiser_ids": [],
    },
    # ── Global (no region filter) ─────────────────────────────────────────
    {
        "name": "Revolut", "region": "Global", "category": "Global",
        "website": "https://www.revolut.com/",
        "search_terms": ["Revolut"],
        "advertiser_ids": [
            {"id": "AR07098428377224183809", "name": "Revolut Ltd"},
        ],
    },
    {
        "name": "Monzo", "region": "Global", "category": "Global",
        "website": "https://monzo.com/",
        "search_terms": ["Monzo"],
        "advertiser_ids": [
            {"id": "AR07289389941828616193", "name": "MONZO BANK LIMITED"},
        ],
    },
    {
        "name": "Cash App", "region": "Global", "category": "Global",
        "website": "https://cash.app/",
        "search_terms": ["Cash App"],
        "advertiser_ids": [
            {"id": "AR14896030700992987137", "name": "Block, Inc."},
        ],
        "domain_filter": "cash.app",  # Filter by domain to exclude Square/BitKey
    },
    {
        "name": "Wise", "region": "Global", "category": "Global",
        "website": "https://wise.com/",
        "search_terms": ["Wise"],
        "advertiser_ids": [
            {"id": "AR14378710480124379137", "name": "Wise Payments Limited"},
        ],
    },
    {
        "name": "Klarna", "region": "Global", "category": "Global",
        "website": "https://www.klarna.com/",
        "search_terms": ["Klarna"],
        "advertiser_ids": [
            {"id": "AR03841049863391281153", "name": "Klarna AB"},
        ],
    },
]

# ── Output columns (same as scraper.py for dashboard compatibility) ───────────
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
    "Embed URL",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("firecrawl_scraper")


# ── Advertiser Discovery (lightweight, no FireCrawl pages used) ───────────────

_SEARCH_HEADERS = {
    "authority": "adstransparency.google.com",
    "accept": "application/json",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def _init_session(region: str) -> requests.Session:
    """Get a session with cookies from the Transparency Center."""
    session = requests.Session()
    session.headers.update(_SEARCH_HEADERS)
    params = {"region": region} if region not in ("Global", "anywhere") else {}
    session.get("https://adstransparency.google.com/", params=params, timeout=15)
    return session


def discover_advertiser_ids(name: str, search_terms: list, website: str, region: str) -> list[dict]:
    """
    Find advertiser IDs via the Transparency Center search API.
    Uses direct HTTP calls — no FireCrawl pages consumed.
    Returns list of {"id": ..., "name": ...} dicts.
    """
    api_region = "anywhere" if region == "Global" else region
    try:
        session = _init_session(api_region)
    except Exception as e:
        log.warning(f"  Could not init session for {region}: {e}")
        return []

    terms = list(dict.fromkeys(search_terms + [name]))
    seen_ids = set()
    results = []

    for term in terms:
        try:
            data = {"f.req": json.dumps({"1": term, "2": 10, "3": 10})}
            resp = session.post(
                "https://adstransparency.google.com/anji/_/rpc/SearchService/SearchSuggestions",
                params={"authuser": "0"},
                data=data,
                timeout=15,
            )
            suggestions = resp.json().get("1", [])
        except Exception:
            time.sleep(5)
            continue

        for s in suggestions:
            info = s.get("1", {})
            adv_name = info.get("1", "")
            adv_id = info.get("2", "")
            country = info.get("3", "")
            if not adv_id or adv_id in seen_ids:
                continue

            # Match by name similarity + country preference
            term_lower = term.lower()
            name_lower = adv_name.lower()
            name_match = term_lower in name_lower or name_lower.startswith(term_lower.split()[0])
            country_match = country in ("SA", "AE", region, "")

            if name_match and country_match:
                results.append({"id": adv_id, "name": adv_name})
                seen_ids.add(adv_id)

        if results:
            break
        time.sleep(3)

    # Fallback: search by domain
    if not results and website:
        try:
            domain = website.replace("https://", "").replace("http://", "").rstrip("/")
            data = {"f.req": json.dumps({"2": 40, "3": {"12": {"1": domain}}})}
            resp = session.post(
                "https://adstransparency.google.com/anji/_/rpc/SearchService/SearchCreatives",
                params={"authuser": ""},
                data=data,
                timeout=15,
            )
            ads = resp.json().get("1", [])
            if ads:
                adv_id = ads[0].get("1", "")
                adv_name = ads[0].get("12", "")
                if adv_id and adv_id not in seen_ids:
                    results.append({"id": adv_id, "name": adv_name})
        except Exception:
            pass

    return results


# ── FireCrawl Scraping ────────────────────────────────────────────────────────

def scrape_with_firecrawl(url: str) -> dict:
    """
    Scrape a single URL via FireCrawl. Costs 1 page.
    Returns the response data dict or None on failure.
    """
    if not FIRECRAWL_API_KEY:
        log.error("FIRECRAWL_API_KEY not set. Add it to .env")
        return None

    payload = {
        "url": url,
        "formats": ["html", "markdown"],
        "waitFor": 5000,
    }

    try:
        resp = requests.post(
            FIRECRAWL_URL,
            headers={
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=90,
        )
        result = resp.json()
    except Exception as e:
        log.error(f"FireCrawl request failed: {e}")
        return None

    if not result.get("success"):
        log.error(f"FireCrawl error for {url}: {result.get('error', result)}")
        return None

    return result.get("data", {})


def build_advertiser_url(advertiser_id: str, region: str, domain_filter: str = "", format_filter: str = "") -> str:
    """Build the Transparency Center URL for an advertiser.

    Always includes preset-date=Last+7+days to get fresh ads with thumbnails.
    """
    url = f"https://adstransparency.google.com/advertiser/{advertiser_id}"
    params = []
    if region and region not in ("Global", "anywhere"):
        params.append(f"region={region}")
    if domain_filter:
        params.append(f"domain={domain_filter}")
    if format_filter:
        params.append(f"format={format_filter}")
    params.append("preset-date=Last+7+days")
    if params:
        url += "?" + "&".join(params)
    return url


def parse_ads_from_scrape(scrape_data: dict, advertiser_id: str, advertiser_name: str) -> list[dict]:
    """
    Parse ad data from FireCrawl scrape response.

    The Transparency Center markdown renders ad cards in this pattern:
      - Image ad:  [![](simgad_url)](creative_link)
      - Video ad:  [![](ytimg_url)\n\n_videocam_](creative_link)
      - Text ad:   [Advertisement (N of M)](creative_link)  (no thumbnail)
    """
    markdown = scrape_data.get("markdown", "")
    html = scrape_data.get("html", "")
    content = markdown + "\n" + html

    # ── Extract unique creative IDs ──────────────────────────────────────
    creative_matches = re.findall(r"creative/(CR\d+)", content)
    creative_ids = list(dict.fromkeys(creative_matches))

    if not creative_ids:
        log.warning(f"  No creative IDs found in scrape for {advertiser_id}")
        debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_firecrawl")
        os.makedirs(debug_dir, exist_ok=True)
        with open(os.path.join(debug_dir, f"{advertiser_id}.json"), "w") as f:
            json.dump(scrape_data, f, indent=2, ensure_ascii=False)
        log.info(f"  Saved debug response to debug_firecrawl/{advertiser_id}.json")
        return []

    log.info(f"  Found {len(creative_ids)} unique creative IDs")

    # ── Parse ad cards from markdown ─────────────────────────────────────
    # Pattern 1: Image/Video with thumbnail
    #   [![](IMAGE_URL)\n\n_videocam_](CREATIVE_LINK)   ← video
    #   [![](IMAGE_URL)](CREATIVE_LINK)                  ← image
    # Pattern 2: Text ad (no thumbnail)
    #   [Advertisement (N of M)](CREATIVE_LINK)
    card_pattern = re.compile(
        r'\[!\[\]\(([^)]+)\)'       # ![](IMAGE_URL)
        r'(.*?)'                     # optional content between (e.g. \n\n_videocam_)
        r'\]\('                      # ](
        r'[^)]*creative/(CR\d+)'    # creative link with ID
        r'[^)]*\)',                  # rest of URL + closing )
        re.DOTALL,
    )

    creative_image_map = {}
    creative_format_map = {}

    for match in card_pattern.finditer(markdown):
        img_url = match.group(1)
        between = match.group(2)
        cid = match.group(3)

        # Normalize image URL
        if img_url.startswith("//"):
            img_url = "https:" + img_url

        # Only keep Google-hosted ad images
        if any(d in img_url for d in [
            "googlesyndication.com", "googleusercontent.com",
            "ytimg.com", "gstatic.com",
        ]):
            creative_image_map[cid] = img_url

        # Format: _videocam_ marker = video, otherwise image
        if "_videocam_" in between:
            creative_format_map[cid] = "Video"
        elif cid not in creative_format_map:
            creative_format_map[cid] = "Image"

    # Text ads: cards without thumbnails
    text_pattern = re.compile(
        r'\[Advertisement \(\d+ of \d+\)\]'
        r'\([^)]*creative/(CR\d+)[^)]*\)'
    )
    for match in text_pattern.finditer(markdown):
        cid = match.group(1)
        if cid not in creative_format_map:
            creative_format_map[cid] = "Text"
        # Text ads have no thumbnail — mark explicitly so we don't override
        if cid not in creative_image_map:
            creative_image_map[cid] = ""

    # ── Fallback: associate images via HTML proximity ────────────────────
    for cid in creative_ids:
        if cid in creative_image_map:
            continue
        pattern = rf'(?s)(.{{0,2000}}creative/{re.escape(cid)}.{{0,500}})'
        match = re.search(pattern, html)
        if not match:
            continue
        context = match.group(1)
        imgs = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', context)
        for img in imgs:
            if img.startswith("//"):
                img = "https:" + img
            if any(d in img for d in ["googlesyndication", "googleusercontent", "ytimg", "gstatic"]):
                creative_image_map[cid] = img
                break

    # ── Build ad records (skip Text ads — they have no visual value) ────
    ads = []
    for cid in creative_ids:
        image_url = creative_image_map.get(cid, "")
        ad_format = creative_format_map.get(cid, "")

        # Infer format from image URL if not detected
        if not ad_format and image_url:
            if "ytimg" in image_url:
                ad_format = "Video"
            else:
                ad_format = "Image"

        # Skip text ads — no visual preview value
        if ad_format == "Text":
            continue

        ads.append({
            "creative_id": cid,
            "advertiser_id": advertiser_id,
            "advertiser_name": advertiser_name,
            "ad_format": ad_format,
            "last_shown": "",
            "image_url": image_url,
            "video_url": "",
            "landing_page": "",
            "preview_url": f"https://adstransparency.google.com/advertiser/{advertiser_id}/creative/{cid}",
            "embed_url": "",
        })

    return ads


# ── Google Sheets & Dashboard Output ──────────────────────────────────────────

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


def load_existing_creative_ids(ws) -> set:
    """Load existing creative IDs from output sheet for dedup."""
    try:
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return set()
        cid_idx = HEADERS.index("Creative ID")
        reg_idx = HEADERS.index("Region")
        return {
            f"{row[cid_idx]}_{row[reg_idx]}"
            for row in all_values[1:]
            if len(row) > cid_idx and row[cid_idx]
        }
    except Exception:
        return set()


def write_results(gc, all_rows: list[list], batch_id: str) -> str:
    """Write results to Google Sheet. Returns sheet URL."""
    sh = get_or_create_output_sheet(gc)

    try:
        ws = sh.worksheet("Ads Data")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Ads Data", rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")

    existing_ids = load_existing_creative_ids(ws)
    log.info(f"Existing ads in sheet: {len(existing_ids)}")

    cid_idx = HEADERS.index("Creative ID")
    reg_idx = HEADERS.index("Region")
    new_rows = []
    skipped = 0
    for row in all_rows:
        key = f"{row[cid_idx]}_{row[reg_idx]}"
        if key in existing_ids:
            skipped += 1
            continue
        new_rows.append(row)

    log.info(f"New ads to add: {len(new_rows)}, duplicates skipped: {skipped}")

    if not new_rows:
        log.info("No new ads to add.")
        return sh.url

    BATCH_SIZE = 50
    for i in range(0, len(new_rows), BATCH_SIZE):
        batch = new_rows[i : i + BATCH_SIZE]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        log.info(f"  Written {min(i + BATCH_SIZE, len(new_rows))}/{len(new_rows)} rows")
        time.sleep(1)

    log.info(f"Output sheet URL: {sh.url}")
    return sh.url


def _clean_row_for_dashboard(d: dict) -> dict:
    """Clean a single row dict for dashboard display."""
    img = d.get("Image URL", "")
    if "<img" in img:
        m = re.search(r'src=["\']([^"\'> ]+)', img)
        img = m.group(1) if m else ""
    if "sadbundle" in img or img.endswith(".html") or img.endswith(".js"):
        img = ""
    d["Image URL"] = img

    vid = d.get("Video URL", "")
    if vid:
        vid = vid.replace("&amp;", "&")
        if vid.startswith("//"):
            vid = "https:" + vid
    d["Video URL"] = vid

    if "Platform" not in d:
        d["Platform"] = "Google Ads"

    # Status: Active if Last Shown is within 30 days
    last_shown = d.get("Last Shown", "")
    if last_shown:
        try:
            ls_date = datetime.datetime.strptime(last_shown, "%Y-%m-%d").date()
            days_ago = (datetime.date.today() - ls_date).days
            d["Status"] = "Active" if days_ago <= 30 else "Inactive"
        except (ValueError, TypeError):
            d["Status"] = "Active"
    else:
        d["Status"] = "Active"

    return d


def generate_dashboard(all_rows: list[list]):
    """Generate dashboard data files, merging with existing data."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    public_dir = os.path.join(script_dir, "public")
    js_path = os.path.join(public_dir, "ads_data.js")

    # Load existing dashboard data
    existing_data = []
    if os.path.exists(js_path):
        try:
            with open(js_path) as f:
                raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
            existing_data = json.loads(raw)
            log.info(f"Loaded {len(existing_data)} existing ads from dashboard")
        except Exception as e:
            log.warning(f"Could not load existing dashboard data: {e}")

    # Build lookup by composite key
    existing_map = {}
    for d in existing_data:
        key = f"{d.get('Creative ID', '')}_{d.get('Region', '')}"
        existing_map[key] = d

    # Merge new rows
    new_count = 0
    updated_count = 0
    for row in all_rows:
        d = {h: (row[i] if i < len(row) else "") for i, h in enumerate(HEADERS)}
        d = _clean_row_for_dashboard(d)

        key = f"{d.get('Creative ID', '')}_{d.get('Region', '')}"
        if key in existing_map:
            old = existing_map[key]
            # Preserve media URLs if new scrape didn't find them
            for field in ("Image URL", "Video URL", "Embed URL", "Landing Page / Destination URL"):
                if not d.get(field) and old.get(field):
                    d[field] = old[field]
            existing_map[key] = d
            updated_count += 1
        else:
            existing_map[key] = d
            new_count += 1

    # Re-compute status for all — and drop Google Ads without any preview image
    data = []
    dropped = 0
    for d in existing_map.values():
        d = _clean_row_for_dashboard(d)
        platform = d.get("Platform", "Google Ads")
        has_preview = bool(d.get("Image URL") or d.get("Local Image") or d.get("Screenshot") or d.get("Embed URL"))
        if platform == "Google Ads" and not has_preview:
            dropped += 1
            continue
        data.append(d)
    if dropped:
        log.info(f"Dropped {dropped} Google Ads without preview images")
    data.sort(key=lambda x: x.get("Last Shown", ""), reverse=True)

    # Write JS data files
    js_content = "const ADS_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
    os.makedirs(public_dir, exist_ok=True)
    with open(js_path, "w") as f:
        f.write(js_content)
    with open(os.path.join(script_dir, "ads_data.js"), "w") as f:
        f.write(js_content)
    with open(os.path.join(script_dir, "ads_data.json"), "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    log.info(f"Dashboard: {len(data)} total ads ({new_count} new, {updated_count} updated)")


# ── Main Scraper Flow ─────────────────────────────────────────────────────────

def scrape_competitor(competitor: dict, batch_id: str) -> list[list]:
    """Scrape all ads for a single competitor. Returns list of row data."""
    name = competitor["name"]
    website = competitor["website"]
    region = competitor["region"]
    category = competitor["category"]
    today = datetime.date.today().isoformat()

    log.info(f"Processing '{name}' ({region})...")

    # ── Step 1: Get advertiser IDs ───────────────────────────────────────
    adv_list = competitor.get("advertiser_ids", [])
    if not adv_list:
        log.info(f"  No known IDs — discovering via search API...")
        adv_list = discover_advertiser_ids(
            name, competitor.get("search_terms", [name]), website, region,
        )
        if adv_list:
            log.info(f"  Discovered: {[a['name'] for a in adv_list]}")
        else:
            log.warning(f"  Could not find advertiser for '{name}' in {region}")
            return []

    # ── Step 2: Scrape each advertiser's page via FireCrawl ──────────────
    domain_filter = competitor.get("domain_filter", "")
    rows = []
    for adv in adv_list:
        adv_id = adv["id"]
        adv_name = adv["name"]

        # Scrape default page (IMAGE ads mostly)
        url = build_advertiser_url(adv_id, region, domain_filter=domain_filter)
        log.info(f"  Scraping {adv_name} ({adv_id}) — {url}")
        scrape_data = scrape_with_firecrawl(url)
        if not scrape_data:
            log.warning(f"  FireCrawl scrape failed for {adv_id}")
            continue

        ads = parse_ads_from_scrape(scrape_data, adv_id, adv_name)
        log.info(f"  Parsed {len(ads)} ads from default page")

        # Also scrape VIDEO format separately (video thumbnails often missing from default)
        time.sleep(3)
        url_video = build_advertiser_url(adv_id, region, domain_filter=domain_filter, format_filter="VIDEO")
        log.info(f"  Scraping VIDEO page — {url_video}")
        scrape_video = scrape_with_firecrawl(url_video)
        if scrape_video:
            video_ads = parse_ads_from_scrape(scrape_video, adv_id, adv_name)
            # Merge: add video ads not already found
            existing_cids = {a["creative_id"] for a in ads}
            new_video = [a for a in video_ads if a["creative_id"] not in existing_cids]
            # Also update format for any that were detected as Image but are actually Video
            video_cids = {a["creative_id"] for a in video_ads}
            for a in ads:
                if a["creative_id"] in video_cids and a.get("ad_format") != "Video":
                    a["ad_format"] = "Video"
            ads.extend(new_video)
            log.info(f"  +{len(new_video)} video ads from VIDEO page")

        log.info(f"  Total: {len(ads)} ads from {adv_name}")

        for ad in ads:
            image_url = ad.get("image_url", "")
            video_url = ad.get("video_url", "")
            image_preview = f'=IMAGE("{image_url}")' if image_url else ""

            row = [
                name,                                  # Competitor Name
                website,                               # Competitor Website
                category,                              # Category
                region,                                 # Region
                adv_id,                                 # Advertiser ID
                adv_name,                               # Advertiser Name
                ad["creative_id"],                      # Creative ID
                ad.get("ad_format", ""),                # Ad Format
                ad.get("last_shown", ""),               # Last Shown
                ad["preview_url"],                      # Ad Preview URL
                ad.get("landing_page", ""),             # Landing Page
                image_url,                              # Image URL
                image_preview,                          # Image Preview
                video_url,                              # Video URL
                video_url,                              # Video Preview
                today,                                  # Date Collected
                "",                                     # New This Week
                batch_id,                               # Scrape Batch ID
                "Google Ads",                           # Platform
                ad.get("embed_url", ""),                # Embed URL
            ]
            rows.append(row)

        # Respect FireCrawl rate limits (free plan: 2 concurrent, low rate)
        time.sleep(3)

    return rows


def main():
    batch_id = f"batch_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    log.info(f"=== FireCrawl Google Ads Scraper — {batch_id} ===")
    log.info(f"Competitors: {len(COMPETITORS)}")

    # Count estimated FireCrawl pages
    total_ids = sum(len(c.get("advertiser_ids", [])) or 1 for c in COMPETITORS)
    log.info(f"Estimated FireCrawl pages this run: ~{total_ids}")

    # Try to authenticate for Google Sheets (optional — dashboard is primary output)
    gc = None
    try:
        gc = get_gspread_client()
    except Exception as e:
        log.warning(f"Google Sheets auth failed (will skip sheet output): {e}")

    # Deduplicate advertiser IDs across competitors (e.g. EmiratesNBD / Liv share an ID)
    scraped_adv_ids = {}  # adv_id+region → list of ads
    all_rows = []

    for comp in COMPETITORS:
        region = comp["region"]
        adv_list = comp.get("advertiser_ids", [])

        # Check if we already scraped this advertiser for this region
        needs_scrape = False
        for adv in adv_list:
            key = f"{adv['id']}_{region}"
            if key not in scraped_adv_ids:
                needs_scrape = True
                break

        if not needs_scrape and adv_list:
            # Reuse previously scraped data but relabel for this competitor
            log.info(f"Reusing scraped data for '{comp['name']}' ({region})")
            for adv in adv_list:
                key = f"{adv['id']}_{region}"
                cached_ads = scraped_adv_ids.get(key, [])
                for ad in cached_ads:
                    row = list(ad)  # copy
                    row[0] = comp["name"]
                    row[1] = comp["website"]
                    row[2] = comp["category"]
                    all_rows.append(row)
            continue

        rows = scrape_competitor(comp, batch_id)
        all_rows.extend(rows)

        # Cache the scraped ads by advertiser ID + region
        for adv in (adv_list or []):
            key = f"{adv['id']}_{region}"
            scraped_adv_ids[key] = [r for r in rows if r[4] == adv["id"]]

        log.info(f"  → {len(rows)} ads for {comp['name']} ({region})")

    log.info(f"\n=== Total ads collected: {len(all_rows)} ===")

    if all_rows:
        generate_dashboard(all_rows)
        if gc:
            try:
                sheet_url = write_results(gc, all_rows, batch_id)
                log.info(f"Sheet: {sheet_url}")
            except Exception as e:
                log.warning(f"Google Sheets write failed: {e}")
        log.info("Done! Dashboard data updated.")
    else:
        log.warning("No ads found. Check debug_firecrawl/ for raw responses.")

    return all_rows


if __name__ == "__main__":
    main()
