#!/usr/bin/env python3
"""
Google Ads Transparency Center Scraper Agent
Scrapes competitor ads for SA (Saudi Arabia) and AE (UAE) regions.
Reads competitors from a Google Sheet and exports results back to a Google Sheet.
Designed to run once per week.
"""

import json
import datetime
import time
import hashlib
import logging
import os
import re
import requests
from typing import Optional

# Force IPv4 — Google rate-limits IPv6 more aggressively
import urllib3.util.connection
urllib3.util.connection.HAS_IPV6 = False

from GoogleAds import GoogleAds
from google.auth import default
import gspread
# Using raw Sheets API for formatting (gspread_formatting has auth scope issues)

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_SHEET_ID = "1xP3ivxuECvba9Z9zahbPLsiSLSSpUt6DvTHClYkJ-Go"
OUTPUT_SHEET_NAME = "Google Ads Transparency - Competitor Ads"
REGIONS = ["SA", "AE"]
MAX_ADS_PER_ADVERTISER = 500
SCRAPE_DELAY = 3.0  # seconds between ad detail API calls
SEARCH_DELAY = 5.0  # seconds between search/suggestion calls
COMPETITOR_DELAY = 8.0  # seconds between processing different competitors
RETRY_DELAY = 30.0  # seconds to wait on rate limit before retry
MAX_RETRIES = 3

# ── Known advertiser mappings ─────────────────────────────────────────────────
# Maps (competitor_name, region) → list of search terms + known advertiser IDs.
# This helps when the default name search doesn't find the right advertiser.
# Format: {"search_terms": [...], "known_ids": [{"id": ..., "name": ...}]}
ADVERTISER_OVERRIDES = {
    ("Rajhi Bank", "SA"): {
        "search_terms": ["Al Rajhi Bank", "Al Rajhi Banking"],
        "known_ids": [
            {"id": "AR07393135804576432129", "name": "Al Rajhi Banking & Investment Corp."},
            {"id": "AR17149597601662763009", "name": "Al Rajhi Banking and Investment Corporation"},
        ],
    },
    ("EmiratesNBD", "AE"): {
        "search_terms": ["Emirates NBD"],
        "known_ids": [
            {"id": "AR11606100870541869057", "name": "EMIRATES NBD (P.J.S.C)"},
        ],
    },
    ("Liv (owned by EmiratesNBD)", "AE"): {
        "search_terms": ["Emirates NBD", "Liv bank"],
        "known_ids": [
            # Liv is a product of EmiratesNBD — ads may be under Emirates NBD
            {"id": "AR11606100870541869057", "name": "EMIRATES NBD (P.J.S.C)"},
        ],
    },
    ("STC Pay", "SA"): {
        "search_terms": ["stc pay", "STC", "Saudi Telecom"],
        "known_ids": [],
    },
    ("STCBank", "SA"): {
        "search_terms": ["stc bank", "STC Bank", "Saudi Telecom"],
        "known_ids": [],
    },
    ("Saudi National Bank", "SA"): {
        "search_terms": ["Saudi National Bank", "The Saudi National Bank", "SNB", "Al Ahli"],
        "known_ids": [],
    },
    ("Tamara", "SA"): {
        "search_terms": ["Tamara", "Tamara Company", "Tamara Financing"],
        "known_ids": [],
    },
    ("Tamara", "AE"): {
        "search_terms": ["Tamara", "Tamara Company"],
        "known_ids": [],
    },
    ("Mamo", "AE"): {
        "search_terms": ["Mamo Pay", "mamopay", "Mamo"],
        "known_ids": [],
    },
    ("Wio", "AE"): {
        "search_terms": ["Wio Bank", "Wio bank PJSC"],
        "known_ids": [],
    },
    ("Revolut", "AE"): {
        "search_terms": ["Revolut"],
        "known_ids": [],
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ads_scraper")

# ── Global competitors (not region-specific, scrape with region="anywhere") ────
GLOBAL_COMPETITORS = [
    {"name": "Revolut", "website": "https://www.revolut.com/", "category": "Global", "known_id": "AR07098428377224183809", "known_name": "Revolut Ltd"},
    {"name": "Monzo", "website": "https://monzo.com/", "category": "Global", "known_id": "AR07289389941828616193", "known_name": "MONZO BANK LIMITED"},
    # Cash App / Square / Block removed from Google Ads tracking
    {"name": "Wise", "website": "https://wise.com/", "category": "Global", "known_id": "AR14378710480124379137", "known_name": "Wise Payments Limited"},
    {"name": "Klarna", "website": "https://www.klarna.com/", "category": "Global", "known_id": "AR03841049863391281153", "known_name": "Klarna AB"},
]

# ── Output columns ────────────────────────────────────────────────────────────
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


def retry_on_rate_limit(func):
    """Decorator to retry functions on rate limit / JSON parse errors."""
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except (json.JSONDecodeError, ValueError) as e:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_DELAY * (attempt + 1)
                    log.warning(f"Rate limited (attempt {attempt+1}/{MAX_RETRIES}), waiting {wait}s...")
                    time.sleep(wait)
                else:
                    log.error(f"Failed after {MAX_RETRIES} retries: {e}")
                    raise
            except Exception as e:
                if "Expecting value" in str(e) and attempt < MAX_RETRIES - 1:
                    wait = RETRY_DELAY * (attempt + 1)
                    log.warning(f"API error (attempt {attempt+1}/{MAX_RETRIES}), waiting {wait}s...")
                    time.sleep(wait)
                else:
                    raise
    return wrapper


def safe_search_suggestions(ga: GoogleAds, term: str) -> list:
    """Search suggestions with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            result = ga.get_all_search_suggestions(term)
            time.sleep(SEARCH_DELAY)
            return result
        except (json.JSONDecodeError, ValueError, requests.exceptions.JSONDecodeError):
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (attempt + 1)
                log.warning(f"  Rate limited searching '{term}', waiting {wait}s...")
                time.sleep(wait)
                ga.refresh_session()
                time.sleep(5)
            else:
                return []
    return []


def safe_creative_search(ga: GoogleAds, adv_id: str, count: int) -> list:
    """Creative search with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            result = ga.creative_search_by_advertiser_id(adv_id, count=count)
            time.sleep(SEARCH_DELAY)
            return result
        except (json.JSONDecodeError, ValueError, requests.exceptions.JSONDecodeError):
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (attempt + 1)
                log.warning(f"  Rate limited fetching creatives, waiting {wait}s...")
                time.sleep(wait)
                ga.refresh_session()
                time.sleep(5)
            else:
                return []
    return []


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


def read_competitors(gc) -> list[dict]:
    """Read competitor list from the input Google Sheet."""
    sh = gc.open_by_key(INPUT_SHEET_ID)
    ws = sh.worksheet("Competitors")
    rows = ws.get_all_values()

    # Headers: ['', 'Saudi Arabia', 'Website', 'UAE', 'Website']
    competitors = []
    for row in rows[1:]:  # skip header
        category = row[0] if row[0] else ""
        # SA competitor
        if row[1].strip():
            competitors.append({
                "name": row[1].strip(),
                "website": row[2].strip(),
                "region": "SA",
                "category": "GCC",
            })
        # AE competitor
        if row[3].strip():
            competitors.append({
                "name": row[3].strip(),
                "website": row[4].strip(),
                "region": "AE",
                "category": "GCC",
            })

    # Add global competitors
    for gc_comp in GLOBAL_COMPETITORS:
        competitors.append({
            "name": gc_comp["name"],
            "website": gc_comp["website"],
            "region": "Global",
            "category": "Global",
            "known_id": gc_comp["known_id"],
            "known_name": gc_comp["known_name"],
        })

    return competitors


def _parse_suggestion(s: dict) -> Optional[dict]:
    """Parse a search suggestion into a standard dict."""
    info = s.get("1", {})
    if not info.get("1") or not info.get("2"):
        return None
    ad_info = info.get("4", {}).get("2", {})
    ad_count_str = ad_info.get("2", "0") if ad_info else "0"
    return {
        "name": info["1"],
        "id": info["2"],
        "country": info.get("3", ""),
        "ad_count": int(ad_count_str) if ad_count_str else 0,
    }


def find_advertiser(ga: GoogleAds, name: str, website: str, region: str = "") -> list[dict]:
    """
    Find advertisers on Transparency Center.
    Returns a list of matched advertisers (may be multiple for one competitor).
    Uses overrides, name search, and domain search.
    """
    results = []
    seen_ids = set()

    # Check for hardcoded overrides first
    override = ADVERTISER_OVERRIDES.get((name, region))

    # Strategy 0: Known advertiser IDs from overrides
    if override and override.get("known_ids"):
        for known in override["known_ids"]:
            if known["id"] not in seen_ids:
                results.append({
                    "name": known["name"],
                    "id": known["id"],
                    "country": region,
                    "ad_count": 0,
                })
                seen_ids.add(known["id"])

    if results:
        return results

    # Build list of search terms
    search_terms = [name]
    if override and override.get("search_terms"):
        search_terms = override["search_terms"] + [name]
    # Deduplicate while preserving order
    search_terms = list(dict.fromkeys(search_terms))

    # Strategy 1: Search by name variations, prefer SA/AE country matches
    for term in search_terms:
        suggestions = safe_search_suggestions(ga, term)
        if not suggestions:
            continue

        # First pass: exact/close match with preferred country
        for s in suggestions:
            parsed = _parse_suggestion(s)
            if not parsed or parsed["id"] in seen_ids:
                continue
            s_name = parsed["name"].lower()
            s_country = parsed["country"]
            term_lower = term.lower()

            # Strong match: name matches and country is SA or AE
            name_match = (
                s_name == term_lower
                or term_lower in s_name
                or s_name.startswith(term_lower.split()[0])
            )
            country_match = s_country in ("SA", "AE", region)

            if name_match and country_match:
                results.append(parsed)
                seen_ids.add(parsed["id"])

        # Second pass: name match regardless of country (if no results yet)
        if not results:
            for s in suggestions:
                parsed = _parse_suggestion(s)
                if not parsed or parsed["id"] in seen_ids:
                    continue
                s_name = parsed["name"].lower()
                term_lower = term.lower()

                if s_name == term_lower or term_lower in s_name:
                    results.append(parsed)
                    seen_ids.add(parsed["id"])
                    break  # take first good match

        if results:
            return results

    # Strategy 2: Search by domain
    if website:
        domain = website.replace("https://", "").replace("http://", "").rstrip("/")

        # Try domain URL suggestion
        suggestions = ga.get_all_search_suggestions(f"https://{domain}")
        if suggestions:
            for s in suggestions:
                if s.get("2"):
                    found_domain = s["2"].get("1", "")
                    if domain.lower() in found_domain.lower() or found_domain.lower() in domain.lower():
                        adv = ga.get_advistisor_by_domain(domain)
                        if adv and adv["Advertisor Id"] not in seen_ids:
                            results.append({
                                "name": adv["Name"],
                                "id": adv["Advertisor Id"],
                                "country": "",
                                "ad_count": 0,
                            })
                            seen_ids.add(adv["Advertisor Id"])

        # Direct domain search
        if not results:
            adv = ga.get_advistisor_by_domain(domain)
            if adv and adv["Advertisor Id"] not in seen_ids:
                results.append({
                    "name": adv["Name"],
                    "id": adv["Advertisor Id"],
                    "country": "",
                    "ad_count": 0,
                })

    return results


def _extract_urls_from_string(text: str) -> list[str]:
    """Extract all URLs from a string (handles HTML snippets, JS, etc.)."""
    return re.findall(r'https?://[^\s\'"<>\\]+', text)


def _find_image_url(obj, depth=0) -> str:
    """Recursively search a nested dict/list for a tpc.googlesyndication.com/simgad image URL."""
    if depth > 10:
        return ""
    if isinstance(obj, str):
        if "tpc.googlesyndication.com" in obj and "simgad" in obj:
            urls = _extract_urls_from_string(obj)
            for u in urls:
                if "simgad" in u:
                    return u.split("'")[0].split('"')[0].split("\\")[0]
        return ""
    if isinstance(obj, dict):
        for v in obj.values():
            found = _find_image_url(v, depth + 1)
            if found:
                return found
    if isinstance(obj, list):
        for item in obj:
            found = _find_image_url(item, depth + 1)
            if found:
                return found
    return ""


def _find_video_url(obj, depth=0) -> str:
    """Recursively search for a video URL (googlevideo.com or youtube.com)."""
    if depth > 10:
        return ""
    if isinstance(obj, str):
        if "googlevideo.com" in obj or "youtube.com" in obj or "youtu.be" in obj:
            urls = _extract_urls_from_string(obj)
            for u in urls:
                if "googlevideo.com" in u or "youtube.com" in u or "youtu.be" in u:
                    return u
        return ""
    if isinstance(obj, dict):
        for v in obj.values():
            found = _find_video_url(v, depth + 1)
            if found:
                return found
    if isinstance(obj, list):
        for item in obj:
            found = _find_video_url(item, depth + 1)
            if found:
                return found
    return ""


def _find_landing_page(obj, depth=0) -> str:
    """Recursively search for a landing page URL (not google/syndication)."""
    if depth > 10:
        return ""
    if isinstance(obj, str):
        if obj.startswith("http") and "google" not in obj.lower() and "syndication" not in obj.lower():
            return obj
        return ""
    if isinstance(obj, dict):
        for v in obj.values():
            found = _find_landing_page(v, depth + 1)
            if found:
                return found
    if isinstance(obj, list):
        for item in obj:
            found = _find_landing_page(item, depth + 1)
            if found:
                return found
    return ""


def _resolve_displayads_url(ga: GoogleAds, url: str) -> tuple[str, str]:
    """
    Resolve a displayads-formats.googleusercontent.com URL to get the actual
    image (simgad) or video (googlevideo) URL from the JS response.
    Returns (image_url, video_url).
    """
    image_url = ""
    video_url = ""
    try:
        resp = ga.reqs.post(url, timeout=15)
        text = resp.text

        # Look for simgad image URLs
        simgad_matches = re.findall(r'https?://tpc\.googlesyndication\.com/[^\s\'"\\<>]+simgad/\d+', text)
        if simgad_matches:
            image_url = simgad_matches[0]

        # Look for googlevideo URLs (for video ads)
        for chunk in text.split("CDATA["):
            if "googlevideo.com" in chunk:
                decoded = chunk.split("]")[0]
                try:
                    decoded = decoded.encode("utf-8").decode("unicode_escape")
                    decoded = decoded.encode("utf-8").decode("unicode_escape")
                except Exception:
                    pass
                vid_urls = re.findall(r'https?://[^\s\'"\\<>]*googlevideo\.com[^\s\'"\\<>]*', decoded)
                if vid_urls:
                    video_url = vid_urls[0]
                    break

        # Also look for youtube URLs
        if not video_url:
            yt_matches = re.findall(r'https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)[^\s\'"\\<>]+', text)
            if yt_matches:
                video_url = yt_matches[0]

        # Extract thumbnail for video ads: YouTube thumbnails or lh3 images
        if not image_url:
            # YouTube video thumbnail (highest quality)
            yt_thumbs = re.findall(r'https?://i\d*\.ytimg\.com/vi/([^/]+)/[a-z]+\.jpg', text)
            if yt_thumbs:
                image_url = f"https://i.ytimg.com/vi/{yt_thumbs[0]}/hqdefault.jpg"
            else:
                # lh3.googleusercontent.com hosted images (ad creatives)
                lh3 = re.findall(r'(?:https?:)?//lh3\.googleusercontent\.com/[^\s\'"\\<>)]+', text)
                if lh3:
                    url_found = lh3[0]
                    if url_found.startswith("//"):
                        url_found = "https:" + url_found
                    image_url = url_found

    except Exception as e:
        log.debug(f"Could not resolve displayads URL: {e}")

    return image_url, video_url


def get_ad_details_safe(ga: GoogleAds, advertiser_id: str, creative_id: str) -> dict:
    """
    Safely get ad details with error handling.
    Returns a dict with ad info or partial info on error.
    """
    result = {
        "creative_id": creative_id,
        "ad_format": "",
        "last_shown": "",
        "image_url": "",
        "video_url": "",
        "landing_page": "",
        "preview_url": f"https://adstransparency.google.com/advertiser/{advertiser_id}/creative/{creative_id}",
    }

    try:
        # Call the raw API to get creative details, with retry
        resp = None
        for attempt in range(MAX_RETRIES):
            data = {
                "f.req": '{"1":"' + advertiser_id + '","2":"' + creative_id + '","5":{"1":1}}',
            }
            response = ga.reqs.post(
                "https://adstransparency.google.com/anji/_/rpc/LookupService/GetCreativeById",
                params={"authuser": "0"},
                data=data,
            )
            try:
                resp = response.json().get("1", {})
                break
            except (json.JSONDecodeError, ValueError, requests.exceptions.JSONDecodeError):
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_DELAY * (attempt + 1)
                    time.sleep(wait)
                else:
                    return result
        if not resp:
            return result

        # Ad format
        format_int = resp.get("8", 0)
        result["ad_format"] = {1: "Text", 2: "Image", 3: "Video"}.get(format_int, f"Unknown({format_int})")

        # Last shown date
        ts = resp.get("4", {}).get("1")
        if ts:
            result["last_shown"] = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d")

        # ── Extract media URLs ────────────────────────────────────────────
        creatives = resp.get("5", [])
        if not creatives:
            return result

        creative_block = creatives[0]

        # Dump first few raw responses for debugging
        debug_dir = os.path.join(os.path.dirname(__file__) or ".", "debug_responses")
        os.makedirs(debug_dir, exist_ok=True)
        debug_count = len(os.listdir(debug_dir))
        if debug_count < 30:  # save first 30 for debugging
            with open(os.path.join(debug_dir, f"{creative_id}.json"), "w") as f:
                json.dump({"full_response": resp, "creative_block": creative_block}, f, indent=2, ensure_ascii=False)

        # --- Strategy 1: Direct URL extraction from known paths ---
        # Image ads: resp["5"][0]["3"]["2"] contains HTML with simgad URL
        # Video ads: resp["5"][0]["2"]["4"] contains displayads/video URL
        # Text ads:  resp["5"][0]["1"]["4"] contains displayads URL

        raw_link = ""
        if result["ad_format"] == "Video":
            raw_link = (creative_block.get("2", {}).get("4", "")
                        or creative_block.get("1", {}).get("4", ""))
        elif result["ad_format"] == "Image":
            # The image HTML snippet contains the simgad URL or sadbundle iframe
            # Check ALL creative variants (resp["5"] can have multiple entries)
            for variant in creatives:
                raw_html = variant.get("3", {}).get("2", "")
                if not raw_html:
                    continue
                # Extract simgad URL from HTML string
                simgad = re.findall(r'https?://tpc\.googlesyndication\.com[^\s\'"\\<>]*simgad/\d+', raw_html)
                if simgad:
                    result["image_url"] = simgad[0]
                    break
                # Extract sadbundle iframe URL (HTML5/rich media ads)
                sadbundle = re.findall(r'https?://tpc\.googlesyndication\.com/archive/sadbundle/[^\s\'"\\<>]+', raw_html)
                if sadbundle and not result.get("embed_url"):
                    result["embed_url"] = sadbundle[0]
                # Fallback: try any URL in the HTML
                if "'" in raw_html and not result["image_url"]:
                    parts = raw_html.split("'")
                    for part in parts:
                        if part.startswith("http") and "sadbundle" not in part:
                            raw_link = part
                            break
            if not result["image_url"] and not raw_link:
                raw_link = creative_block.get("1", {}).get("4", "")
        else:
            raw_link = creative_block.get("1", {}).get("4", "")

        # --- Strategy 2: Resolve displayads URLs to get actual media ---
        if raw_link and "displayads" in raw_link:
            img_url, vid_url = _resolve_displayads_url(ga, raw_link)
            if img_url and not result["image_url"]:
                result["image_url"] = img_url
            if vid_url and not result["video_url"]:
                result["video_url"] = vid_url
        elif raw_link and "googlevideo.com" in raw_link:
            result["video_url"] = raw_link
        elif raw_link and "simgad" in raw_link:
            result["image_url"] = raw_link

        # --- Strategy 3: Deep recursive search as fallback ---
        if not result["image_url"] and result["ad_format"] == "Image":
            result["image_url"] = _find_image_url(creative_block)
        if not result["video_url"] and result["ad_format"] == "Video":
            result["video_url"] = _find_video_url(creative_block)

        # --- Strategy 4: Resolve ANY displayads URL found in the response ---
        # Works for both image and video ads that don't have direct media URLs
        if not result["image_url"] or (not result["video_url"] and result["ad_format"] == "Video"):
            all_text = json.dumps(creative_block)
            displayads_urls = re.findall(r'https?://displayads-formats\.googleusercontent\.com[^\s\'"\\<>]+', all_text)
            for durl in displayads_urls[:2]:
                img_url, vid_url = _resolve_displayads_url(ga, durl)
                if vid_url and not result["video_url"]:
                    result["video_url"] = vid_url
                if img_url and not result["image_url"]:
                    result["image_url"] = img_url

        # --- Strategy 5: Search the FULL response (not just creative_block) ---
        if not result["image_url"] and result["ad_format"] in ("Image", "Video"):
            full_text = json.dumps(resp)
            simgad_all = re.findall(r'https?://tpc\.googlesyndication\.com/[^\s\'"\\<>]*simgad/\d+', full_text)
            if simgad_all:
                result["image_url"] = simgad_all[0]
            if not result["image_url"]:
                # Try archive/simgad pattern
                archive_all = re.findall(r'https?://tpc\.googlesyndication\.com/archive/simgad/\d+', full_text)
                if archive_all:
                    result["image_url"] = archive_all[0]

        # --- Extract landing page ---
        result["landing_page"] = _find_landing_page(creative_block)

    except Exception as e:
        log.warning(f"Error getting details for {creative_id}: {e}")

    return result


def scrape_competitor(ga: GoogleAds, competitor: dict, batch_id: str) -> list[list]:
    """Scrape all ads for a single competitor in a single region. Returns list of row data."""
    name = competitor["name"]
    website = competitor["website"]
    region = competitor["region"]
    category = competitor.get("category", "GCC")
    today = datetime.date.today().isoformat()

    log.info(f"Searching for '{name}' ({website}) in {region}...")

    # For global competitors with known IDs, use them directly
    if competitor.get("known_id"):
        advertisers = [{"name": competitor["known_name"], "id": competitor["known_id"], "country": "", "ad_count": 0}]
    else:
        advertisers = find_advertiser(ga, name, website, region)

    if not advertisers:
        log.warning(f"  Could not find advertiser for '{name}' in {region}")
        return []

    rows = []
    for advertiser in advertisers:
        adv_name = advertiser["name"]
        adv_id = advertiser["id"]
        log.info(f"  Found: {adv_name} ({adv_id}), ~{advertiser['ad_count']} ads")

        # Fetch creative IDs
        creative_ids = safe_creative_search(ga, adv_id, MAX_ADS_PER_ADVERTISER)
        log.info(f"  Fetched {len(creative_ids)} creative IDs")

        for i, cid in enumerate(creative_ids):
            if i > 0 and i % 10 == 0:
                log.info(f"  Processing ad {i+1}/{len(creative_ids)}...")

            detail = get_ad_details_safe(ga, adv_id, cid)
            time.sleep(SCRAPE_DELAY)

            preview_url = detail.get("preview_url", "")
            landing_page = detail.get("landing_page", "")
            image_url = detail.get("image_url", "")
            video_url = detail.get("video_url", "")

            # Image preview formula (Google Sheets IMAGE function)
            image_preview = f'=IMAGE("{image_url}")' if image_url else ""
            # Video preview - link to the video or thumbnail
            video_preview = video_url if video_url else ""

            row = [
                name,                                  # Competitor Name
                website,                               # Competitor Website
                category,                               # Category (GCC / Global)
                region,                                 # Region
                adv_id,                                 # Advertiser ID
                adv_name,                               # Advertiser Name (TC)
                cid,                                    # Creative ID
                detail.get("ad_format", ""),            # Ad Format
                detail.get("last_shown", ""),           # Last Shown
                preview_url,                            # Ad Preview URL
                landing_page,                           # Landing Page
                image_url,                              # Image URL
                image_preview,                          # Image Preview (formula)
                video_url,                              # Video URL
                video_preview,                          # Video Preview
                today,                                  # Date Collected
                "",                                     # New This Week (filled later)
                batch_id,                               # Scrape Batch ID
                "Google Ads",                           # Platform
                detail.get("embed_url", ""),             # Embed URL (sadbundle iframe)
            ]
            rows.append(row)

    return rows


def get_or_create_output_sheet(gc) -> gspread.Spreadsheet:
    """Get existing output sheet or create a new one."""
    try:
        sh = gc.open(OUTPUT_SHEET_NAME)
        log.info(f"Found existing spreadsheet: {OUTPUT_SHEET_NAME}")
        return sh
    except gspread.SpreadsheetNotFound:
        log.info(f"Creating new spreadsheet: {OUTPUT_SHEET_NAME}")
        sh = gc.create(OUTPUT_SHEET_NAME)
        # Share with anyone who has the link
        sh.share("", perm_type="anyone", role="writer")
        return sh


def load_existing_creative_ids(ws) -> set:
    """Load all existing creative IDs from the output sheet to detect duplicates."""
    try:
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return set()
        # Creative ID is column index 6 (0-based), Region is index 3
        # Build composite key: creative_id + region
        cid_idx = HEADERS.index("Creative ID")
        reg_idx = HEADERS.index("Region")
        return {
            f"{row[cid_idx]}_{row[reg_idx]}" for row in all_values[1:] if len(row) > cid_idx and row[cid_idx]
        }
    except Exception:
        return set()


def load_previous_week_ids(ws) -> set:
    """Load creative IDs from the previous week to detect new ads."""
    try:
        all_values = ws.get_all_values()
        if len(all_values) <= 1:
            return set()

        today = datetime.date.today()
        week_ago = (today - datetime.timedelta(days=14)).isoformat()  # 2 weeks back

        date_col = HEADERS.index("Date Collected")
        cid_idx = HEADERS.index("Creative ID")
        reg_idx = HEADERS.index("Region")
        prev_ids = set()
        for row in all_values[1:]:
            if len(row) > date_col and row[date_col]:
                if row[date_col] < today.isoformat() and row[date_col] >= week_ago:
                    prev_ids.add(f"{row[cid_idx]}_{row[reg_idx]}")
        return prev_ids
    except Exception:
        return set()


def format_output_sheet(ws, num_rows: int):
    """Apply formatting to the output sheet using raw Sheets API."""
    sheet_id = ws.id

    # Columns: Name, Website, Region, AdvID, AdvName, CreativeID, Format, LastShown,
    #          PreviewURL, LandingPage, ImageURL, ImagePreview, VideoURL, VideoPreview,
    #          DateCollected, NewThisWeek, BatchID
    col_widths = [150, 200, 50, 200, 200, 200, 80, 100, 250, 250, 250, 200, 250, 200, 100, 100, 150]

    requests_list = [
        # Freeze header row
        {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }
        },
        # Header formatting: dark background, white bold text
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": len(HEADERS)},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.15},
                        "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        # Row height for data rows (to show images)
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": max(num_rows, 2)},
                "properties": {"pixelSize": 100},
                "fields": "pixelSize",
            }
        },
    ]

    # Column widths
    for i, w in enumerate(col_widths):
        requests_list.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                "properties": {"pixelSize": w},
                "fields": "pixelSize",
            }
        })

    # Conditional formatting: highlight "NEW" cells in "New This Week" column (index 15) green
    new_col_idx = len(HEADERS) - 2  # "New This Week" is 2nd from last
    requests_list.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": new_col_idx, "endColumnIndex": new_col_idx + 1}],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": "NEW"}]},
                    "format": {
                        "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85},
                        "textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0.5, "blue": 0}},
                    },
                },
            },
            "index": 0,
        }
    })

    try:
        ws.spreadsheet.batch_update({"requests": requests_list})
    except Exception as e:
        log.warning(f"Could not apply formatting: {e}")


def write_results(gc, all_rows: list[list], batch_id: str):
    """Write results to the output Google Sheet."""
    sh = get_or_create_output_sheet(gc)

    # Get or create the data worksheet
    try:
        ws = sh.worksheet("Ads Data")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Ads Data", rows=1000, cols=len(HEADERS))
        ws.append_row(HEADERS, value_input_option="USER_ENTERED")

    # Load existing data for dedup and new-detection
    existing_ids = load_existing_creative_ids(ws)
    previous_week_ids = load_previous_week_ids(ws)

    log.info(f"Existing ads in sheet: {len(existing_ids)}")
    log.info(f"Previous week ads: {len(previous_week_ids)}")

    # Filter duplicates and mark new ads
    cid_idx = HEADERS.index("Creative ID")
    reg_idx = HEADERS.index("Region")
    new_rows = []
    skipped = 0
    for row in all_rows:
        creative_id = row[cid_idx]
        region = row[reg_idx]
        composite_key = f"{creative_id}_{region}"

        if composite_key in existing_ids:
            skipped += 1
            continue

        # Mark as new if not seen in previous weeks
        # "New This Week" is 2nd from last column
        new_col_idx = len(HEADERS) - 2
        if composite_key not in previous_week_ids and previous_week_ids:
            row[new_col_idx] = "NEW"

        new_rows.append(row)

    log.info(f"New ads to add: {len(new_rows)}, duplicates skipped: {skipped}")

    if not new_rows:
        log.info("No new ads to add.")
        return sh.url

    # Append rows in batches
    BATCH_SIZE = 50
    for i in range(0, len(new_rows), BATCH_SIZE):
        batch = new_rows[i : i + BATCH_SIZE]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        log.info(f"  Written {min(i + BATCH_SIZE, len(new_rows))}/{len(new_rows)} rows")
        time.sleep(1)

    # Format the sheet
    total_rows = len(ws.get_all_values())
    format_output_sheet(ws, total_rows)

    log.info(f"Output sheet URL: {sh.url}")
    return sh.url


def write_summary(gc, sh_url: str, all_rows: list, batch_id: str):
    """Write a summary tab with per-competitor stats."""
    sh = get_or_create_output_sheet(gc)

    try:
        summary_ws = sh.worksheet("Summary")
        summary_ws.clear()
    except gspread.WorksheetNotFound:
        summary_ws = sh.add_worksheet(title="Summary", rows=100, cols=8)

    # Build summary
    new_col_idx = HEADERS.index("New This Week")
    fmt_idx = HEADERS.index("Ad Format")
    reg_idx = HEADERS.index("Region")
    cat_idx = HEADERS.index("Category")
    stats = {}
    for row in all_rows:
        key = (row[0], row[reg_idx])  # (name, region)
        if key not in stats:
            stats[key] = {"website": row[1], "category": row[cat_idx], "total": 0, "new": 0, "formats": set()}
        stats[key]["total"] += 1
        if len(row) > new_col_idx and row[new_col_idx] == "NEW":
            stats[key]["new"] += 1
        if row[fmt_idx]:
            stats[key]["formats"].add(row[fmt_idx])

    summary_headers = [
        "Competitor", "Region", "Website", "Total Ads Found",
        "New This Week", "Ad Formats", "Scrape Date", "Batch ID"
    ]
    summary_rows = [summary_headers]
    for (name, region), s in sorted(stats.items()):
        summary_rows.append([
            name, region, s["website"], s["total"], s["new"],
            ", ".join(sorted(s["formats"])),
            datetime.date.today().isoformat(), batch_id,
        ])

    summary_ws.update(range_name="A1", values=summary_rows, value_input_option="USER_ENTERED")

    # Format summary header
    try:
        summary_ws.spreadsheet.batch_update({"requests": [
            {
                "repeatCell": {
                    "range": {"sheetId": summary_ws.id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {"userEnteredFormat": {
                        "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.15},
                        "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                    }},
                    "fields": "userEnteredFormat(backgroundColor,textFormat)",
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": summary_ws.id, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }
            },
        ]})
    except Exception as e:
        log.warning(f"Could not format summary: {e}")


def _clean_row_for_dashboard(d: dict) -> dict:
    """Clean a single row dict for dashboard display."""
    # Clean image URLs: extract src from <img> tags, skip non-image URLs
    img = d.get("Image URL", "")
    if "<img" in img:
        m = re.search(r'src=["\']([^"\'> ]+)', img)
        img = m.group(1) if m else ""
    if "sadbundle" in img or img.endswith(".html") or img.endswith(".js"):
        img = ""
    d["Image URL"] = img
    # Clean video URLs
    vid = d.get("Video URL", "")
    if vid:
        vid = vid.replace("&amp;", "&")
        if vid.startswith("//"):
            vid = "https:" + vid
    d["Video URL"] = vid
    if "Platform" not in d:
        d["Platform"] = "Google Ads"

    # Determine ad status: "Active" if Last Shown is within the last 30 days, else "Inactive"
    last_shown = d.get("Last Shown", "")
    if last_shown:
        try:
            ls_date = datetime.datetime.strptime(last_shown, "%Y-%m-%d").date()
            days_ago = (datetime.date.today() - ls_date).days
            d["Status"] = "Active" if days_ago <= 30 else "Inactive"
        except (ValueError, TypeError):
            d["Status"] = "Active"
    else:
        d["Status"] = "Active"  # Unknown = Active

    return d


def generate_dashboard(all_rows: list[list]):
    """Generate a local HTML dashboard from scraped data, merging with existing data."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    public_dir = os.path.join(script_dir, "public")
    js_path = os.path.join(public_dir, "ads_data.js")

    # Load existing dashboard data (if any)
    existing_data = []
    if os.path.exists(js_path):
        try:
            with open(js_path) as f:
                raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
            existing_data = json.loads(raw)
            log.info(f"Loaded {len(existing_data)} existing ads from dashboard")
        except Exception as e:
            log.warning(f"Could not load existing dashboard data: {e}")

    # Build lookup of existing ads by composite key (creative_id + region)
    existing_map = {}
    for d in existing_data:
        key = f"{d.get('Creative ID', '')}_{d.get('Region', '')}"
        existing_map[key] = d

    # Convert new rows to dicts and merge
    new_count = 0
    updated_count = 0
    for row in all_rows:
        d = {}
        for i, h in enumerate(HEADERS):
            d[h] = row[i] if i < len(row) else ""
        d = _clean_row_for_dashboard(d)

        key = f"{d.get('Creative ID', '')}_{d.get('Region', '')}"
        if key in existing_map:
            # Update existing entry with fresh data (newer scrape wins)
            old = existing_map[key]
            # Preserve image/video/embed URLs if new scrape didn't find them
            if not d.get("Image URL") and old.get("Image URL"):
                d["Image URL"] = old["Image URL"]
            if not d.get("Video URL") and old.get("Video URL"):
                d["Video URL"] = old["Video URL"]
            if not d.get("Embed URL") and old.get("Embed URL"):
                d["Embed URL"] = old["Embed URL"]
            existing_map[key] = d
            updated_count += 1
        else:
            existing_map[key] = d
            new_count += 1

    # Re-compute status for ALL ads (existing ones may have become inactive)
    data = []
    for d in existing_map.values():
        d = _clean_row_for_dashboard(d)
        data.append(d)

    # Sort by Last Shown (newest first)
    data.sort(key=lambda x: x.get("Last Shown", ""), reverse=True)

    # Write JS data file
    with open(js_path, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(data, f, ensure_ascii=False)
        f.write(";")

    # Also write to root for backward compat
    root_js_path = os.path.join(script_dir, "ads_data.js")
    with open(root_js_path, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(data, f, ensure_ascii=False)
        f.write(";")

    # Also write JSON for other uses
    json_path = os.path.join(script_dir, "ads_data.json")
    with open(json_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    log.info(f"Dashboard data written: {len(data)} total ads ({new_count} new, {updated_count} updated)")
    log.info(f"  Open dashboard.html in a browser to view")


def main():
    batch_id = f"batch_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    log.info(f"=== Google Ads Transparency Scraper - {batch_id} ===")
    log.info(f"Regions: {REGIONS}")

    # Authenticate
    gc = get_gspread_client()

    # Read competitors
    competitors = read_competitors(gc)
    log.info(f"Loaded {len(competitors)} competitor entries")
    for c in competitors:
        log.info(f"  {c['name']} ({c['region']}) - {c['website']}")

    # Initialize scrapers per region (with rate-limit check)
    all_regions = REGIONS + ["Global"]  # Global = "anywhere" (no region filter)
    scrapers = {}
    for region in all_regions:
        scraper_region = "anywhere" if region == "Global" else region
        log.info(f"Initializing scraper for region {region}...")
        for attempt in range(MAX_RETRIES):
            try:
                ga = GoogleAds(region=scraper_region)
                # Verify we're not rate-limited by doing a test search
                time.sleep(3)
                test = ga.get_all_search_suggestions("test")
                scrapers[region] = ga
                log.info(f"  Scraper for {region} ready")
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_DELAY * (attempt + 1) * 2
                    log.warning(f"  Rate limited initializing {region}, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    log.error(f"  Cannot initialize scraper for {region}: {e}")
                    log.error(f"  You may be IP-blocked. Try again in 10-15 minutes.")
                    return []
        time.sleep(5)

    # Scrape each competitor
    all_rows = []
    for comp in competitors:
        region = comp["region"]
        ga = scrapers[region]

        try:
            rows = scrape_competitor(ga, comp, batch_id)
            all_rows.extend(rows)
            log.info(f"  → {len(rows)} ads found for {comp['name']} ({region})")
        except Exception as e:
            log.error(f"  Error scraping {comp['name']} ({region}): {e}")
            # Try refreshing session and retry once
            try:
                ga.refresh_session()
                rows = scrape_competitor(ga, comp, batch_id)
                all_rows.extend(rows)
                log.info(f"  → {len(rows)} ads found for {comp['name']} ({region}) (retry)")
            except Exception as e2:
                log.error(f"  Retry also failed for {comp['name']} ({region}): {e2}")

        time.sleep(COMPETITOR_DELAY)  # pause between competitors

    log.info(f"\n=== Total ads collected: {len(all_rows)} ===")

    # Write to output sheet and generate dashboard
    if all_rows:
        sheet_url = write_results(gc, all_rows, batch_id)
        write_summary(gc, sheet_url, all_rows, batch_id)
        generate_dashboard(all_rows)
        log.info(f"Done! Sheet: {sheet_url}")
    else:
        log.warning("No ads found for any competitor.")

    return all_rows


if __name__ == "__main__":
    main()
