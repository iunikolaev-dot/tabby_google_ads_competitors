#!/usr/bin/env python3
"""
Refresh Meta ad images using Playwright.
Loads each competitor's Meta Ad Library page, intercepts image URLs,
matches them to existing ads by ad_archive_id, and downloads locally.

Usage:
    python3 refresh_meta_images.py              # all competitors
    python3 refresh_meta_images.py --competitor "Tamara"  # one competitor
"""

import json
import os
import re
import sys
import time
import hashlib
import argparse
import logging
import requests
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("refresh_meta")

SCRIPT_DIR = Path(__file__).parent
ADS_DATA_PATH = SCRIPT_DIR / "public" / "ads_data.js"
IMAGES_DIR = SCRIPT_DIR / "public" / "meta_images"

# Page ID → competitor name
PAGE_MAP = {
    "888799511134149": "Cash App",
    "107593894218382": "Tamara",
    "105245002169048": "Tiqmo",
    "100238958486269": "D360 Bank",
    "370543246139130": "Barq",
    "102791935482897": "Wio Bank",
    "141270813154032": "STC Bank",
    "379823329174805": "HALA Payment",
    "102701872367080": "Alaan",
    "390926061079580": "Klarna",
    "113612035651775": "Monzo",
}


def load_ads_data():
    with open(ADS_DATA_PATH) as f:
        raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
    return json.loads(raw)


def save_ads_data(ads):
    with open(ADS_DATA_PATH, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(ads, f, ensure_ascii=False)
        f.write(";")


def download_image(session, url, filepath):
    """Download image from URL."""
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200 and len(resp.content) > 1000:
            with open(filepath, "wb") as f:
                f.write(resp.content)
            return True
    except Exception:
        pass
    return False


def scrape_page_images(page, page_id, max_scroll=15):
    """
    Load Meta Ad Library page for a competitor and extract ad images.
    Returns dict: {ad_archive_id: image_url}
    """
    url = f"https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=ALL&is_targeted_country=false&media_type=all&search_type=page&view_all_page_id={page_id}"

    # Track all scontent image URLs
    image_urls = {}  # url -> response object
    ad_images = {}   # ad_archive_id -> image_url

    def on_response(response):
        resp_url = response.url
        if "scontent" in resp_url and ("t39.35426" in resp_url or "t45.5328" in resp_url):
            # These are ad creative images (t39.35426 = ad images)
            image_urls[resp_url] = True

    page.on("response", on_response)

    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
            time.sleep(5)
        except Exception as e:
            log.warning(f"  Failed to load page: {e}")
            return {}

    time.sleep(3)

    # Scroll to load more ads
    for i in range(max_scroll):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        # Check for "See more" button
        try:
            see_more = page.query_selector('div[role="button"]:has-text("See more")')
            if see_more:
                see_more.click()
                time.sleep(2)
        except Exception:
            pass

    # Now parse the page content to match ad_archive_ids to images
    # Extract ad cards and their images
    content = page.content()

    # Find all ad_archive_id values
    archive_ids = re.findall(r'ad_archive_id[=:][\s"]*(\d+)', content)
    log.info(f"  Found {len(archive_ids)} ad_archive_ids, {len(image_urls)} ad images intercepted")

    # Strategy: Extract img src from each ad card container
    ad_cards = page.query_selector_all('[class*="x1gslohp"]')  # Ad card containers
    if not ad_cards:
        ad_cards = page.query_selector_all('[class*="xrvj5dj"]')

    # Get all images with scontent URLs from the page
    all_imgs = page.evaluate('''() => {
        const imgs = document.querySelectorAll('img[src*="scontent"]');
        const results = [];
        for (const img of imgs) {
            // Walk up to find the closest ad container with ad_archive_id
            let el = img;
            let archiveId = null;
            for (let i = 0; i < 20 && el; i++) {
                el = el.parentElement;
                if (!el) break;
                const html = el.innerHTML;
                const match = html.match(/ad_archive_id[=:]\\s*"?(\\d+)/);
                if (match) {
                    archiveId = match[1];
                    break;
                }
            }
            if (archiveId && img.src.includes('t39.35426')) {
                results.push({id: archiveId, src: img.src, w: img.naturalWidth, h: img.naturalHeight});
            }
        }
        return results;
    }''')

    for item in all_imgs:
        aid = item['id']
        src = item['src']
        # Prefer larger images
        if aid not in ad_images or (item.get('w', 0) > 100):
            ad_images[aid] = src

    # Also try: for intercepted URLs, match by proximity in page
    log.info(f"  Matched {len(ad_images)} ads to images")
    return ad_images


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--competitor", type=str, help="Only refresh this competitor")
    args = parser.parse_args()

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Loading ads data...")
    ads = load_ads_data()

    # Build index of Meta ads needing images
    meta_ads_by_cid = {}
    for i, ad in enumerate(ads):
        if ad.get("Platform") == "Meta Ads":
            cid = ad.get("Creative ID", "")
            if cid:
                meta_ads_by_cid[cid] = i

    # Filter competitors
    pages_to_scrape = PAGE_MAP.copy()
    if args.competitor:
        pages_to_scrape = {pid: name for pid, name in PAGE_MAP.items() if name == args.competitor}

    log.info(f"Will scrape {len(pages_to_scrape)} competitors")

    from playwright.sync_api import sync_playwright

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    total_downloaded = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1200, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="en-US",
        )

        for page_id, comp_name in pages_to_scrape.items():
            log.info(f"\n=== {comp_name} (page {page_id}) ===")
            page = context.new_page()

            ad_images = scrape_page_images(page, page_id)
            page.close()

            # Download images and update ads_data
            downloaded = 0
            for archive_id, img_url in ad_images.items():
                if archive_id not in meta_ads_by_cid:
                    continue

                ads_idx = meta_ads_by_cid[archive_id]
                ext = "png" if ".png" in img_url.lower() else "jpg"
                filename = f"{archive_id}.{ext}"
                filepath = IMAGES_DIR / filename
                local_url = f"/meta_images/{filename}"

                # Skip if already downloaded
                if filepath.exists() and filepath.stat().st_size > 1000:
                    ads[ads_idx]["Local Image"] = local_url
                    ads[ads_idx]["Image URL"] = img_url  # Update with fresh URL too
                    downloaded += 1
                    continue

                if download_image(session, img_url, filepath):
                    ads[ads_idx]["Local Image"] = local_url
                    ads[ads_idx]["Image URL"] = img_url
                    downloaded += 1
                    time.sleep(0.2)

            total_downloaded += downloaded
            log.info(f"  Downloaded {downloaded} images for {comp_name}")

            # Save after each competitor
            save_ads_data(ads)
            time.sleep(3)

        browser.close()

    log.info(f"\nDone! Total: {total_downloaded} images downloaded/updated")
    log.info(f"Images saved to: {IMAGES_DIR}")


if __name__ == "__main__":
    main()
