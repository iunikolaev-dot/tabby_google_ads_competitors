#!/usr/bin/env python3
"""
Download Meta ad images locally so they don't expire.
fbcdn URLs expire after a few days, so we save them as local files.

Usage:
    python3 download_meta_images.py          # download up to 100 missing images
    python3 download_meta_images.py --all    # download all missing images
    python3 download_meta_images.py --refresh # re-download expired images too
"""

import json
import os
import sys
import time
import hashlib
import argparse
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("download_meta_images")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADS_DATA_PATH = os.path.join(SCRIPT_DIR, "public", "ads_data.js")
IMAGES_DIR = os.path.join(SCRIPT_DIR, "public", "meta_images")
DELAY = 0.3  # seconds between downloads (Meta CDN is generous)


def load_ads_data():
    with open(ADS_DATA_PATH) as f:
        raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
    return json.loads(raw)


def save_ads_data(ads):
    with open(ADS_DATA_PATH, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(ads, f, ensure_ascii=False)
        f.write(";")


def url_to_filename(url, creative_id):
    """Generate a stable filename from the creative ID."""
    ext = "jpg"
    if ".png" in url.lower():
        ext = "png"
    elif ".webp" in url.lower():
        ext = "webp"
    return f"{creative_id}.{ext}"


def download_image(session, url, filepath):
    """Download an image from URL to filepath. Returns True on success."""
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200 and len(resp.content) > 500:
            with open(filepath, "wb") as f:
                f.write(resp.content)
            return True
        return False
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--refresh", action="store_true", help="Re-download images with expired URLs")
    args = parser.parse_args()

    limit = 999999 if args.all else args.limit
    os.makedirs(IMAGES_DIR, exist_ok=True)

    log.info("Loading ads data...")
    ads = load_ads_data()

    # Find Meta ads that need local images
    to_download = []
    for i, ad in enumerate(ads):
        if ad.get("Platform") != "Meta Ads":
            continue

        image_url = ad.get("Image URL", "")
        local_path = ad.get("Local Image", "")

        # Skip if already has a working local image
        if local_path and os.path.exists(os.path.join(SCRIPT_DIR, "public", local_path.lstrip("/"))):
            continue

        # Need to download if has fbcdn URL but no local copy
        if image_url and ("fbcdn" in image_url or "facebook" in image_url or "fb.com" in image_url):
            to_download.append((i, ad, image_url))

    log.info(f"Meta ads needing local images: {len(to_download)}")
    log.info(f"Will process up to: {min(limit, len(to_download))}")

    if not to_download:
        log.info("Nothing to do!")
        return

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    })

    downloaded = 0
    failed = 0

    for idx, (ads_idx, ad, url) in enumerate(to_download[:limit]):
        creative_id = ad.get("Creative ID", "") or hashlib.md5(url.encode()).hexdigest()[:16]
        filename = url_to_filename(url, creative_id)
        filepath = os.path.join(IMAGES_DIR, filename)
        local_url = f"/meta_images/{filename}"

        # Check if file already exists on disk
        if os.path.exists(filepath) and os.path.getsize(filepath) > 500:
            ads[ads_idx]["Local Image"] = local_url
            downloaded += 1
            continue

        if download_image(session, url, filepath):
            ads[ads_idx]["Local Image"] = local_url
            downloaded += 1
            if downloaded % 50 == 0:
                log.info(f"  Progress: {downloaded} downloaded...")
        else:
            failed += 1

        # Save checkpoint every 100
        if downloaded > 0 and downloaded % 100 == 0:
            save_ads_data(ads)
            log.info(f"  --- Checkpoint saved ({downloaded} images) ---")

        time.sleep(DELAY)

    # Final save
    if downloaded > 0:
        save_ads_data(ads)

    log.info(f"\nDone! {downloaded} images saved locally, {failed} failed")
    log.info(f"Images saved to: {IMAGES_DIR}")


if __name__ == "__main__":
    main()
