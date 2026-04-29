#!/usr/bin/env python3
"""
Screenshot Google Ads from the Transparency Center.
Takes screenshots of ad creatives for ads missing image previews.

Usage:
    python3 screenshot_ads.py                  # screenshot up to 50 ads
    python3 screenshot_ads.py --limit 10       # screenshot 10 ads
    python3 screenshot_ads.py --competitor "Cash App"  # only Cash App ads
    python3 screenshot_ads.py --all            # all missing ads
"""

import json
import os
import sys
import time
import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("screenshot_ads")

SCRIPT_DIR = Path(__file__).parent
ADS_DATA_PATH = SCRIPT_DIR / "public" / "ads_data.js"
SCREENSHOTS_DIR = SCRIPT_DIR / "public" / "screenshots"
DELAY = 4  # seconds between screenshots


def load_ads_data():
    with open(ADS_DATA_PATH) as f:
        raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
    return json.loads(raw)


def save_ads_data(ads):
    with open(ADS_DATA_PATH, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(ads, f, ensure_ascii=False)
        f.write(";")


def screenshot_ad(page, adv_id, creative_id, output_path):
    """Navigate to Transparency Center creative page and screenshot the ad."""
    url = f"https://adstransparency.google.com/advertiser/{adv_id}/creative/{creative_id}"

    try:
        page.goto(url, wait_until="networkidle", timeout=20000)
    except Exception:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15000)
            time.sleep(3)
        except Exception as e:
            log.warning(f"  Failed to load page: {e}")
            return False

    # Check for CAPTCHA/rate limit
    content = page.content()
    if "unusual traffic" in content or "recaptcha" in content.lower():
        log.error("  CAPTCHA detected — rate limited. Stopping.")
        return None  # Signal to stop

    # Wait for the ad creative to render
    time.sleep(2)

    # Try to find the creative element and screenshot just that
    # The Transparency Center uses a creative-preview component
    creative_el = None
    selectors = [
        'creative-preview',
        '[class*="creative-container"]',
        '[class*="preview-container"]',
        '.ad-rendering',
        'iframe[src*="displayads"]',
        'iframe[src*="sadbundle"]',
        'img[src*="simgad"]',
        'img[src*="2mdn"]',
    ]

    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                box = el.bounding_box()
                if box and box['width'] > 50 and box['height'] > 50:
                    creative_el = el
                    break
        except Exception:
            continue

    try:
        if creative_el:
            # Screenshot just the creative element
            creative_el.screenshot(path=str(output_path), type="jpeg", quality=85)
        else:
            # Screenshot the main content area (crop out header/footer)
            # The creative is usually centered in the top portion
            page.screenshot(
                path=str(output_path),
                type="jpeg",
                quality=85,
                clip={"x": 0, "y": 0, "width": 800, "height": 600}
            )
        return True
    except Exception as e:
        log.warning(f"  Screenshot failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Screenshot Google Ads from Transparency Center")
    parser.add_argument("--limit", type=int, default=50, help="Max ads to screenshot (default: 50)")
    parser.add_argument("--all", action="store_true", help="Screenshot all missing ads")
    parser.add_argument("--competitor", type=str, help="Only this competitor")
    args = parser.parse_args()

    limit = 999999 if args.all else args.limit
    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Loading ads data...")
    ads = load_ads_data()

    # Find Google Ads missing screenshots
    missing = []
    for i, ad in enumerate(ads):
        if (
            ad.get("Platform", "Google Ads") == "Google Ads"
            and ad.get("Ad Format") in ("Image", "Video")
            and not ad.get("Image URL")
            and not ad.get("Screenshot")
            and ad.get("Advertiser ID")
            and ad.get("Creative ID")
            and ad.get("Status") == "Active"
        ):
            if args.competitor and ad.get("Competitor Name") != args.competitor:
                continue
            missing.append((i, ad))

    # Sort by Last Shown descending
    missing.sort(key=lambda x: x[1].get("Last Shown", ""), reverse=True)

    log.info(f"Ads needing screenshots: {len(missing)}")
    log.info(f"Will process up to: {min(limit, len(missing))}")

    if not missing:
        log.info("Nothing to do!")
        return

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1200, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        taken = 0
        failed = 0

        for idx, (ads_idx, ad) in enumerate(missing[:limit]):
            adv_id = ad["Advertiser ID"]
            cr_id = ad["Creative ID"]
            name = ad.get("Competitor Name", "")
            filename = f"{cr_id}.jpg"
            output_path = SCREENSHOTS_DIR / filename

            # Skip if screenshot already exists
            if output_path.exists():
                ads[ads_idx]["Screenshot"] = f"/screenshots/{filename}"
                taken += 1
                continue

            log.info(f"[{taken + 1}/{min(limit, len(missing))}] {name} - {cr_id[:20]}...")

            result = screenshot_ad(page, adv_id, cr_id, output_path)

            if result is None:
                # CAPTCHA — stop everything
                log.error("Rate limited. Saving progress and stopping.")
                break
            elif result:
                ads[ads_idx]["Screenshot"] = f"/screenshots/{filename}"
                taken += 1
                log.info(f"  -> Screenshot saved")
            else:
                failed += 1

            # Save checkpoint every 10
            if taken > 0 and taken % 10 == 0:
                save_ads_data(ads)
                log.info(f"  --- Checkpoint saved ({taken} screenshots) ---")

            time.sleep(DELAY)

        browser.close()

    # Final save
    if taken > 0:
        save_ads_data(ads)

    log.info(f"\nDone! {taken} screenshots taken, {failed} failed, {len(missing) - taken - failed} remaining")


if __name__ == "__main__":
    main()
