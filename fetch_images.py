#!/usr/bin/env python3
"""
Fetch missing image URLs for Google Ads in the dashboard.
Reads ads_data.js, finds ads without Image URL, fetches from Transparency Center API.
Saves progress after every batch so you can stop and resume safely.

Usage:
    python3 fetch_images.py              # fetch up to 250 images (safe batch)
    python3 fetch_images.py --limit 100  # fetch up to 100 images
    python3 fetch_images.py --all        # fetch all (will hit rate limits, auto-stops)
"""

import json
import re
import sys
import time
import datetime
import logging
import argparse
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fetch_images")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADS_DATA_PATH = os.path.join(SCRIPT_DIR, "public", "ads_data.js")
DELAY = 20  # seconds between requests — slow and steady to avoid rate limits
BATCH_SAVE = 10  # save after every N images fetched


def load_ads_data():
    with open(ADS_DATA_PATH) as f:
        raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
    return json.loads(raw)


def save_ads_data(ads):
    with open(ADS_DATA_PATH, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(ads, f, ensure_ascii=False)
        f.write(";")


def fetch_image_for_creative(session, adv_id, creative_id):
    """Fetch image/embed URL for a single creative from the Transparency Center API."""
    image_url = ""
    embed_url = ""

    req_data = {
        "f.req": '{"1":"' + adv_id + '","2":"' + creative_id + '","5":{"1":1}}'
    }
    try:
        response = session.post(
            "https://adstransparency.google.com/anji/_/rpc/LookupService/GetCreativeById",
            params={"authuser": "0"},
            data=req_data,
            timeout=15,
        )
        resp = response.json().get("1", {})
    except Exception:
        return "", ""

    creatives = resp.get("5", [])
    if not creatives:
        return "", ""

    ad_format = {1: "Text", 2: "Image", 3: "Video"}.get(resp.get("8", 0), "")

    full_text = json.dumps(creatives)

    # Strategy 1: Direct image URL from HTML snippet (simgad, 2mdn, etc.)
    for variant in creatives:
        raw_html = variant.get("3", {}).get("2", "")
        if raw_html:
            # simgad URLs
            simgad = re.findall(
                r"https?://tpc\.googlesyndication\.com[^\s'\"\\<>]*simgad/\d+",
                raw_html,
            )
            if simgad:
                return simgad[0], ""

            # 2mdn.net direct image URLs (Cash App, etc.)
            mdn = re.findall(
                r'https?://s\d+\.2mdn\.net/[^\s\'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)',
                raw_html, re.IGNORECASE,
            )
            if mdn:
                return mdn[0], ""

            # img src= from HTML
            img_src = re.findall(
                r'<img[^>]+src=["\']([^"\']+)["\']',
                raw_html, re.IGNORECASE,
            )
            for src in img_src:
                if any(h in src for h in ["2mdn.net", "googlesyndication.com", "googleusercontent.com", "gstatic.com"]):
                    return src, ""

            # Sadbundle (HTML5 rich media)
            sadbundle = re.findall(
                r"https?://tpc\.googlesyndication\.com/archive/sadbundle/[^\s'\"\\<>]+",
                raw_html,
            )
            if sadbundle:
                embed_url = sadbundle[0]

    # Strategy 2: displayads embed URL (for HTML5/rich media ads)
    for variant in creatives:
        display_url = (
            variant.get("1", {}).get("4", "")
            or variant.get("2", {}).get("4", "")
        )
        if display_url and "displayads" in display_url:
            # Try resolving for direct images
            try:
                dresp = session.get(display_url, timeout=15)
                dtext = dresp.text

                # simgad
                simgad = re.findall(
                    r"https?://tpc\.googlesyndication\.com[^\s'\"\\<>]*simgad/\d+",
                    dtext,
                )
                if simgad:
                    return simgad[0], embed_url

                # 2mdn.net
                mdn = re.findall(
                    r'https?://s\d+\.2mdn\.net/[^\s\'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)',
                    dtext, re.IGNORECASE,
                )
                if mdn:
                    return mdn[0], embed_url

                # YouTube thumbnail
                yt = re.findall(
                    r"https?://i\d*\.ytimg\.com/vi/([^/]+)/[a-z]+\.jpg", dtext
                )
                if yt:
                    return f"https://i.ytimg.com/vi/{yt[0]}/hqdefault.jpg", embed_url

                # lh3 Google-hosted image
                lh3 = re.findall(
                    r'(?:https?:)?//lh3\.googleusercontent\.com/[^\s\'"\\<>)]+',
                    dtext,
                )
                if lh3:
                    u = lh3[0]
                    if u.startswith("//"):
                        u = "https:" + u
                    return u, embed_url
            except Exception:
                pass

            # If can't resolve, use displayads URL as embed
            if not embed_url:
                embed_url = display_url

    # Strategy 3: Deep search full response for any image URLs
    # simgad
    simgad_all = re.findall(
        r"https?://tpc\.googlesyndication\.com[^\s'\"\\<>]*simgad/\d+", full_text
    )
    if simgad_all:
        return simgad_all[0], embed_url

    # 2mdn.net
    mdn_all = re.findall(
        r'https?://s\d+\.2mdn\.net/[^\s\'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)',
        full_text, re.IGNORECASE,
    )
    if mdn_all:
        return mdn_all[0], embed_url

    return image_url, embed_url


def main():
    parser = argparse.ArgumentParser(description="Fetch missing image URLs")
    parser.add_argument("--limit", type=int, default=15, help="Max images to fetch (default: 15 to stay under rate limit)")
    parser.add_argument("--all", action="store_true", help="Fetch all (auto-stops on rate limit)")
    args = parser.parse_args()

    limit = 999999 if args.all else args.limit

    log.info("Loading ads data...")
    ads = load_ads_data()
    log.info(f"Total ads: {len(ads)}")

    # Find ads missing images, prioritize by Last Shown (newest first)
    missing = []
    for i, ad in enumerate(ads):
        if (
            ad.get("Platform", "Google Ads") == "Google Ads"
            and ad.get("Ad Format") in ("Image", "Video")
            and not ad.get("Image URL")
            and not ad.get("Embed URL")
            and ad.get("Advertiser ID")
            and ad.get("Creative ID")
        ):
            missing.append((i, ad))

    # Sort: Active ads first, then by Last Shown descending (newest first)
    missing.sort(key=lambda x: (0 if x[1].get("Status") == "Active" else 1, x[1].get("Last Shown", "")), reverse=False)
    missing.sort(key=lambda x: (0 if x[1].get("Status") == "Active" else 1))
    # Within each group, sort by Last Shown descending
    active = [(i, a) for i, a in missing if a.get("Status") == "Active"]
    inactive = [(i, a) for i, a in missing if a.get("Status") != "Active"]
    active.sort(key=lambda x: x[1].get("Last Shown", ""), reverse=True)
    inactive.sort(key=lambda x: x[1].get("Last Shown", ""), reverse=True)
    missing = active + inactive
    log.info(f"Ads missing images: {len(missing)} ({len(active)} active, {len(inactive)} inactive)")
    log.info(f"Will fetch up to: {min(limit, len(missing))}")

    if not missing:
        log.info("Nothing to do!")
        return

    # Initialize a simple requests session
    import requests
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://adstransparency.google.com",
        "Referer": "https://adstransparency.google.com/",
    })

    # Test if we're rate limited using the actual endpoint we'll call
    log.info("Testing API access...")
    try:
        test_ad = missing[0][1]
        test_data = {"f.req": '{"1":"' + test_ad["Advertiser ID"] + '","2":"' + test_ad["Creative ID"] + '","5":{"1":1}}'}
        r = session.post(
            "https://adstransparency.google.com/anji/_/rpc/LookupService/GetCreativeById",
            params={"authuser": "0"},
            data=test_data,
            timeout=15,
        )
        if r.status_code == 429:
            log.error("Rate limited (429)! Try again later.")
            return
        log.info(f"Session OK (status {r.status_code})")
    except Exception as e:
        log.error(f"Connection error: {e}")
        return

    fetched = 0
    failed = 0
    consecutive_fails = 0

    for idx, (ads_idx, ad) in enumerate(missing[:limit]):
        adv_id = ad["Advertiser ID"]
        cr_id = ad["Creative ID"]
        name = ad.get("Competitor Name", "")

        image_url, embed_url = fetch_image_for_creative(session, adv_id, cr_id)

        if image_url:
            ads[ads_idx]["Image URL"] = image_url
            fetched += 1
            consecutive_fails = 0
            log.info(f"  [{fetched}] {name} {cr_id[:20]}... -> image OK")
        elif embed_url:
            ads[ads_idx]["Embed URL"] = embed_url
            fetched += 1
            consecutive_fails = 0
            log.info(f"  [{fetched}] {name} {cr_id[:20]}... -> embed OK")
        else:
            failed += 1
            consecutive_fails += 1
            if consecutive_fails >= 10:
                log.warning("10 consecutive failures — likely rate limited. Stopping.")
                break

        # Save checkpoint
        if fetched > 0 and fetched % BATCH_SAVE == 0:
            save_ads_data(ads)
            log.info(f"  --- Checkpoint saved ({fetched} images so far) ---")

        import random
        time.sleep(DELAY + random.uniform(0, 4))

    # Final save
    if fetched > 0:
        save_ads_data(ads)

    log.info(f"\nDone! Fetched {fetched} images, {failed} failed, {len(missing) - fetched - failed} remaining")
    log.info(f"Run again to continue: python3 fetch_images.py")


if __name__ == "__main__":
    main()
