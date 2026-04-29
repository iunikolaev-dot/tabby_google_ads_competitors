#!/usr/bin/env python3
"""
Merge Apify Google Ads Transparency scraper output into ads_data.js.
Matches by creative ID and backfills image/embed URLs for ads missing previews.

Usage:
    python3 merge_apify.py apify_output.json
    python3 merge_apify.py cashapp_images.json cashapp_videos.json monzo_revolut.json
"""

import json
import re
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ADS_DATA_PATH = os.path.join(SCRIPT_DIR, "public", "ads_data.js")


def load_ads_data():
    with open(ADS_DATA_PATH) as f:
        raw = f.read().replace("const ADS_DATA = ", "", 1).rstrip().rstrip(";")
    return json.loads(raw)


def save_ads_data(ads):
    with open(ADS_DATA_PATH, "w") as f:
        f.write("const ADS_DATA = ")
        json.dump(ads, f, ensure_ascii=False)
        f.write(";")


def extract_image_url(preview_urls):
    """Extract best image URL from Apify previewUrls array."""
    if not preview_urls:
        return "", ""

    image_url = ""
    embed_url = ""

    for preview in preview_urls:
        # Direct simgad URL from img tag
        simgad = re.findall(
            r"https?://tpc\.googlesyndication\.com/archive/simgad/\d+", preview
        )
        if simgad:
            return simgad[0], ""

        # 2mdn.net direct image
        mdn = re.findall(
            r'https?://s\d+\.2mdn\.net/[^\s\'"\\<>]+\.(?:png|jpg|jpeg|gif|webp)',
            preview, re.IGNORECASE,
        )
        if mdn:
            return mdn[0], ""

        # img src from any HTML
        img_src = re.findall(r'src=["\']([^"\']+)["\']', preview, re.IGNORECASE)
        for src in img_src:
            if any(h in src for h in ["2mdn.net", "googlesyndication.com", "googleusercontent.com", "gstatic.com"]):
                return src, ""

        # YouTube thumbnail from iframe
        yt = re.findall(r"youtube\.com/embed/([^?\"']+)", preview)
        if yt:
            return f"https://i.ytimg.com/vi/{yt[0]}/hqdefault.jpg", ""

        # Sadbundle (HTML5 embed)
        sadbundle = re.findall(
            r"https?://tpc\.googlesyndication\.com/archive/sadbundle/[^\s'\"\\<>]+",
            preview,
        )
        if sadbundle:
            embed_url = sadbundle[0]

        # displayads URL as embed fallback
        displayads = re.findall(
            r"https?://displayads-formats\.googleusercontent\.com/[^\s'\"\\<>]+",
            preview,
        )
        if displayads and not embed_url:
            embed_url = displayads[0]

    return image_url, embed_url


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 merge_apify.py <apify_output.json> [more_files...]")
        sys.exit(1)

    # Load all Apify results
    apify_ads = []
    for filepath in sys.argv[1:]:
        print(f"Loading {filepath}...")
        with open(filepath) as f:
            data = json.load(f)
        if isinstance(data, list):
            apify_ads.extend(data)
        else:
            apify_ads.append(data)
    print(f"Loaded {len(apify_ads)} Apify ads")

    # Build lookup by creative ID
    apify_lookup = {}
    for ad in apify_ads:
        cr_id = ad.get("creativeId", "")
        if cr_id:
            apify_lookup[cr_id] = ad

    print(f"Unique creative IDs from Apify: {len(apify_lookup)}")

    # Load dashboard data
    print("Loading ads_data.js...")
    ads = load_ads_data()
    print(f"Total dashboard ads: {len(ads)}")

    # Find ads missing images and try to fill from Apify
    filled_image = 0
    filled_embed = 0
    not_found = 0

    for i, ad in enumerate(ads):
        if ad.get("Image URL") or ad.get("Embed URL"):
            continue  # already has preview

        cr_id = ad.get("Creative ID", "")
        if not cr_id:
            continue

        apify_ad = apify_lookup.get(cr_id)
        if not apify_ad:
            not_found += 1
            continue

        preview_urls = apify_ad.get("previewUrls", [])
        image_url, embed_url = extract_image_url(preview_urls)

        if image_url:
            ads[i]["Image URL"] = image_url
            filled_image += 1
        elif embed_url:
            ads[i]["Embed URL"] = embed_url
            filled_embed += 1

    print(f"\nResults:")
    print(f"  Filled with image URL: {filled_image}")
    print(f"  Filled with embed URL: {filled_embed}")
    print(f"  Not found in Apify data: {not_found}")
    print(f"  Total filled: {filled_image + filled_embed}")

    if filled_image + filled_embed > 0:
        save_ads_data(ads)
        print(f"\nSaved to {ADS_DATA_PATH}")
    else:
        print("\nNothing to update.")


if __name__ == "__main__":
    main()
