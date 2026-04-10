#!/usr/bin/env python3
"""Phase 2 test: run experthasan on 10 Cash App ads to verify pattern."""
import json
import os
import re
import time
import requests


def load_env():
    env = {}
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


ENV = load_env()
APIFY_TOKEN = ENV["APIFY_TOKEN"]


def load_db():
    with open("public/ads_data.js") as f:
        content = f.read()
    start = content.index("[")
    return json.loads(content[start : content.rindex("]") + 1])


def apify_creative_details(adv_id, creative_id):
    resp = requests.post(
        "https://api.apify.com/v2/acts/experthasan~google-ads-transparency-api/runs",
        params={"token": APIFY_TOKEN},
        json={
            "searchType": "creative_details",
            "advertiserId": adv_id,
            "creativeId": creative_id,
            "countryCode": "US",
        },
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code != 201:
        return None
    run_id = resp.json()["data"]["id"]

    elapsed = 0
    while elapsed < 180:
        r = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            params={"token": APIFY_TOKEN},
            timeout=15,
        )
        status = r.json()["data"]["status"]
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
        time.sleep(5)
        elapsed += 5

    if status != "SUCCEEDED":
        return None

    dataset_id = r.json()["data"]["defaultDatasetId"]
    items = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items",
        params={"token": APIFY_TOKEN},
        timeout=30,
    ).json()
    return items[0] if items else None


def main():
    data = load_db()

    # Mix: 5 video + 5 image Cash App ads without previews
    no_preview = [
        d
        for d in data
        if d.get("Competitor Name") == "Cash App"
        and d.get("Platform") == "Google Ads"
        and not d.get("Image URL")
        and not d.get("Local Video")
        and not d.get("Local Image")
    ]

    videos = [d for d in no_preview if d.get("Ad Format") == "Video"][:5]
    images = [d for d in no_preview if d.get("Ad Format") == "Image"][:5]
    test_ads = videos + images

    print(f"Testing {len(test_ads)} Cash App ads ({len(videos)} video, {len(images)} image)", flush=True)
    print(f"Total without preview in DB: {len(no_preview)}", flush=True)

    stats = {"video_url": 0, "image_in_content": 0, "nothing": 0, "square": 0, "mightyhive": 0, "cashapp": 0}
    details = []

    for i, ad in enumerate(test_ads):
        cid = ad["Creative ID"]
        adv = ad["Advertiser ID"]
        fmt = ad["Ad Format"]
        print(f"\n[{i + 1}/{len(test_ads)}] {fmt} {cid}", flush=True)

        item = apify_creative_details(adv, cid)
        if not item:
            print("  FAIL", flush=True)
            continue

        adv_name = item.get("advertiser_name", "")
        variants = item.get("variants", [])

        video_url = None
        image_url = None
        for v in variants:
            if v.get("video_url") and not video_url:
                video_url = v["video_url"].replace("\\", "")
            if not image_url:
                vc = v.get("content", "") or ""
                m = re.search(r'<img[^>]+src=["\']([^"\'>]+)', vc)
                if m:
                    image_url = m.group(1)

        brand = "other"
        if "square" in adv_name.lower():
            brand = "Square"
            stats["square"] += 1
        elif "mightyhive" in adv_name.lower():
            brand = "MightyHive"
            stats["mightyhive"] += 1
        elif "cash" in adv_name.lower() or "block" in adv_name.lower():
            brand = "CashApp/Block"
            stats["cashapp"] += 1

        print(f"  advertiser: {adv_name} ({brand})", flush=True)
        print(f"  variants: {len(variants)}, video_url: {bool(video_url)}, image: {bool(image_url)}", flush=True)

        if video_url:
            stats["video_url"] += 1
        elif image_url:
            stats["image_in_content"] += 1
        else:
            stats["nothing"] += 1

        details.append({
            "cid": cid, "fmt": fmt, "adv_name": adv_name, "brand": brand,
            "variants": len(variants), "video_url": bool(video_url), "image_url": bool(image_url),
        })

        time.sleep(1)

    print(f"\n=== Summary ===", flush=True)
    print(f"video_url: {stats['video_url']}/{len(test_ads)}", flush=True)
    print(f"image in HTML: {stats['image_in_content']}/{len(test_ads)}", flush=True)
    print(f"nothing: {stats['nothing']}/{len(test_ads)}", flush=True)
    print(f"\nBy brand:", flush=True)
    print(f"  Square: {stats['square']}", flush=True)
    print(f"  MightyHive: {stats['mightyhive']}", flush=True)
    print(f"  CashApp/Block: {stats['cashapp']}", flush=True)
    print(f"\nCost: ${len(test_ads) * 0.015:.2f}", flush=True)

    with open("/tmp/phase2_test.json", "w") as f:
        json.dump({"stats": stats, "details": details}, f, indent=2)


if __name__ == "__main__":
    main()
