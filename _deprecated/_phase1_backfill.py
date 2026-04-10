#!/usr/bin/env python3
"""Phase 1: MP4 backfill for Wise/Revolut/Monzo via experthasan actor.

Checkpoints after each ad — safe to interrupt and resume.
Skips ads that already have a local MP4 file.
"""
import json
import os
import re
import sys
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
APIFY_TOKEN = ENV.get("APIFY_TOKEN", "")
assert APIFY_TOKEN, "APIFY_TOKEN missing from .env"

VIDEOS_DIR = "public/google_videos"
ADS_JS = "public/ads_data.js"
ADS_JS_ROOT = "ads_data.js"
RESULTS_FILE = "_phase1_results.json"


def load_db():
    with open(ADS_JS) as f:
        content = f.read()
    start = content.index("[")
    return json.loads(content[start : content.rindex("]") + 1])


def save_db(data):
    js = "const ADS_DATA = " + json.dumps(data, ensure_ascii=False) + ";"
    with open(ADS_JS, "w") as f:
        f.write(js)
    with open(ADS_JS_ROOT, "w") as f:
        f.write(js)


def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            return json.load(f)
    return {}


def save_results(results):
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)


def apify_creative_details(adv_id, creative_id):
    """Call experthasan creative_details. Returns item dict or None."""
    try:
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
            print(f"    start HTTP {resp.status_code}", flush=True)
            return None
        run_id = resp.json()["data"]["id"]
    except Exception as e:
        print(f"    start err: {e}", flush=True)
        return None

    elapsed = 0
    while elapsed < 180:
        try:
            r = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                params={"token": APIFY_TOKEN},
                timeout=15,
            )
            status = r.json()["data"]["status"]
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
        except Exception as e:
            print(f"    poll err: {e}", flush=True)
        time.sleep(5)
        elapsed += 5

    if status != "SUCCEEDED":
        print(f"    {status}", flush=True)
        return None

    try:
        dataset_id = r.json()["data"]["defaultDatasetId"]
        items = requests.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}/items",
            params={"token": APIFY_TOKEN},
            timeout=30,
        ).json()
        return items[0] if items else None
    except Exception as e:
        print(f"    fetch err: {e}", flush=True)
        return None


def extract_preview(item):
    """Extract video_url and image_url from an experthasan item."""
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
    return video_url, image_url


def download_mp4(url, cid):
    """Download MP4 to public/google_videos/{cid}.mp4. Returns local path or None."""
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200 or len(r.content) < 1000:
            print(f"    dl HTTP {r.status_code}, {len(r.content)} bytes", flush=True)
            return None
        path = f"{VIDEOS_DIR}/{cid}.mp4"
        with open(path, "wb") as f:
            f.write(r.content)
        return f"/google_videos/{cid}.mp4"
    except Exception as e:
        print(f"    dl err: {e}", flush=True)
        return None


def main():
    os.makedirs(VIDEOS_DIR, exist_ok=True)
    data = load_db()
    results = load_results()

    TARGETS = ["Wise", "Revolut", "Monzo", "Cash App", "Klarna"]
    all_candidates = [
        d
        for d in data
        if d.get("Competitor Name") in TARGETS
        and d.get("Platform") == "Google Ads"
        and d.get("Ad Format") == "Video"
        and not d.get("Image URL")
        and not d.get("Local Video")
    ]

    # Skip ads that already have a local MP4 on disk
    already_downloaded = set()
    for cid in [d["Creative ID"] for d in all_candidates]:
        if os.path.exists(f"{VIDEOS_DIR}/{cid}.mp4"):
            already_downloaded.add(cid)

    # Apply downloaded MP4s to DB immediately
    db_updated_from_disk = 0
    by_cid = {d["Creative ID"]: d for d in data}
    for cid in already_downloaded:
        d = by_cid.get(cid)
        if d and not d.get("Local Video"):
            d["Local Video"] = f"/google_videos/{cid}.mp4"
            db_updated_from_disk += 1
    if db_updated_from_disk:
        save_db(data)
        print(f"Recovered {db_updated_from_disk} local MP4s into DB", flush=True)

    # Refresh candidate list (drop those that now have Local Video set)
    candidates = [
        d
        for d in all_candidates
        if not d.get("Local Video") and d["Creative ID"] not in results
    ]

    from collections import Counter
    by_comp = Counter(d["Competitor Name"] for d in candidates)
    print(f"Phase 1 backfill — {len(candidates)} ads remaining", flush=True)
    print(f"By competitor: {dict(by_comp)}", flush=True)
    print(f"Already downloaded: {len(already_downloaded)}", flush=True)
    print(f"Already in results cache: {len(results)}", flush=True)

    total_cost = 0.0
    ok_count = 0

    for i, ad in enumerate(candidates):
        cid = ad["Creative ID"]
        adv = ad["Advertiser ID"]
        name = ad["Competitor Name"]
        print(f"\n[{i + 1}/{len(candidates)}] {name} {cid}", flush=True)

        item = apify_creative_details(adv, cid)
        total_cost += 0.015
        if not item:
            results[cid] = {"status": "failed"}
            save_results(results)
            continue

        video_url, image_url = extract_preview(item)
        result = {"status": "ok", "video_url": video_url, "image_url": image_url}

        updated_field = None
        if video_url:
            local = download_mp4(video_url, cid)
            if local:
                result["local_video"] = local
                ad["Local Video"] = local
                updated_field = "Local Video"
                print(f"  ✓ MP4", flush=True)
                ok_count += 1
            else:
                print(f"  dl failed", flush=True)
        elif image_url:
            ad["Image URL"] = image_url
            result["applied_image"] = True
            updated_field = "Image URL"
            print(f"  ✓ image URL", flush=True)
            ok_count += 1
        else:
            variants = item.get("variants", [])
            print(f"  ✗ no preview ({len(variants)} variants)", flush=True)

        results[cid] = result
        save_results(results)

        # Save DB after EVERY successful update (checkpoint)
        if updated_field:
            save_db(data)

        time.sleep(1)

    print(f"\n=== Phase 1 done ===", flush=True)
    print(f"Processed this run: {len(candidates)}", flush=True)
    print(f"Got preview: {ok_count}", flush=True)
    print(f"Approx cost: ${total_cost:.2f}", flush=True)

    # Final summary: count MP4s in DB
    data = load_db()
    with_local = sum(1 for d in data if d.get("Local Video"))
    print(f"Total ads with Local Video in DB: {with_local}", flush=True)


if __name__ == "__main__":
    main()
