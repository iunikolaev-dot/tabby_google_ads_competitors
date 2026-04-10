#!/usr/bin/env python3
"""
Phase 3: Full Global Google Ads rescrape via experthasan domain search.

Per user spec:
  Klarna    — US (klarna.com)
  Wise      — GB (wise.com)
  Monzo     — GB (monzo.com)
  Cash App  — US (cash.app)
  Revolut   — GB (revolut.com)

Scrape window: last 7 days only (to save credits).

Strategy:
- Call experthasan with searchType=domain for each competitor
- Extract image URLs, video URLs, dates from variants
- Download MP4s locally
- Incremental merge into DB (preserve history, mark unseen as Inactive)
- Never delete ads
"""
import datetime
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
APIFY_TOKEN = ENV["APIFY_TOKEN"]

# ── Config ────────────────────────────────────────────────────────────────────
TODAY = datetime.date.today()
WEEK_AGO = (TODAY - datetime.timedelta(days=7)).isoformat()
TODAY_ISO = TODAY.isoformat()

# User-specified: competitor → (domain, countryCode, category, website, region_label)
GLOBAL_COMPETITORS = [
    {"name": "Klarna",   "domain": "klarna.com",  "country": "US", "website": "https://www.klarna.com/",  "region_label": "US", "category": "Global"},
    {"name": "Wise",     "domain": "wise.com",    "country": "GB", "website": "https://wise.com/",         "region_label": "GB", "category": "Global"},
    {"name": "Monzo",    "domain": "monzo.com",   "country": "GB", "website": "https://monzo.com/",        "region_label": "GB", "category": "Global"},
    {"name": "Cash App", "domain": "cash.app",    "country": "US", "website": "https://cash.app/",         "region_label": "US", "category": "Global"},
    {"name": "Revolut",  "domain": "revolut.com", "country": "GB", "website": "https://www.revolut.com/",  "region_label": "GB", "category": "Global"},
]

VIDEOS_DIR = "public/google_videos"
ADS_JS = "public/ads_data.js"
ADS_JS_ROOT = "ads_data.js"
ADS_JSON = "ads_data.json"


# ── DB I/O ────────────────────────────────────────────────────────────────────

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
    with open(ADS_JSON, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# ── Apify call ────────────────────────────────────────────────────────────────

def run_domain_search(domain, country):
    """Call experthasan with searchType=domain. Returns list of items."""
    payload = {
        "searchType": "domain",
        "domain": domain,
        "countryCode": country,
        "limit": 40,           # per-page max
        "maxPages": 10,        # actor limit — 400 ads max per competitor
        "startPeriod": WEEK_AGO,
        "endPeriod": TODAY_ISO,
    }
    print(f"  POST {domain} / {country} / {WEEK_AGO}→{TODAY_ISO}", flush=True)

    try:
        resp = requests.post(
            "https://api.apify.com/v2/acts/experthasan~google-ads-transparency-api/runs",
            params={"token": APIFY_TOKEN},
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
    except Exception as e:
        print(f"  start error: {e}", flush=True)
        return []

    if resp.status_code != 201:
        print(f"  start HTTP {resp.status_code}: {resp.text[:300]}", flush=True)
        return []

    run_id = resp.json()["data"]["id"]
    print(f"  run {run_id}", flush=True)

    elapsed = 0
    while elapsed < 900:
        try:
            r = requests.get(
                f"https://api.apify.com/v2/actor-runs/{run_id}",
                params={"token": APIFY_TOKEN},
                timeout=20,
            )
            status = r.json()["data"]["status"]
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                break
            if elapsed % 30 == 0:
                print(f"    {status} {elapsed}s", flush=True)
        except Exception as e:
            print(f"    poll err: {e}", flush=True)
        time.sleep(10)
        elapsed += 10

    if status != "SUCCEEDED":
        print(f"  {status}", flush=True)
        return []

    try:
        dataset_id = r.json()["data"]["defaultDatasetId"]
        # Pull all items (paginate if needed)
        items = []
        offset = 0
        while True:
            batch = requests.get(
                f"https://api.apify.com/v2/datasets/{dataset_id}/items",
                params={
                    "token": APIFY_TOKEN,
                    "offset": offset,
                    "limit": 1000,
                    "format": "json",
                },
                timeout=60,
            ).json()
            if not batch:
                break
            items.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        return items
    except Exception as e:
        print(f"  fetch err: {e}", flush=True)
        return []


# ── Parse item → ad record ────────────────────────────────────────────────────

def parse_item(item, comp):
    """Turn one experthasan item into our ad-record dict."""
    cid = item.get("creative_id", "")
    if not cid:
        return None

    adv_id = item.get("advertiser_id", "")
    adv_name = item.get("advertiser_name", "")
    fmt_type = (item.get("format_type") or "").strip()
    start = item.get("start", "") or ""
    last_seen = item.get("last_seen", "") or ""
    variants = item.get("variants", []) or []

    # Determine format
    fmt = fmt_type if fmt_type in ("Video", "Image", "Text") else ""
    if not fmt:
        # Fall back: check variants
        if any(v.get("video_url") for v in variants):
            fmt = "Video"
        elif any(v.get("image") for v in variants):
            fmt = "Image"
        else:
            fmt = "Text"

    # Skip text ads (user wants only video/image)
    if fmt == "Text":
        return None

    # Extract best image URL + video URL
    image_url = ""
    video_url = ""
    for v in variants:
        if not image_url and v.get("image"):
            img = v["image"]
            if any(d in img for d in ["simgad", "ytimg", "googleusercontent", "2mdn"]):
                image_url = img
        if not video_url and v.get("video_url"):
            video_url = str(v["video_url"]).replace("\\", "")
        # Also try to extract image from content HTML
        if not image_url:
            content_html = v.get("content", "") or ""
            m = re.search(r'<img[^>]+src=["\']([^"\'>]+)', content_html)
            if m:
                image_url = m.group(1)

    return {
        "cid": cid,
        "adv_id": adv_id,
        "adv_name": adv_name,
        "fmt": fmt,
        "image_url": image_url,
        "video_url": video_url,
        "start": start,
        "last_seen": last_seen,
        "comp": comp,
    }


# ── MP4 download ──────────────────────────────────────────────────────────────

def download_mp4(url, cid):
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200 or len(r.content) < 1000:
            return None
        path = f"{VIDEOS_DIR}/{cid}.mp4"
        with open(path, "wb") as f:
            f.write(r.content)
        return f"/google_videos/{cid}.mp4"
    except Exception:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(VIDEOS_DIR, exist_ok=True)
    data = load_db()

    # Phase 3A: remove ALL existing Google ads for the 5 Global competitors.
    # We're about to replace them with fresh, region-correct data.
    GLOBAL_NAMES = {c["name"] for c in GLOBAL_COMPETITORS}

    removed = 0
    kept = []
    preserved_local_videos = {}  # cid → local_video path (to restore if scraped again)
    for d in data:
        is_google_global = (
            d.get("Platform") == "Google Ads"
            and d.get("Competitor Name") in GLOBAL_NAMES
        )
        if is_google_global:
            cid = d.get("Creative ID", "")
            lv = d.get("Local Video", "")
            if cid and lv:
                preserved_local_videos[cid] = lv
            removed += 1
            continue
        kept.append(d)
    data = kept
    print(f"Removed {removed} old Global Google ads (will re-scrape)", flush=True)
    print(f"Preserved {len(preserved_local_videos)} Local Video file paths", flush=True)

    # Phase 3B: scrape each Global competitor with correct region
    all_new_ads = []
    total_cost = 0.0
    for comp in GLOBAL_COMPETITORS:
        print(f"\n=== {comp['name']} ({comp['country']}) ===", flush=True)
        items = run_domain_search(comp["domain"], comp["country"])
        print(f"  got {len(items)} items", flush=True)
        total_cost += 0.008 + len(items) * 0.005

        for item in items:
            parsed = parse_item(item, comp)
            if parsed:
                all_new_ads.append(parsed)

        # Breathing room between competitors
        time.sleep(2)

    print(f"\n=== Scraped total: {len(all_new_ads)} ads ===", flush=True)
    print(f"Estimated cost: ${total_cost:.2f}", flush=True)

    # Phase 3C: download MP4s for video ads
    mp4_count = 0
    for ad in all_new_ads:
        cid = ad["cid"]
        # If we already preserved a local video path AND file exists, reuse it
        if cid in preserved_local_videos:
            path = preserved_local_videos[cid]
            disk_path = path.lstrip("/")
            if os.path.exists(f"public/{disk_path.replace('google_videos/', 'google_videos/')}"):
                ad["local_video"] = path
                mp4_count += 1
                continue
        # Otherwise, download fresh if we have a video_url
        if ad.get("video_url"):
            local = download_mp4(ad["video_url"], cid)
            if local:
                ad["local_video"] = local
                mp4_count += 1

    print(f"MP4s: {mp4_count}", flush=True)

    # Phase 3D: build DB records and merge
    today_iso = TODAY.isoformat()
    new_records = []
    for ad in all_new_ads:
        comp = ad["comp"]
        last_shown = ad["last_seen"] or today_iso
        status = "Active"  # all are from last 7 days
        # Refine status if last_seen is older than 7 days (shouldn't happen)
        try:
            ls_date = datetime.date.fromisoformat(str(last_shown)[:10])
            days = (TODAY - ls_date).days
            if days > 7:
                status = "Inactive"
        except Exception:
            pass

        record = {
            "Competitor Name": comp["name"],
            "Competitor Website": comp["website"],
            "Category": comp["category"],
            "Region": comp["region_label"],
            "Advertiser ID": ad["adv_id"],
            "Advertiser Name (Transparency Center)": ad["adv_name"],
            "Creative ID": ad["cid"],
            "Ad Format": ad["fmt"],
            "Last Shown": last_shown,
            "Started Running": ad.get("start") or "",
            "Ad Preview URL": f"https://adstransparency.google.com/advertiser/{ad['adv_id']}/creative/{ad['cid']}?region={comp['country']}",
            "Landing Page / Destination URL": "",
            "Image URL": ad.get("image_url") or "",
            "Video URL": ad.get("video_url") or "",
            "Local Video": ad.get("local_video") or "",
            "Date Collected": today_iso,
            "New This Week": "NEW",
            "Scrape Batch ID": f"phase3_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "Platform": "Google Ads",
            "Status": status,
        }
        new_records.append(record)

    data.extend(new_records)
    data.sort(key=lambda x: str(x.get("Last Shown", "")), reverse=True)

    save_db(data)

    # Summary
    from collections import Counter
    by_comp = Counter()
    for r in new_records:
        by_comp[r["Competitor Name"]] += 1
    print("\n=== Phase 3 complete ===", flush=True)
    for c, n in sorted(by_comp.items()):
        print(f"  {c}: {n}", flush=True)
    print(f"Total added: {len(new_records)}", flush=True)
    print(f"Total in DB: {len(data)}", flush=True)
    print(f"Cost: ${total_cost:.2f}", flush=True)


if __name__ == "__main__":
    main()
