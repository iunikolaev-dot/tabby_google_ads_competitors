# Competitor Ad Intelligence — Architecture Blueprint

**Owner:** Ilya Nikolaev
**Repo:** `Google / Meta Ads competitors/`
**Last updated:** 2026-04-29

---

## 1. What this project does

Tracks competitor ads across **Google Ads Transparency Center** and **Meta Ad Library** for ~16 fintech competitors in three market groupings (KSA, UAE, Global). Output is a single dashboard with current + historical creatives, image/video previews inline, refreshed weekly. Replaces a manual 5+ hour/week tracking process.

Two source platforms, one dataset, one dashboard.

---

## 2. Top-level layout

```
Google Ads competitors/
├── public/                 ← deployed to Vercel
│   ├── ads_data.js         ← single source of truth (SOT). const ADS_DATA = [...]
│   ├── index.html          ← landing
│   ├── dashboard.html      ← main UI (filters, gallery, search)
│   ├── ad-render.html      ← single-ad render page (for preview iframes)
│   ├── meta_images/        ← downloaded Meta creatives  (gitignored, 1.6 GB)
│   ├── google_videos/      ← downloaded Google videos    (gitignored, 61 MB)
│   └── screenshots/        ← debug captures              (gitignored)
│
├── api/                    ← Vercel serverless functions
│   ├── analyze.js          ← OpenAI ad-analysis endpoint (uses prompt in OpenAI ad analysis prompt.rtf)
│   └── preview.js          ← creative preview proxy
│
├── v2/PRD_v2.md            ← binding spec for the v2 pipeline
├── PRD.md                  ← legacy v1 PRD (historical)
├── config.py               ← single config: competitors, advertiser IDs, hard caps, schema version
├── safety_check.py         ← MUST run before any network call (see PRD §4.9)
│
├── scrapers/               ← v2 source-specific scrapers
│   ├── apify_google.py     ← Apify Google Ads Transparency actor wrapper
│   ├── apify_meta.py       ← Apify Meta Ad Library actor wrapper
│   ├── firecrawl_google.py ← FireCrawl fallback scraper
│   └── vision_filter.py    ← OpenAI Vision filter (text-vs-image classification)
│
├── pipeline/               ← v2 ingestion & integrity layer
│   ├── merge.py            ← 2-phase merge into ads_data.js (tmp → rename)
│   ├── preview_validator.py← marks rows preview_status=verified|broken|unverified
│   └── recovery.py         ← rebuild SOT from backups
│
├── backups/                ← timestamped gzipped SOT snapshots (gitignored)
├── staging/                ← intermediate pipeline outputs (gitignored)
├── logs/, metrics/         ← run telemetry (gitignored)
│
├── run_weekly.py           ← v2 orchestrator entrypoint
├── scraper.py              ← v1 legacy scraper (still wired to LaunchAgent)
├── meta_scraper.py         ← Meta Ad Library scraper (Apify-backed)
├── firecrawl_scraper.py    ← FireCrawl wrapper (Google fallback)
├── fetch_images.py         ← downloads Meta images locally
├── refresh_meta_images.py  ← re-downloads expired Meta CDN URLs
├── download_meta_images.py ← bulk Meta media downloader
├── screenshot_ads.py       ← Playwright screenshot capture (debug)
├── merge_apify.py          ← Apify result merger
└── vercel.json             ← deploy config
```

---

## 3. Data flow

```
                           ┌──────────────────────────┐
                           │  config.py (competitors) │
                           └────────────┬─────────────┘
                                        │
              ┌─────────────────────────┼─────────────────────────┐
              │                         │                         │
       ┌──────▼──────┐          ┌───────▼───────┐         ┌───────▼───────┐
       │ apify_google│          │  apify_meta   │         │firecrawl_     │
       │ (Apify $)   │          │  (Apify $)    │         │google (fallbk)│
       └──────┬──────┘          └───────┬───────┘         └───────┬───────┘
              │                         │                         │
              └─────────────┬───────────┴─────────────────────────┘
                            │
                  ┌─────────▼──────────┐
                  │  staging/*.json    │   ← per-source raw output
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │ vision_filter.py   │   ← classify Image vs Text (Google only)
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │ preview_validator  │   ← preview_status: verified|broken|unverified
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │ pipeline/merge.py  │   ← 2-phase merge (tmp → atomic rename)
                  └─────────┬──────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
     ┌────────▼─────────┐       ┌─────────▼────────┐
     │ public/ads_data.js│       │ backups/*.gz     │
     │ (SOT)             │       │ (timestamped)    │
     └────────┬─────────┘       └──────────────────┘
              │
     ┌────────▼─────────┐
     │ Vercel deploy    │
     │ → dashboard.html │
     └──────────────────┘
```

The dashboard is **fully static** — `dashboard.html` reads `ads_data.js` at load time and filters client-side. No backend DB. The only server code is the two Vercel functions in `api/` (OpenAI proxy + preview).

---

## 4. The data model

Every row in `public/ads_data.js` is one creative. Key fields:

| Field | Notes |
|---|---|
| `Competitor Name`|
| `Category` | `KSA` / `UAE` / `Global` |
| `Platform` | `Google Ads` or `Meta Ads` |
| `Region` | Country code (SA, AE) or platform region (FACEBOOK, INSTAGRAM) |
| `Advertiser ID` | `AR...` for Google, numeric for Meta |
| `Creative ID` | `CR...` for Google, numeric for Meta |
| `Ad Format` | `Image` or `Video` (Text is **forbidden** — purged + ingest-blocked) |
| `Ad Preview URL` | Link to Transparency Center / Meta Ad Library |
| `Image URL` / `Video URL` | Direct media URL (often expires for Meta) |
| `Local Image` / `Local Video` | Local path under `public/meta_images/` or `public/google_videos/` |
| `Date Collected`, `Last Shown`, `Started Running`, `Status` | Lifecycle |
| `Scrape Batch ID` | e.g. `meta_20260408_180557` |
| `schema_version`, `preview_status`, `source_actor`, `preview_checked_at` | v2 fields |

**Schema version is currently 2** (`config.SCHEMA_VERSION`). Bump on row-shape change; migrations live under `pipeline/migrations/`.

---

## 5. Competitors (from `config.py`)

| Category | Competitor | Google Advertiser ID(s) |
|---|---|---|
| Global | |
| Global | |
| Global |  |
| Global |  |
| Global |  |
| KSA |  |
| KSA | |
| KSA | 
| UAE |  |
| UAE |  |

Full list in `config.py` → `COMPETITORS`.

---

## 6. Schedule & entrypoints

- **LaunchAgent:** `~/Library/LaunchAgents/com.tabby.ads-scraper.plist` runs `scraper.py` every **Monday 09:00 local** (legacy v1 entry — still active).
- **v2 entrypoint:** `run_weekly.py` (manual for now; gated on human approval per PRD §4.9 / Rule 0).
- **Manual helpers:** `run.sh` (Google) / `run_meta.sh` (Meta).

---

## 7. Deployment

Static site on **Vercel** (`vercel.json`).
- `public/` is served at root.
- `api/analyze.js` and `api/preview.js` are Vercel serverless functions.
- `X-Robots-Tag: noindex` on all routes — share via direct link only.
- `.env` / `.env*.local` hold OpenAI + Apify + FireCrawl keys (gitignored).

---

## 8. Cost & rate-limit constraints

| Source | Plan | Notes |
|---|---|---|
| **Apify** | $29/month budget | **Hard rule: always ask the user before triggering any paid actor.** Budget is shared across Google + Meta runs. |
| **FireCrawl** | Free | 500 pages/month, 2 concurrent, low rate limits. Used as Google fallback only. |
| **OpenAI** | Pay-as-you-go | Vision filter + analyze endpoint. |
| **Google Ads Transparency** | Free, rate-limited | Direct scraping is brittle; we hit it via Apify. |
| **Meta Ad Library** | Free, rate-limited | Same — accessed via Apify. |

PRD v2 enforces this with **Rule 0 (approval gate)** and `safety_check.py` invoked before every entrypoint.


---

## 11. Onboarding checklist for a new contributor

1. Read `v2/PRD_v2.md` end to end. It's binding.
2. Read this file for the layout.
3. Set up `.env` with `APIFY_TOKEN`, `OPENAI_API_KEY`, `FIRECRAWL_API_KEY` (ask Ilya).
4. `python3 safety_check.py` — must pass before doing anything else.
5. Inspect `public/ads_data.js` shape with a small script before touching the pipeline.
6. **Never** run a paid scraper without explicit "go ahead" from Ilya in the current session.
7. **Never** edit `public/ads_data.js` by hand. Go through `pipeline/merge.py`.
8. Backups live in `backups/` — recovery is via `pipeline/recovery.py`.

---

## 12. Open questions / next steps

- Decide what to do with the 119 mediaaless Google Video ads (link-out / Playwright spike / purge).
- v2 entrypoint (`run_weekly.py`) is still manual — wire it to a scheduler once stable.
- v1 `scraper.py` is still the LaunchAgent target. Cut over to `run_weekly.py` when v2 has run cleanly twice.
- Consider migrating SOT from a 5 MB JS file to SQLite/DuckDB once it crosses ~10k rows.
