# Product Requirements Document — v2
## Competitor Ad Intelligence System

**Status:** Recovering from 2026-04-09 incident (613 Global Google ads wiped).
**Owner:** Ilya Nikolaev (Tabby Marketing).
**Last updated:** 2026-04-09.
**Deployment:** https://tabby-ad-intelligence.vercel.app (`X-Robots-Tag: noindex`, direct link only).
**Document version:** 2.0. Supersedes v1. Changes are tracked in §11.

---

## 0. How to read this document

This is an execution-ready specification. Every section labeled **MUST**, **SHALL**, or **INVARIANT** is a hard requirement that an executing agent (human or AI) is forbidden to violate. Every section labeled **SHOULD** or **MAY** is a preference. If an agent cannot satisfy a **MUST**, it stops, reports, and waits for a human decision. It does not improvise.

Two rules override everything else in this document:

> **Rule 0 — Approval gate.** No paid API call may be initiated without explicit human approval in the current session. Approval is per-run, not standing. The phrase "go ahead" or "approved" in the chat constitutes approval. Memory of past approvals does not carry over.
>
> **Rule 1 — Safety check first.** Every entry point invokes `safety_check.py` before any network call. If it fails, the run aborts. No exceptions.

---

## 1. Problem Statement

Tabby's marketing team tracks ~16 fintech competitors across two ad platforms (Google Ads Transparency Center, Meta Ad Library) and three market groupings (KSA, UAE, Global). Manual tracking takes 5+ hours per platform per week, goes stale within days, and provides no historical view. We need one dashboard with current and historical creatives, refreshed weekly, with visual previews inline.

---

## 2. Goals and non-goals

### 2.1 Goals (binding)

| # | Goal | Acceptance criterion |
|---|---|---|
| G1 | Single dashboard for all competitor ads across Google + Meta | One URL, one filter set, both platforms visible. |
| G2 | Weekly refresh on a fixed schedule, **gated on human approval** | Pipeline is *scheduled* for Mondays 06:00 GST but *does not run* until approval is given in the current session (see §4.9). |
| G3 | Visual previews inline | Image and video assets render in the UI. Broken or missing previews must be detected and surfaced (see §4.10). |
| G4 | Historical retention forever | Once an ad is in the DB, it stays. Status flips to Inactive after 7 days without observation. Deletion is forbidden except per Invariant I6 (§4.7). |
| G5 | Region-aware scraping | Each competitor has one canonical region. Ads from other regions are merged into one row with `Regions[]` appended, not stored as duplicates. |
| G6 | Filterable by competitor, region, format, status, date, platform | All six filters present in the dashboard. |
| G7 | FireCrawl-first cost discipline | Google scraping uses FireCrawl by default, within free tier (≤500 pages/month). Apify is a fallback only for advertisers FireCrawl failed to return preview URLs for, and only with approval. |
| G8 | Preview integrity self-check | After every merge, the pipeline validates that every ad's declared preview asset is actually renderable. Failures are flagged, not silently shipped. |

### 2.2 Non-goals

- Sub-daily refresh.
- Spend estimation from Transparency Center (unreliable).
- Targeting insights beyond what is publicly disclosed.
- OCR / copy extraction (deferred).
- Real-time alerting (deferred).

---

## 3. Current state

### 3.1 What works

| Component | State |
|---|---|
| Dashboard UI (grid + table, lightbox, dark theme, filters) | Live. |
| Meta Ad Library scraping via Apify `curious_coder/facebook-ads-library-scraper` | Working. 1,913 Meta ads in latest run. |
| Meta image local download (3,713 files in `public/meta_images/`) | Working. |
| Google preview URLs for regional advertisers via FireCrawl markdown card pattern | Working for Rajhi Bank, EmiratesNBD, Tamara, Ziina. |
| OpenAI Vision brand filter (Cash App vs Square vs BitKey) | Working. |
| Local Google MP4 videos | 116 files on disk, recoverable. |
| Vercel static deployment with `X-Robots-Tag: noindex` | Working. |

### 3.2 What is broken

| # | Component | Issue | Severity | Owner action |
|---|---|---|---|---|
| B1 | Global Google competitors | 613 ads (Klarna, Wise, Monzo, Cash App, Revolut) deleted by failed Phase 3 rescrape on 2026-04-09. Live Vercel deployment still serves the pre-deletion file from CDN. | **P0** | Run recovery procedure §10.1 before any new scrape. |
| B2 | `experthasan/google-ads-transparency-api` | Apify actor is officially under maintenance (confirmed via Apify console banner). Returns HTTP 500 on every call. | **P0** | Replaced with `crawlerbros/google-ads-scraper`. See §4.3.2 and §11. |
| B3 | FireCrawl on Google Transparency Center | 60s server-side timeout on every listing scrape since ~2026-04-08. Was working the day before. | **P1** | Diagnose with §10.2 before relying on FireCrawl-first strategy in §4.3. |
| B4 | No automated scheduler | `run_weekly.py` exists but has no cron / Action / trigger. | **P1** | Set up GitHub Actions per §4.9. |
| B5 | Single Meta Apify run for all 13 competitors | One page failure → zero data for everyone. | **P1** | Split into per-competitor batches per §4.3.1. |

### 3.3 Data inventory (as of 2026-04-09 21:30 GST)

```
Total: 2,513 ads in DB (down from ~3,126 before Phase 3 deletion)

Google Ads (regional intact, globals deleted):
  EmiratesNBD                267 Active /  42 Inactive
  Rajhi Bank                 104 Active /   0 Inactive
  Liv (EmiratesNBD subsid.)    0 Active /  30 Inactive
  Tamara                      33 Active /  46 Inactive
  Ziina                       13 Active /  64 Inactive
  Barq                         0 Active /   1 Inactive
  Klarna                       0 Active /   0 Inactive   ← DELETED, recoverable
  Wise                         0 Active /   0 Inactive   ← DELETED, recoverable
  Monzo                        0 Active /   0 Inactive   ← DELETED, recoverable
  Cash App                     0 Active /   0 Inactive   ← DELETED, recoverable
  Revolut                      0 Active /   0 Inactive   ← DELETED, recoverable

Meta Ads:
  Wise 778, Revolut 445, Tamara 136, Tiqmo 135, Cash App 122,
  Klarna 105, Monzo 92, Wio 43, Alaan 41, HALA 15, D360 1
  All Active, 0 Inactive (Meta history began today after merge fix)

Local assets on disk:
  Meta images:    3,713 files
  Google videos:    116 MP4 files
```

---

## 4. Architecture

### 4.1 Data flow

```
                    ┌──────────────────────┐
                    │  python run_weekly.py│  Single orchestrator
                    │  --mode={dry|live}   │  Default: dry
                    │  --max-cost-usd=N    │  Default: 2.00
                    └──────────┬───────────┘
                               │
                  ┌────────────▼────────────┐
                  │  safety_check.py        │  Aborts on any P-violation
                  │  • lock file            │
                  │  • backup verified      │
                  │  • approval token       │
                  │  • Apify balance ≥ $5   │
                  │  • SoT row count ≥ 90%  │
                  └────────────┬────────────┘
                               │ ok
     ┌─────────────────────────┼───────────────────────────┐
     ▼                         ▼                           ▼
┌──────────┐            ┌────────────┐              ┌─────────────┐
│FireCrawl │            │  Apify     │              │   OpenAI    │
│ Google   │            │            │              │ gpt-4o-mini │
│ listings │            │ Meta:      │              │  Vision     │
│ (default,│            │ curious_   │              │             │
│ free)    │            │ coder      │              │ Cash App    │
│          │            │            │              │ brand filter│
│ Per-     │            │ Google:    │              │ + preview   │
│ advertiser│           │ crawler    │              │ classifier  │
│ pages    │            │ bros       │              │ (§4.10)     │
│          │            │ (fallback  │              │             │
│          │            │ only, per  │              │             │
│          │            │ §4.3.2)    │              │             │
└────┬─────┘            └─────┬──────┘              └──────┬──────┘
     │                        │                            │
     ▼                        ▼                            ▼
   Per-advertiser         Per-competitor              Filter results,
   parsed cards →         Apify run results →         flag UNKNOWN,
   normalized rows        normalized rows             classify preview
     │                        │                       integrity
     └────────────────────────┼────────────────────────────┘
                              ▼
                   ┌──────────────────────┐
                   │  staging/{batch}.js  │  Per-batch staging
                   │  + integrity report  │
                   └──────────┬───────────┘
                              │ postconditions pass
                              ▼
                   ┌──────────────────────┐
                   │  2-phase atomic merge│  See §4.8
                   │  → public/ads_data.js│
                   └──────────┬───────────┘
                              ▼
                   ┌──────────────────────┐
                   │  preview_validator.py│  See §4.10
                   │  HEAD checks + sample│
                   │  rendering           │
                   └──────────┬───────────┘
                              ▼
                   ┌──────────────────────┐
                   │  manifest.json +     │
                   │  backups/ds_data.js  │
                   │  .{batch}.gz         │
                   └──────────┬───────────┘
                              ▼
                          Vercel deploy
                          (manual, gated)
```

### 4.2 Single source of truth

`public/ads_data.js` is the only canonical store. Any other file with the same name (`ads_data.js` at repo root, `ads_data.json` backup) **MUST** be deleted from the repo. If an agent finds either of these files outside `backups/`, it stops and asks before proceeding. The repeated `meta_scraper loaded from stale ads_data.json` bug in v1 is structurally fixed by deleting the stale paths, not by careful coding.

### 4.3 Tool responsibilities

| Tool | Role | Why this tool | Cost contract |
|---|---|---|---|
| **FireCrawl** | Default Google Transparency Center scraper. Per-advertiser pages. Free tier only. | Free, fast, returns clean markdown with image-creative pairings. Sufficient for regional advertisers today. | ≤100 pages per run. ≤500 pages per month total. Hard cap enforced in code. |
| **Apify `curious_coder/facebook-ads-library-scraper`** | Meta Ad Library scraping. | Meta Ad Library is bot-protected; this actor handles residential proxies. | $0.00075 per ad. Hard cap of $3.00 per run. |
| **Apify `crawlerbros/google-ads-scraper`** | Google fallback. **Used only for advertisers where FireCrawl returned no preview URLs in the current run** (per §4.3.2). | $0.70 per 1,000 results = ~7× cheaper than `experthasan`. Returns `previewUrl`, `imageUrl`, `videoUrl`, `firstShown`, `lastShown`, `format`. Maintained, 5.0 rating. **Caveat: `videoUrl` is YouTube-hosted only**, so MP4 downloads from `googlevideo.com` are no longer available. See §4.3.3. | $0.70 per 1,000 ads. Hard cap of $2.00 per run. Default `resultsLimit=200` per advertiser. |
| **OpenAI `gpt-4o-mini` Vision** | (a) Cash App brand filter. (b) Preview integrity sampling per §4.10. | Block, Inc. is the parent of Cash App / Square / BitKey and shares one Google advertiser ID. Only image classification can disambiguate. | ~$0.0002 per image. Hard cap: 500 calls per run. |
| **Vercel** | Static deployment with `X-Robots-Tag: noindex`. | Free, zero-config. Serves as DR mirror for `ads_data.js`. | Free tier. |

#### 4.3.1 Meta scraper batching

The Meta Apify actor **MUST** be invoked once per competitor, not once for all 13. Rationale: a single failed page in a 13-competitor run wipes the run for everyone. With per-competitor invocations, one failure is one missing competitor for one week, not zero data for everyone. Cost is unchanged (pricing is per-result, not per-run, and start fees are de minimis).

Implementation note: results from each per-competitor run are written to `staging/meta_{competitor}_{batch_id}.json` and merged at the end of the run, atomically.

#### 4.3.2 FireCrawl-first fallback rule (binding)

This is the rule for Google scraping in `run_weekly.py`:

```
1. For each competitor in COMPETITORS where Google is configured:
     - Attempt FireCrawl scrape of the advertiser's Transparency Center page.
     - If FireCrawl returns N rows AND ≥ 70% of those rows have a non-empty
       preview/image/video URL → mark this competitor as "FireCrawl OK".
     - Else → add the competitor to FALLBACK_LIST.

2. If FALLBACK_LIST is empty → done. Skip Apify entirely.

3. If FALLBACK_LIST is not empty:
     - Print FALLBACK_LIST and the estimated Apify cost
       (= sum(resultsLimit) × $0.0007).
     - Pause and wait for explicit human approval.
     - On approval, invoke crawlerbros/google-ads-scraper ONLY for the
       competitors in FALLBACK_LIST.
     - On rejection, write FALLBACK_LIST to logs/fallback_skipped.json
       and continue without Apify Google data this run.

4. NEVER call Apify Google for an advertiser that FireCrawl already
   handled successfully in the same run, even if the agent thinks the
   FireCrawl data could be "more complete." Cost discipline > completeness.
```

The 70% preview-URL coverage threshold is the trigger for "FireCrawl failed for this advertiser." It is configurable in `config.py` as `FIRECRAWL_MIN_PREVIEW_COVERAGE = 0.70`.

#### 4.3.3 Video MP4 regression — disclosed

The previous `experthasan` actor returned real `googlevideo.com` MP4 URLs, which we downloaded to `public/google_videos/`. The new `crawlerbros` actor returns only `videoUrl` for YouTube-hosted ads (i.e., a YouTube link, not an MP4). This means:

- The 116 existing MP4s on disk **MUST be preserved**. Do not delete them. They are linked from existing rows by `Local Video` and remain valid.
- New Google video ads scraped via `crawlerbros` will have a YouTube `videoUrl` and **no `Local Video`**. The dashboard MUST handle both cases.
- For the dashboard: if `Local Video` exists, render `<video>` with the local file. Else if `videoUrl` is a YouTube URL, render an embedded YouTube player. Else fall back to the static image.
- This is a deliberate trade-off: `crawlerbros` is 7× cheaper and currently maintained; `experthasan` is broken and was never reliable for MP4 URLs anyway. We accept the regression and document it.

If MP4 capture for new ads becomes a hard requirement later, the path forward is `apify/screenshot-url` on the `adLibraryUrl` to capture a still frame, plus an embedded YouTube player. This is in §7.3.

### 4.4 Competitor configuration

Stored in `config.py` as a list of dicts. The values below are authoritative; agents must not edit them without confirming the advertiser ID against `adstransparency.google.com` first.

| Competitor | Category | Google region | Google advertiser ID | Meta FB page ID |
|---|---|---|---|---|
| Klarna | Global | US | `AR05325035143755202561` (Klarna INC) | `390926061079580` |
| Wise | Global | GB | `AR14378710480124379137` (Wise Payments Limited) | `116206531782887` |
| Monzo | Global | GB | `AR07289389941828616193` (Monzo Bank Limited) | `113612035651775` |
| Cash App | Global | US | `AR14896030700992987137` (Block, Inc.) + Vision filter | `888799511134149` |
| Revolut | Global | GB | `AR07098428377224183809` (Revolut Ltd) | `335642513253333` |
| Tamara | Regional | SA | `AR02766979019476566017` | `107593894218382` |
| EmiratesNBD | Regional | AE | `AR11606100870541869057` | — |
| Al Rajhi Bank | Regional | SA | `AR07393135804576432129`, `AR17149597601662763009` | — |
| Ziina | Regional | AE | `AR06959610023805796353` | — |
| Tiqmo | Regional | SA | — | `105245002169048` |
| D360 Bank | Regional | SA | — | `100238958486269` |
| Barq | Regional | SA | — | `370543246139130` |
| Wio Bank | Regional | AE | — | `102791935482897` |
| STC Bank | Regional | SA | — | `141270813154032` |
| HALA Payment | Regional | SA | — | `379823329174805` |
| Alaan | Regional | AE | — | `102701872367080` |

Region precedence rule: if an ad for a Global competitor is observed in a region other than the canonical one (e.g., Klarna ad seen in DE while canonical is US), it is **merged** into the same row with the new region appended to `Regions[]`. It is not stored as a duplicate row.

### 4.5 Data model

Each ad in `public/ads_data.js` is a flat object with these fields. Schema version 2.

```js
{
  "schema_version": 2,
  "Competitor Name": "Wise",
  "Category": "Global",
  "Platform": "Google Ads",                  // or "Meta"
  "Advertiser ID": "AR14378710480124379137", // Google only
  "Advertiser Name": "Wise Payments Limited",
  "Page ID": "",                             // Meta only
  "Creative ID": "CR14733594866059575297",   // Google CR... or Meta archive ID
  "Ad Format": "Image",                      // Image | Video | Text | HTML5
  "Image URL": "https://tpc.googlesyndication.com/archive/simgad/...",
  "Video URL": "",                           // YouTube URL (crawlerbros) or fbcdn (Meta)
  "Local Image": "/meta_images/CR....jpg",   // present iff downloaded
  "Local Video": "/google_videos/CR....mp4", // present iff downloaded
  "Ad Preview URL": "https://adstransparency.google.com/...",
  "Landing Page": "",
  "Regions": ["US", "DE"],                   // canonical first
  "First Shown": "2026-01-15",
  "Last Shown": "2026-04-08",
  "Date Collected": "2026-04-09",
  "first_seen_batch_id": "weekly_20260115_060000_a1b2c3d4",
  "last_seen_batch_id":  "weekly_20260409_060000_e5f6g7h8",
  "source_actor": "firecrawl" | "crawlerbros" | "curious_coder",
  "retired": false,
  "retired_reason": "",
  "preview_status": "ok" | "missing" | "broken" | "unverified",
  "preview_checked_at": "2026-04-09T06:12:33Z",
  "Status": "Active"                         // DERIVED, do not trust as source
}
```

Notes:

- `Status` is a **derived cache** computed from `Last Shown`. Recomputed on every read by the dashboard. Stored only as a denormalized convenience. The single authoritative source for active/inactive is `today - Last Shown ≤ 7 days`.
- `Regions` is an array, never a scalar. Canonical region is index 0.
- `Creative ID` is the dedup key together with `Platform`. Region is **not** part of the key.
- `New This Week` is computed at read time as `first_seen_batch_id == current_batch_id`. Not stored.
- `Scrape Batch ID` field from v1 is replaced by `first_seen_batch_id` and `last_seen_batch_id` for proper temporal tracking.

### 4.6 File layout

```
/Google Ads competitors/
├── run_weekly.py              ← orchestrator, CLI entry point
├── safety_check.py            ← preconditions, runs before any network call
├── config.py                  ← competitor list, thresholds, hard caps
├── scrapers/
│   ├── firecrawl_google.py    ← FireCrawl-first Google scraper
│   ├── apify_google.py        ← crawlerbros fallback, used per §4.3.2
│   ├── apify_meta.py          ← per-competitor Meta runs
│   └── vision_filter.py       ← OpenAI vision (Cash App + preview check)
├── pipeline/
│   ├── merge.py               ← 2-phase atomic merge
│   ├── preview_validator.py   ← post-merge integrity check (§4.10)
│   └── recovery.py            ← CDN-based recovery (§10.1)
├── .env                       ← API keys
├── public/
│   ├── dashboard.html         ← single-file vanilla JS dashboard
│   ├── ads_data.js            ← SINGLE SOURCE OF TRUTH
│   ├── meta_images/           ← Meta thumbnails
│   └── google_videos/         ← legacy MP4s (preserved, not extended)
├── staging/                   ← per-batch scraper outputs, gitignored
├── backups/                   ← timestamped gzipped SoT snapshots
├── logs/                      ← structured run logs
├── metrics/                   ← per-run row counts for anomaly detection
├── manifest.json              ← latest batch_id, sha256, row counts
├── api/                       ← Vercel serverless (analyze.js etc.)
└── vercel.json
```

Files **deleted** from v1 layout: `ads_data.js` at repo root, `ads_data.json` backup at repo root, `_phase1_backfill.py`, `_phase3_global_rescrape.py`. The phase scripts must not exist in the repo; their logic is folded into `run_weekly.py` with proper guardrails. If the agent finds them, it stops and asks before proceeding.

### 4.7 Invariants (binding)

These hold at all times. Violations are P0.

| # | Invariant |
|---|---|
| **I1** | `public/ads_data.js` is append-only within a run. No code path may remove a row except via the explicit `retire` operation in I6. |
| **I2** | Every row MUST have a non-empty `Creative ID`. Rows without one are rejected at merge time, logged, and not written. |
| **I3** | Dedup key is `Platform + Creative ID`. Region is **not** part of the key. A creative seen in multiple regions produces one row with `Regions[]` populated. |
| **I4** | `Last Shown` is set ONLY when a scrape successfully observes the ad in the current batch. It is never copied forward, backfilled, or interpolated. |
| **I5** | `Status` is derived from `Last Shown` at read time. The stored value is a cache and MUST NOT be trusted by any consumer other than the UI. |
| **I6** | An ad MAY be marked `retired=true` (NOT deleted) only if (a) the source platform explicitly returns "ad removed," OR (b) the ad is a format we cannot render (e.g., Google HTML5 bundle) AND the format is in `config.RETIREABLE_FORMATS`. Retirement is logged with reason and batch ID. |
| **I7** | `batch_id` format: `{pipeline}_{YYYYMMDD}_{HHMMSS}_{uuid4[:8]}`. Globally unique per run. Every row touched in a run gets a `last_seen_batch_id` update. |
| **I8** | `schema_version` is an integer on every row. Migrations are explicit, numbered, idempotent, and version-bump the entire file. |
| **I9** | `public/ads_data.js` writes are atomic via 2-phase commit (§4.8). Partial writes are impossible by construction. |
| **I10** | The pipeline never writes to `public/ads_data.js` if any postcondition in §4.8 fails. The write is rolled back via `mv`. |

### 4.8 Pipeline execution contract (binding)

**Preconditions** (checked by `safety_check.py` before any network call):

| # | Precondition |
|---|---|
| P1 | `public/ads_data.js` parses as valid JS and contains ≥ 90% of the row count of the previous successful run (per `manifest.json`). |
| P2 | A timestamped backup at `backups/ads_data_{prev_batch_id}.js.gz` exists with SHA256 matching the current `public/ads_data.js`. If absent, the pipeline creates one and proceeds. |
| P3 | All required env vars present: `APIFY_TOKEN`, `FIRECRAWL_KEY`, `OPENAI_KEY`. |
| P4 | Apify account balance ≥ $5.00, queried via `GET /v2/users/me`. |
| P5 | Lock file `/tmp/tabby_scraper.lock` does not exist. On run start, create it with PID; on exit, remove it. |
| P6 | **Approval token present.** A file `/tmp/tabby_approval_{date}.token` containing a fresh approval string from the human exists and is dated today (GST). Without this, the pipeline aborts at the orchestrator level even before `safety_check.py` runs. (See §4.9.) |

**Per-scraper contract:**

| # | Contract |
|---|---|
| C1 | Each scraper returns `{ok: bool, rows: [...], stats: {...}, errors: [...]}`. Never raises. |
| C2 | A scraper run is `healthy` iff `ok=True` AND `len(rows) ≥ 0.5 × historical_median` for that competitor over a rolling 4-week window. Historical medians are stored in `metrics/medians.json`. |
| C3 | An unhealthy scraper does NOT trigger a merge. Its rows are written to `staging/` for manual review. Existing rows for that competitor are untouched. |
| C4 | Merge is 2-phase: write `public/ads_data.js.tmp` → `fsync` → atomic `os.rename` to `public/ads_data.js`. |
| C5 | Cost tracking: each scraper invocation logs estimated cost to `logs/cost_{batch_id}.json`. The orchestrator aborts if cumulative estimated cost exceeds `--max-cost-usd`. |

**Postconditions** (checked after merge, before declaring success):

| # | Postcondition |
|---|---|
| Q1 | Row count after merge ≥ row count before merge (monotonic growth). |
| Q2 | Every row from the previous file still exists by `(Platform, Creative ID)` dedup key. |
| Q3 | SHA256 of new `public/ads_data.js` is written to `manifest.json` with the batch ID. |
| Q4 | Smoke test: query 5 random rows and confirm their `Local Image` / `Local Video` files exist on disk. |
| Q5 | Preview validator (§4.10) ran and recorded `preview_status` for every row touched in this batch. |
| Q6 | On ANY postcondition failure: `mv public/ads_data.js public/ads_data.js.failed && mv backups/ads_data_{prev_batch_id}.js.gz public/ads_data.js` and exit non-zero. |

### 4.9 Scheduling and approval gate

**Schedule.** A GitHub Actions workflow (`.github/workflows/weekly.yml`) runs every Monday 06:00 GST (Sunday 02:00 UTC). The workflow does **not** start the scraper. It performs three things:

1. Posts a Slack notification (`#tabby-acquisition-alerts`): "Weekly scraper window is open. Reply `approve` in the chat with Claude to start."
2. Creates an empty file `/tmp/tabby_approval_pending_{date}.token` to indicate the window is open.
3. Logs the open window to `logs/schedule.json`.

The actual scrape **only** runs when Ilya, in a chat session, types something equivalent to "approved" or "go ahead." On approval, Claude (or the executing agent) writes the approval token to `/tmp/tabby_approval_{date}.token` and invokes `python run_weekly.py --mode=live --max-cost-usd=2.00`. Without this token, every entry point exits immediately with: `ERROR: no approval token for {date}. Run aborted.`

**Why this design.** It prevents the failure mode where (a) the agent runs an expensive job because "weekly cron fired" without the human present to monitor, or (b) the human approved a run a week ago and the agent assumes that approval persists. Approval is per-day and per-run.

**Manual override.** Ilya can still run `python run_weekly.py --mode=dry` at any time without an approval token. Dry mode performs all preconditions, all scraper invocations against test fixtures, and a no-op merge. It costs $0 and writes nothing to `public/ads_data.js`.

### 4.10 Preview integrity validator (binding)

After every merge, `pipeline/preview_validator.py` runs against rows touched in the current batch. Its job is to answer one question: **"Will this preview actually render in the dashboard?"**

The validator runs three checks per row, in order, stopping at the first conclusive answer:

| Check | Logic | Outcome |
|---|---|---|
| **Check 1: Local file** | If `Local Image` or `Local Video` is set, verify the file exists on disk and is > 1 KB. | `ok` if both true; `broken` if file missing or zero-byte. |
| **Check 2: Remote HEAD** | If no local file, send an HTTP HEAD to `Image URL` or `Video URL`. Accept HTTP 200 with `Content-Type` matching `image/*` or `video/*` or `text/html` (YouTube). | `ok` on 200 with valid type; `broken` on 4xx/5xx; `missing` on connection error or no URL at all. |
| **Check 3: Vision sample** | For 5% of rows newly added in this batch (random sample), download the asset and pass to `gpt-4o-mini` Vision with the prompt: *"Is this a renderable advertising image / video frame, or is it an error page, blank image, or unrelated content? Answer one word: RENDERABLE, ERROR, BLANK, UNRELATED."* | `ok` on RENDERABLE; `broken` otherwise. Hard cap of 100 vision calls per run to control cost. |

Each row gets `preview_status` ∈ {`ok`, `missing`, `broken`, `unverified`} and `preview_checked_at`.

**If a row's preview fails Check 1 or Check 2, the validator MUST attempt to find out why:**

1. For `missing` (no URL): re-query the source scraper for that single creative. If still empty, mark `unverified` and add to `logs/preview_misses_{batch_id}.json`.
2. For `broken` (HTTP error or wrong content-type): try the alternate field (e.g., if `Image URL` is broken, try `Video URL`; if both broken, try `Ad Preview URL`). If all alternatives fail, mark `broken` and log.
3. For Meta `fbcdn` URLs that 403 (typical fbcdn expiry): if `Local Image` is missing on disk, re-trigger the local download. If the download itself fails, mark `broken`.

The dashboard **MUST** filter or visually flag rows with `preview_status` ∈ {`broken`, `missing`} so they don't appear as "successful ads with invisible previews." A toggle in the UI lets Ilya see broken rows for debugging.

**Run summary** is posted to Slack:
```
Weekly run weekly_20260420_060000_a1b2c3d4 complete.
  New ads:           +47
  Newly inactive:    +12
  Preview status:    2,401 ok / 31 broken / 8 missing / 73 unverified
  Cost:              $1.18 / $2.00 cap
  Broken previews:   logs/preview_misses_weekly_20260420_060000_a1b2c3d4.json
```

---

## 5. Cost analysis

### 5.1 Steady-state weekly cost (projected, post-cleanup)

| Operation | Provider | Cost | Notes |
|---|---|---|---|
| Meta scrape (per-competitor, ~1,900 ads) | Apify `curious_coder` | ~$1.43 | 1,900 × $0.00075 |
| Google scrape (regional advertisers) | FireCrawl free tier | $0.00 | ≤ 80 pages |
| Google scrape (Global fallback, 5 advertisers × 200 ads) | Apify `crawlerbros` | ~$0.70 | 1,000 × $0.0007. Only if FireCrawl coverage < 70%. |
| OpenAI Vision (Cash App filter) | OpenAI | ~$0.02 | ~100 images |
| OpenAI Vision (preview validator sample) | OpenAI | ~$0.02 | ≤ 100 calls hard-capped |
| **Total steady-state weekly** | | **~$2.17** | |
| **Total monthly** | | **~$8.70** | |

This fits the $10 free Apify tier, **if** FireCrawl is healthy and the fallback is small. If FireCrawl is broken (current state), worst-case fallback cost is `16 advertisers × 200 ads × $0.0007 = $2.24/run = ~$9/month` from `crawlerbros`. Still under the $29 paid tier with margin.

### 5.2 Hard caps (enforced in code, not policy)

| Cap | Value | Enforcement point |
|---|---|---|
| `MAX_RUN_COST_USD` | 2.00 | Orchestrator aborts when cumulative estimated cost exceeds. |
| `MAX_FALLBACK_COMPETITORS` | 8 | If FireCrawl fallback list is larger than this, abort and ask the human. Indicates FireCrawl is broken, not a "few advertisers needed help." |
| `MAX_FIRECRAWL_PAGES_PER_RUN` | 100 | Hard limit. |
| `MAX_VISION_CALLS_PER_RUN` | 500 | Hard limit. |
| `MAX_RETRIES_PER_API_CALL` | 2 | Idempotent. Stores Apify `run_id` before calling; on retry, fetches the existing dataset rather than starting a new run. |

### 5.3 Cost-spend audit trail

Every paid call writes to `logs/cost_{batch_id}.json`:
```json
{
  "batch_id": "weekly_20260420_060000_a1b2c3d4",
  "calls": [
    {"timestamp": "...", "provider": "apify", "actor": "curious_coder/...", "purpose": "Meta scrape Wise", "estimated_cost_usd": 0.58, "run_id": "abc123"},
    ...
  ],
  "total_estimated_usd": 1.18,
  "cap_usd": 2.00
}
```

A daily summary is committed to `metrics/cost_history.csv` for trend analysis.

---

## 6. Known issues and limitations

### 6.1 Critical (P0)

1. **613 Global Google ads must be recovered** before the next scrape via §10.1.
2. **`experthasan` actor is permanently retired** from the architecture. The `crawlerbros` actor is its replacement, with the documented MP4 regression in §4.3.3.
3. **FireCrawl is currently timing out** on Google Transparency Center listing pages. Diagnose via §10.2 before relying on the FireCrawl-first design in production. If FireCrawl is permanently broken for `adstransparency.google.com`, the architecture falls back gracefully to `crawlerbros` for all Google scraping, at ~$9/month — still affordable.

### 6.2 High (P1)

4. **No Google video MP4 capture for new ads.** Documented trade-off (§4.3.3). YouTube-hosted videos render via embed; non-YouTube videos render as static thumbnail or are flagged `preview_status=missing`.
5. **Cash App HTML5 rich-media ads** are not capturable by any scraper. They are marked `retired=true, retired_reason="format_unsupported_html5"` per Invariant I6.
6. **Klarna advertiser ID was wrong** in earlier runs (Klarna AB Swedish parent). Fixed to `AR05325035143755202561` (Klarna INC US) in §4.4. Will be applied during recovery.

### 6.3 Medium (P2)

7. **`ads_data.js` size will grow.** At 5 MB today, ~10 MB within 6 months given retention. Migration plan: when file exceeds 8 MB OR row count exceeds 8,000, migrate to `/api/ads?page=N` paginated endpoint backed by `public/ads_data_chunks/`. Hard threshold, not aspirational.
8. **No OCR** on images. Deferred. When added, runs as a separate offline batch process so it doesn't block weekly scrapes.
9. **Meta scraper writing to Google Sheets** as a legacy path was removed in v1 cleanup. Confirm gone. If still present, delete it.

### 6.4 Low (P3)

10. No sentiment / theme analysis.
11. No alerting on competitor ad surges.
12. No multi-account Apify parallelization.

---

## 7. Roadmap

### 7.1 Immediate (this week)

1. Run recovery procedure §10.1 to restore the 613 deleted Global Google ads.
2. Replace `experthasan` references in code with `crawlerbros`. Verify against §4.3.
3. Diagnose FireCrawl timeout (§10.2). If unrecoverable, document and fall through to `crawlerbros` as default.
4. Implement `safety_check.py`, `pipeline/merge.py`, `pipeline/preview_validator.py`, `pipeline/recovery.py`.
5. Commit the current `public/ads_data.js` to git as a tagged checkpoint `pre-recovery-2026-04-09`. Free, simple, instant rollback.
6. Delete `_phase*.py` scripts. Their behavior is now folded into `run_weekly.py` with guardrails.

### 7.2 Short-term (next 2 weeks)

7. Set up GitHub Actions workflow per §4.9 (notification + token, **not** auto-execute).
8. Split Meta scraper into per-competitor runs per §4.3.1.
9. Build the preview validator and wire the dashboard to filter `broken` / `missing` rows.
10. End-to-end smoke tests on a frozen fixture so we can validate scraper changes without spending money.

### 7.3 Medium-term (next month)

11. Migrate `public/ads_data.js` to paginated API endpoint when threshold hit.
12. Add `apify/screenshot-url` capture for Google video ads where `videoUrl` is non-YouTube and no Local Video exists. Costs ~$0.001 per screenshot, runs only on new ads.
13. Apply OpenAI Vision brand-filter to all Global competitors, not just Cash App, to catch mis-attribution.
14. Build "Ad Change Feed" view: weekly diff of new + newly inactive ads per competitor.

### 7.4 Long-term

15. OCR extraction of ad copy (Google Cloud Vision or AWS Textract).
16. Optional spend estimation via BigQuery Google Ads Transparency public dataset.
17. Slack alerting for competitor ad surges (≥ N new ads/day).

---

## 8. Open questions for CTO

1. **Budget ceiling.** Is the $10 free Apify tier the target, or is $29/month acceptable for FireCrawl-failure resilience?
2. **Data ownership.** Should the SoT migrate from `public/ads_data.js` to BigQuery / Postgres in the medium term, given Tabby's existing data infrastructure?
3. **Privacy.** Is `noindex` + direct-link sufficient, or should we add Vercel password protection or SSO?
4. **Scope.** Add more competitors? Drop low-volume ones (D360 Bank: 1 ad)?
5. **Ownership.** Who maintains this if Ilya is unavailable? Is there a runbook for non-engineering staff?
6. **Approval gate UX.** Is the per-day approval token model in §4.9 the right friction level, or should it be weekly with override?

---

## 9. Glossary

- **Creative ID (CR...)**: Google Transparency Center unique ID per ad creative. Globally unique.
- **Advertiser ID (AR...)**: Google Transparency Center unique ID per legal advertiser entity.
- **Simgad**: Google-hosted thumbnail image URL format (`tpc.googlesyndication.com/archive/simgad/...`).
- **Ytimg**: YouTube-hosted video thumbnail (`i.ytimg.com/vi/.../hqdefault.jpg`).
- **fbcdn**: Facebook CDN URL for Meta ad images. Expires within hours; must download locally on observation.
- **HTML5 rich-media**: Google interactive ad format using JavaScript bundles. Cannot be scraped or rendered outside Google's origin.
- **Approval token**: A timestamped file at `/tmp/tabby_approval_{date}.token` that gates pipeline execution. See §4.9.
- **Batch ID**: Unique run identifier in the format `{pipeline}_{YYYYMMDD}_{HHMMSS}_{uuid4[:8]}`.

---

## 10. Operational runbooks

### 10.1 Recovery procedure for the 613 deleted Global Google ads

Execute exactly. Do not skip steps. Do not call any scraper before this completes.

```
1. Acquire lock:
     touch /tmp/tabby_scraper.lock
   If file already exists, abort and investigate.

2. Back up current state:
     gzip -c public/ads_data.js > backups/pre_recovery_$(date +%s).js.gz

3. Fetch the live Vercel CDN copy:
     curl -s https://tabby-ad-intelligence.vercel.app/ads_data.js \
       -o /tmp/recovered.js
   Verify size > 1MB. If smaller, the CDN was already refreshed; STOP and escalate.

4. Parse both files (Python):
     current = parse(public/ads_data.js)
     recovered = parse(/tmp/recovered.js)

5. Compute diff:
     current_keys = {(r.Platform, r['Creative ID']) for r in current}
     recovered_only = [r for r in recovered
                       if (r.Platform, r['Creative ID']) not in current_keys
                       and r.Platform == 'Google Ads'
                       and r['Competitor Name'] in
                           {'Klarna','Wise','Monzo','Cash App','Revolut'}]

6. Sanity check the count:
     assert 500 <= len(recovered_only) <= 800, \
       f"Expected ~613, got {len(recovered_only)}. STOP."

7. For each row in recovered_only:
     row['last_seen_batch_id'] = f'recovery_{batch_id}'
     row['source_actor'] = row.get('source_actor', 'recovered_from_cdn')
     row['preview_status'] = 'unverified'
     # Recompute Status from Last Shown
     row['Status'] = 'Active' if (today - parse(row['Last Shown'])).days <= 7 else 'Inactive'

8. Merge into current via 2-phase commit:
     merged = current + recovered_only
     write merged to public/ads_data.js.tmp
     fsync
     os.rename('public/ads_data.js.tmp', 'public/ads_data.js')

9. Run smoke test (Postcondition Q4) on 5 random recovered rows.

10. Run preview validator (§4.10) on the recovered batch. Expect many
    'unverified' or 'broken' for old fbcdn URLs. This is fine; flag them
    and let the next live scrape refresh.

11. Commit:
      git add public/ads_data.js manifest.json
      git commit -m "recovery: restore 613 Global Google ads wiped by
                     failed Phase 3 rescrape on 2026-04-09"
      git tag post-recovery-$(date +%Y%m%d)

12. Release lock:
      rm /tmp/tabby_scraper.lock

13. Only AFTER successful recovery, consider whether to re-run a live
    scrape. Wait for approval per §4.9.
```

### 10.2 FireCrawl timeout diagnostic

```
1. Reproduce in isolation:
     curl -X POST https://api.firecrawl.dev/v2/scrape \
       -H "Authorization: Bearer $FIRECRAWL_KEY" \
       -d '{"url":"https://adstransparency.google.com/advertiser/AR02766979019476566017"}'

2. Time the response. If > 60s and no body, the issue is server-side.

3. Try a non-Google URL of similar complexity to rule out account-level issues:
     curl ... -d '{"url":"https://example.com"}'

4. Try an older FireCrawl scrape format if v2 is broken:
     curl -X POST https://api.firecrawl.dev/v1/scrape ...

5. If FireCrawl is genuinely broken for adstransparency.google.com:
     a. Set FIRECRAWL_ENABLED = False in config.py
     b. Set DEFAULT_GOOGLE_SCRAPER = "crawlerbros" in config.py
     c. Update §6.1 entry 3 to "permanently broken"
     d. Recompute steady-state cost (~$9/month from crawlerbros)
     e. Continue. The architecture handles this case.

6. If FireCrawl works for non-Google URLs but not adstransparency.google.com,
   open a support ticket and proceed in fallback mode.
```

### 10.3 Adding a new competitor

```
1. Find the competitor on adstransparency.google.com. Copy the AR... ID.
2. Find the competitor's Facebook page ID via Graph API or page source.
3. Add to config.py COMPETITORS list with both IDs and a category.
4. Run python run_weekly.py --mode=dry --competitors=NewName to validate
   scrapers can find them.
5. If dry run succeeds, request approval and run live with --competitors=NewName.
6. Verify in dashboard. Commit config.py change.
```

### 10.4 Adding a new field to the schema

```
1. Bump SCHEMA_VERSION in config.py (e.g., 2 → 3).
2. Write a migration in pipeline/migrations/v2_to_v3.py that:
     - reads every row
     - adds the new field with a default
     - sets schema_version = 3
3. Run python -m pipeline.migrations.v2_to_v3 --dry-run.
4. Backup, then run live.
5. Update §4.5 in this PRD with the new field.
6. Update dashboard.html to read the new field with a fallback for safety.
```

---

## 11. Change log

### v2.0 — 2026-04-09

- **Replaced** `experthasan/google-ads-transparency-api` with `crawlerbros/google-ads-scraper` after `experthasan` went into maintenance. Documented the YouTube-only video URL regression in §4.3.3.
- **Reframed** Google scraping as FireCrawl-first with conditional Apify fallback per §4.3.2. Apify Google is no longer called for advertisers FireCrawl handled successfully.
- **Added** per-day approval token gate in §4.9. Pipeline does not run without explicit human approval, even on schedule.
- **Added** preview integrity validator in §4.10. Every row gets a `preview_status` field; broken previews are detected, investigated, and surfaced.
- **Added** binding invariants I1–I10 in §4.7 with code-level enforcement, replacing the prose-only "never delete ads" rule from v1.
- **Added** pipeline execution contract §4.8 with preconditions, per-scraper contracts, postconditions, and rollback semantics.
- **Added** hard cost caps in §5.2 enforced in code, not policy.
- **Added** schema fields: `schema_version`, `Regions[]`, `first_seen_batch_id`, `last_seen_batch_id`, `source_actor`, `retired`, `retired_reason`, `preview_status`, `preview_checked_at`. Removed `Scrape Batch ID`, `New This Week` (now derived).
- **Added** per-competitor Meta batching in §4.3.1 to eliminate single-point-of-failure on the 13-page Meta run.
- **Added** runbooks: §10.1 recovery, §10.2 FireCrawl diagnostic, §10.3 add competitor, §10.4 schema migration.
- **Deleted** files from layout: `_phase1_backfill.py`, `_phase3_global_rescrape.py`, root `ads_data.js`, root `ads_data.json`. The dangerous phase scripts no longer exist.
- **Added** Rule 0 (approval gate) and Rule 1 (safety check first) in §0 as overrides for the entire document.

### v1.0 — 2026-04-09 (earlier today)

Original document. See Appendix A in v1 for incident log.

---

## Appendix A: Incident log (preserved from v1)

| Time | Event |
|---|---|
| 13:00 | Klarna Meta = 0 inactive, wrong Wise previews, Revolut Meta = 0 reported. |
| 13:30 | Diagnosed: meta_scraper never set Status; Revolut FB page_id missing. |
| 14:00 | Fixed: added Revolut to META_PAGE_MAP, rewrote meta_scraper for incremental merge. |
| 15:30 | Meta rescrape run 1 succeeded (1,468 ads). |
| 16:00 | Wise video previews wrong (simgad URLs from old broken walker). |
| 16:30 | Cleared 204 ytimg + 231 suspect simgad URLs. |
| 17:00 | meta_scraper overwrote Local Video mappings — recovered from disk. |
| 17:30 | Fixed meta_scraper bug: load from `public/ads_data.js` not stale `ads_data.json`. |
| 18:00 | Meta rescrape run 2 succeeded (1,913 ads). |
| 18:30 | Klarna Google = 3 ads (should be hundreds). |
| 19:00 | Root cause: wrong advertiser ID (Klarna AB Swedish, needed Klarna INC US). |
| 19:30 | Wrote `_phase3_global_rescrape.py` using `experthasan` domain search. |
| 20:00 | Klarna US test → 400 ads returned, design validated. |
| 20:30 | Phase 3 ran: deleted 613 old Global Google ads → failed on `maxPages > 10` validation. |
| 20:45 | Fixed maxPages → re-run → `experthasan` returning HTTP 500 on every call. |
| 21:00 | Confirmed actor is broken (3 retries, all 500). |
| 21:15 | "You're burning my money. Ask first." |
| 21:20 | PRD v1 written. |
| 21:45 | `experthasan` confirmed under maintenance (Apify console banner screenshot). |
| 22:00 | PRD v2 written: replaced actor, added approval gate, invariants, preview validator. |
