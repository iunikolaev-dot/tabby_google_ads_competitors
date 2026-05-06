"""
config.py — v2 competitor list, thresholds, and hard caps.

This file is the single source of configuration for the v2 pipeline.
Every value here is referenced by `safety_check.py`, `pipeline/merge.py`,
`pipeline/recovery.py`, and the scrapers under `scrapers/`.

IMPORTANT: Editing any value in this file can change pipeline behavior
or cost. Changes to competitor advertiser IDs MUST be verified against
adstransparency.google.com before merging.

Cross-references to PRD v2 (v2/PRD_v2.md):
    §4.3   Tool responsibilities (cost contracts)
    §4.4   Competitor configuration table
    §4.5   Data model / schema version
    §4.7   Invariants I1-I10
    §5.2   Hard caps
    §10.2  FireCrawl waitFor behavior
"""

from __future__ import annotations

import os
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# 1. Schema version
# ─────────────────────────────────────────────────────────────────────────────
# Bump this integer when the row shape in public/ads_data.js changes.
# Every new row written by the pipeline gets `schema_version = SCHEMA_VERSION`.
# Migrations live under pipeline/migrations/ and are numbered v{N}_to_v{N+1}.
SCHEMA_VERSION: int = 2


# ─────────────────────────────────────────────────────────────────────────────
# 2. Filesystem paths (absolute, rooted at this file's parent)
# ─────────────────────────────────────────────────────────────────────────────
REPO_ROOT: Path = Path(__file__).resolve().parent

# Single source of truth. Any other ads_data.* file is forbidden (PRD §4.2).
SOT_PATH: Path = REPO_ROOT / "public" / "ads_data.js"

# Temporary file used by the 2-phase merge. `os.rename` swaps it into place.
SOT_TMP_PATH: Path = REPO_ROOT / "public" / "ads_data.js.tmp"

# Rolled-back file if a postcondition fails mid-merge.
SOT_FAILED_PATH: Path = REPO_ROOT / "public" / "ads_data.js.failed"

# Timestamped gzipped snapshots, one per successful batch.
BACKUPS_DIR: Path = REPO_ROOT / "backups"

# Per-batch raw scraper outputs before they are merged into the SoT.
STAGING_DIR: Path = REPO_ROOT / "staging"

# Structured JSON logs: run logs, cost logs, preview miss lists.
LOGS_DIR: Path = REPO_ROOT / "logs"

# Rolling metrics: historical medians per competitor, cost history CSV.
METRICS_DIR: Path = REPO_ROOT / "metrics"

# manifest.json tracks latest batch_id, sha256, and row counts.
MANIFEST_PATH: Path = REPO_ROOT / "manifest.json"

# Lockfile preventing concurrent runs (PRD precondition P5).
LOCK_PATH: Path = Path("/tmp/tabby_scraper.lock")

# Approval token path. A file at this location (with today's date in the name)
# gates pipeline execution per PRD §4.9. Created when the human says "approved".
def approval_token_path(date_str: str) -> Path:
    """Returns the expected approval token path for a given date (YYYYMMDD)."""
    return Path(f"/tmp/tabby_approval_{date_str}.token")


# ─────────────────────────────────────────────────────────────────────────────
# 3. Competitors (PRD §4.4)
# ─────────────────────────────────────────────────────────────────────────────
# Each competitor has:
#   - name:                canonical display name, also dedup key component
#   - category:            "Global" | "Regional"
#   - google_region:       canonical region code, or None if no Google tracking
#   - google_advertiser_ids: list (usually one, sometimes two like Rajhi Bank)
#   - meta_page_id:        Facebook page ID, or None if no Meta tracking
#   - notes:               free-text clarifications
#
# Region precedence rule (PRD §4.4 last para): if a Global competitor's ad is
# observed in a non-canonical region, it is MERGED into the same row with the
# new region appended to Regions[]. Not stored as a duplicate.

COMPETITORS: list[dict] = [
    # ── Global ──────────────────────────────────────────────────────────────
    {
        "name": "Klarna",
        "category": "Global",
        "google_region": "US",
        "google_advertiser_ids": ["AR05325035143755202561"],  # Klarna INC (US)
        "meta_page_id": "390926061079580",
        "notes": "Earlier used Klarna AB (Swedish). Correct ID is Klarna INC US.",
    },
    {
        "name": "Wise",
        "category": "Global",
        "google_region": "GB",
        "google_advertiser_ids": ["AR14378710480124379137"],  # Wise Payments Limited
        "meta_page_id": "116206531782887",
        "notes": "",
    },
    {
        "name": "Monzo",
        "category": "Global",
        "google_region": "GB",
        "google_advertiser_ids": ["AR07289389941828616193"],  # Monzo Bank Limited
        "meta_page_id": "113612035651775",
        "notes": "",
    },
    {
        "name": "Cash App",
        "category": "Global",
        "google_region": "US",
        "google_advertiser_ids": ["AR14896030700992987137"],  # Block, Inc.
        "meta_page_id": "888799511134149",
        "notes": "Requires OpenAI Vision brand filter — Block Inc. also runs Square and BitKey ads under the same advertiser ID.",
    },
    {
        "name": "Revolut",
        "category": "Global",
        "google_region": "GB",
        "google_advertiser_ids": ["AR07098428377224183809"],  # Revolut Ltd
        "meta_page_id": "335642513253333",
        "notes": "",
    },

    # ── Regional (GCC) ──────────────────────────────────────────────────────
    {
        "name": "Tamara",
        "category": "Regional",
        "google_region": "SA",
        "google_advertiser_ids": ["AR02766979019476566017"],
        "meta_page_id": "107593894218382",
        "notes": "",
    },
    {
        "name": "EmiratesNBD",
        "category": "Regional",
        "google_region": "AE",
        "google_advertiser_ids": ["AR11606100870541869057"],
        "meta_page_id": None,
        "notes": "",
    },
    {
        "name": "Al Rajhi Bank",
        "category": "Regional",
        "google_region": "SA",
        "google_advertiser_ids": [
            "AR07393135804576432129",
            "AR17149597601662763009",
        ],
        "meta_page_id": None,
        "notes": "Two advertiser IDs — both must be scraped and merged.",
    },
    {
        "name": "Ziina",
        "category": "Regional",
        "google_region": "AE",
        "google_advertiser_ids": ["AR06959610023805796353"],
        "meta_page_id": None,
        "notes": "",
    },
    {
        "name": "Tiqmo",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "105245002169048",
        "notes": "Meta only.",
    },
    {
        "name": "D360 Bank",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "100238958486269",
        "notes": "Meta only. Low volume.",
    },
    {
        "name": "Barq",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "370543246139130",
        "notes": "",
    },
    {
        "name": "Wio Bank",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "102791935482897",
        "notes": "",
    },
    {
        "name": "STC Bank",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "141270813154032",
        "notes": "",
    },
    {
        "name": "HALA Payment",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "379823329174805",
        "notes": "",
    },
    {
        "name": "Alaan",
        "category": "Regional",
        "google_region": None,
        "google_advertiser_ids": [],
        "meta_page_id": "102701872367080",
        "notes": "",
    },
]

# The 5 Global Google competitors whose ads were deleted in the 2026-04-09
# incident. Used as a whitelist by pipeline/recovery.py.
GLOBAL_GOOGLE_COMPETITOR_NAMES: set[str] = {
    c["name"] for c in COMPETITORS
    if c["category"] == "Global" and c["google_advertiser_ids"]
}


# ─────────────────────────────────────────────────────────────────────────────
# 4. Scraper selection
# ─────────────────────────────────────────────────────────────────────────────
# FireCrawl-first rule (PRD §4.3.2). Flip these if FireCrawl breaks permanently.
FIRECRAWL_ENABLED: bool = True
DEFAULT_GOOGLE_SCRAPER: str = "firecrawl"  # "firecrawl" | "crawlerbros"

# FireCrawl request parameters. waitFor=10000 is critical — 5000 (the v1 value)
# was insufficient for Google Transparency Center's React hydration and
# caused the intermittent 60s server-side timeouts observed on 2026-04-08/09.
FIRECRAWL_WAIT_FOR_MS: int = 10_000
FIRECRAWL_FORMATS: list[str] = ["html", "markdown"]
FIRECRAWL_TIMEOUT_SECONDS: int = 120

# Coverage threshold for the FireCrawl-first fallback rule (PRD §4.3.2).
# If FireCrawl returns rows for an advertiser but < this fraction have a
# non-empty preview URL, the advertiser is added to the Apify fallback list.
FIRECRAWL_MIN_PREVIEW_COVERAGE: float = 0.70

# Apify actors.
APIFY_META_ACTOR: str = "curious_coder/facebook-ads-library-scraper"
APIFY_GOOGLE_ACTOR: str = "crawlerbros/google-ads-scraper"

# ── Ingestion rules (hard filters) ──────────────────────────────────────
#
# Rule 1: HTML5 rich-media ads are REJECTED on ingestion (never stored).
#   These render as interactive JS bundles (displayads-formats.googleusercontent.com
#   / content.js / sadbundle) with no static thumbnail. No scraper can fix this.
#   If any slip through, pipeline/merge.py deletes them on the next merge.
#
# Rule 2: When using crawlerbros (Apify Google scraper), EXCLUDE format=IMAGE.
#   Reason: IMAGE-format results from the Transparency Center are mostly HTML5
#   bundles mislabeled as IMAGE, with previewUrl pointing to content.js. The
#   useful thumbnails (simgad) are already captured by FireCrawl's listing page.
#   Only VIDEO and TEXT formats are requested from crawlerbros.
GOOGLE_REJECTED_FORMATS: set[str] = {"TEXT"}  # crawlerbros format filter
HTML5_REJECT_INDICATORS: tuple[str, ...] = (
    # Only `sadbundle` is a true HTML5 rich-media bundle indicator.
    # Removed `displayads-formats.googleusercontent.com` and `content.js`
    # on 2026-05-02: empirically those patterns appear in the JS-render
    # embed URL for EVERY legitimate Google VIDEO creative and many IMAGE
    # ones. Treating them as rejection signals dropped 100% of VIDEO ads
    # from crawlerbros output (323 rows lost in the 2026-05-02 Globals
    # merge before this fix). Keep the list narrow.
    "sadbundle",
)
# Legacy constant kept for any code that references it.
RETIREABLE_FORMATS: set[str] = {"html5_bundle", "rich_media_html5"}


# ─────────────────────────────────────────────────────────────────────────────
# 5. Hard caps (PRD §5.2) — enforced in code, not policy
# ─────────────────────────────────────────────────────────────────────────────
MAX_RUN_COST_USD: float = 2.00
MAX_FALLBACK_COMPETITORS: int = 8       # > this → FireCrawl is broken, abort
MAX_FIRECRAWL_PAGES_PER_RUN: int = 100
MAX_VISION_CALLS_PER_RUN: int = 500
MAX_RETRIES_PER_API_CALL: int = 2
APIFY_MIN_BALANCE_USD: float = 5.00     # Precondition P4
APIFY_MONTHLY_BUDGET_USD: float = 29.00 # Plan hard cap
APIFY_ORG_USER_ID: str = "gKw51ox5Nq9w1qdft"
APIFY_USAGE_URL: str = "https://api.apify.com/v2/users/me/usage/monthly"
APIFY_LIMITS_URL: str = "https://api.apify.com/v2/users/me/limits"

# Row count sanity floor: new merged file must have ≥ this fraction of rows
# compared to the previous successful batch (Precondition P1 / Postcondition Q1).
SOT_MIN_ROW_COUNT_FRACTION: float = 0.90

# Per-scraper health check: a run is "healthy" iff rows ≥ this × rolling
# 4-week median for that competitor. Below → staging only, no merge (PRD C2).
SCRAPER_HEALTH_FRACTION: float = 0.50


# ─────────────────────────────────────────────────────────────────────────────
# 6. Environment variables
# ─────────────────────────────────────────────────────────────────────────────
# Keys are loaded from .env (not from this file). This dict just lists which
# ones are required, used by safety_check.py precondition P3.
REQUIRED_ENV_VARS: tuple[str, ...] = (
    "APIFY_TOKEN",
    "FIRECRAWL_KEY",   # a.k.a. FIRECRAWL_API_KEY in v1 — normalized here
    "OPENAI_KEY",      # a.k.a. OPENAI_API_KEY in v1
)

# v1 used slightly different env var names. We accept either to avoid forcing
# users to edit .env. safety_check.py resolves through this alias table.
ENV_VAR_ALIASES: dict[str, tuple[str, ...]] = {
    "APIFY_TOKEN":   ("APIFY_TOKEN", "APIFY_API_TOKEN"),
    "FIRECRAWL_KEY": ("FIRECRAWL_KEY", "FIRECRAWL_API_KEY"),
    "OPENAI_KEY":    ("OPENAI_KEY", "OPENAI_API_KEY"),
}


def resolve_env(name: str) -> str | None:
    """Return the first environment variable set from the alias list for `name`."""
    for alias in ENV_VAR_ALIASES.get(name, (name,)):
        val = os.environ.get(alias)
        if val:
            return val
    return None
