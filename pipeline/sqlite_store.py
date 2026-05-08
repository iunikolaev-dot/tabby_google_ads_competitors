"""
pipeline/sqlite_store.py — read/write helpers for data/ads.db.

The SQLite file is the new source of truth (audit step 5). The legacy
public/ads_data.js is still produced by run_weekly.py during phase 5.1
so the existing dashboard keeps working; phase 5.3 will drop it.

Schema lives in data/schema.sql. This module provides:

    open_db(path=None)         -> sqlite3.Connection (Row factory enabled)
    init_db(conn)              create tables/indexes if missing
    upsert_rows(conn, rows)    REPLACE-style upsert by composite PK
    fetch_all(conn) -> list[dict]   matches the legacy JSON row shape
    json_dict_to_row(d) -> dict     translate dashboard-style dict to DB row
    row_to_json_dict(row) -> dict   inverse — for backward-compat JSON dump

Why a thin module: avoid spreading SQL fragments across run_weekly.py and
api/ads.py. Both import from here.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parent.parent
DB_PATH = REPO / "data" / "ads.db"
SCHEMA_PATH = REPO / "data" / "schema.sql"


# ─── Field mapping ───────────────────────────────────────────────────────────
# Dashboard-style key (the legacy JSON dict) → SQLite column name.

JSON_TO_COLUMN: dict[str, str] = {
    "Platform": "platform",
    "Creative ID": "creative_id",
    "Region": "region",
    "Competitor Name": "competitor_name",
    "Competitor Website": "competitor_website",
    "Category": "category",
    "Advertiser ID": "advertiser_id",
    "Advertiser Name (Transparency Center)": "advertiser_name",
    "Ad Format": "ad_format",
    "Image URL": "image_url",
    "Video URL": "video_url",
    "Embed URL": "embed_url",
    "Ad Preview URL": "ad_preview_url",
    "Landing Page": "landing_page",
    "Landing Page / Destination URL": "landing_page",  # legacy key, same column
    "Screenshot": "screenshot",
    "Status": "status",
    "Started Running": "started_running",
    "First Shown": "started_running",                  # v2-only key, same column
    "Last Shown": "last_shown",
    "Date Collected": "date_collected",
    "New This Week": "new_this_week",
    "seen_in_batches": "seen_in_batches",
    "first_seen_batch_id": "first_seen_batch_id",
    "last_seen_batch_id": "last_seen_batch_id",
    "miss_streak": "miss_streak",
    "Scrape Batch ID": "scrape_batch_id",
    "schema_version": "schema_version",
    "source_actor": "source_actor",
    "preview_status": "preview_status",
    "preview_checked_at": "preview_checked_at",
    "retired": "retired",
    "retired_reason": "retired_reason",
}

COLUMN_TO_JSON: dict[str, str] = {
    # Output mapping — which JSON key the dashboard expects per column.
    # Where two JSON keys mapped onto one column, we pick the canonical key.
    "platform": "Platform",
    "creative_id": "Creative ID",
    "region": "Region",
    "competitor_name": "Competitor Name",
    "competitor_website": "Competitor Website",
    "category": "Category",
    "advertiser_id": "Advertiser ID",
    "advertiser_name": "Advertiser Name (Transparency Center)",
    "ad_format": "Ad Format",
    "image_url": "Image URL",
    "video_url": "Video URL",
    "embed_url": "Embed URL",
    "ad_preview_url": "Ad Preview URL",
    "landing_page": "Landing Page",
    "screenshot": "Screenshot",
    "status": "Status",
    "started_running": "Started Running",
    "last_shown": "Last Shown",
    "date_collected": "Date Collected",
    "new_this_week": "New This Week",
    "seen_in_batches": "seen_in_batches",
    "first_seen_batch_id": "first_seen_batch_id",
    "last_seen_batch_id": "last_seen_batch_id",
    "miss_streak": "miss_streak",
    "scrape_batch_id": "Scrape Batch ID",
    "schema_version": "schema_version",
    "source_actor": "source_actor",
    "preview_status": "preview_status",
    "preview_checked_at": "preview_checked_at",
    "retired": "retired",
    "retired_reason": "retired_reason",
    "regions_csv": "regions_csv",  # passthrough; dashboard ignores
}

INT_COLUMNS = {"seen_in_batches", "miss_streak", "schema_version", "retired"}


# ─── Conversion ──────────────────────────────────────────────────────────────

def json_dict_to_row(d: dict) -> dict:
    """Translate a dashboard-style dict (one row of ads_data.js) into a
    SQLite-column-keyed dict. Skips unknown keys silently."""
    row: dict = {}
    for k, v in d.items():
        col = JSON_TO_COLUMN.get(k)
        if col is None:
            continue
        if col in INT_COLUMNS:
            try:
                row[col] = int(v) if v not in (None, "") else 0
            except (TypeError, ValueError):
                row[col] = 0
            # `retired` may be Python bool → 0/1
            if col == "retired" and isinstance(v, bool):
                row[col] = 1 if v else 0
        else:
            row[col] = "" if v is None else str(v)
    # Normalize Regions[] (list) into regions_csv
    regs = d.get("Regions")
    if isinstance(regs, list) and regs:
        row["regions_csv"] = ", ".join(str(r) for r in regs if r)
    return row


def row_to_json_dict(row: sqlite3.Row | dict) -> dict:
    """Translate a SQLite row back into a dashboard-style dict."""
    if isinstance(row, sqlite3.Row):
        d = {k: row[k] for k in row.keys()}
    else:
        d = dict(row)
    out: dict = {}
    for col, json_key in COLUMN_TO_JSON.items():
        if col not in d:
            continue
        v = d[col]
        if col == "retired":
            out[json_key] = bool(v)
        elif col in INT_COLUMNS:
            out[json_key] = int(v) if v is not None else 0
        else:
            out[json_key] = "" if v is None else v
    # Re-expand regions_csv into Regions[] for dashboard convenience
    if d.get("regions_csv"):
        out["Regions"] = [s.strip() for s in d["regions_csv"].split(",") if s.strip()]
    return out


# ─── Connection / init ───────────────────────────────────────────────────────

def open_db(path: str | Path | None = None) -> sqlite3.Connection:
    p = Path(path) if path else DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text())
    conn.commit()


# ─── Bulk upsert ─────────────────────────────────────────────────────────────

# All columns excluding the PK trio. INSERT OR REPLACE writes the whole row,
# so non-PK columns get overwritten by the new value (this is the desired
# "merge" behavior — caller has already merged history fields).
_COLS = list(COLUMN_TO_JSON.keys())  # platform/creative_id/region first, then others
# Make sure PK is first three for clarity; sqlite doesn't care.

_INSERT_SQL = (
    "INSERT OR REPLACE INTO ads ("
    + ", ".join(_COLS)
    + ") VALUES ("
    + ", ".join(f":{c}" for c in _COLS)
    + ")"
)


def upsert_rows(conn: sqlite3.Connection, rows: Iterable[dict]) -> int:
    """Upsert dashboard-style dicts. Returns count written."""
    n = 0
    cur = conn.cursor()
    for d in rows:
        if not d.get("Creative ID") and not d.get("creative_id"):
            continue
        # Accept either dashboard-key dicts or already-translated dicts
        is_translated = "platform" in d and "Platform" not in d
        row = d if is_translated else json_dict_to_row(d)
        # Fill missing columns with defaults — sqlite's named params demand
        # every name be present.
        for col in _COLS:
            row.setdefault(col, 0 if col in INT_COLUMNS else "")
        cur.execute(_INSERT_SQL, row)
        n += 1
    conn.commit()
    return n


def fetch_all(conn: sqlite3.Connection) -> list[dict]:
    """Return every row as a dashboard-style dict, sorted by Last Shown desc."""
    cur = conn.execute(
        "SELECT * FROM ads ORDER BY last_shown DESC, creative_id"
    )
    return [row_to_json_dict(r) for r in cur.fetchall()]


def count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
