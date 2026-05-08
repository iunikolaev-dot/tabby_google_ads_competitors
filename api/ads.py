"""
api/ads.py — Vercel serverless function. Single endpoint:

    GET /api/ads?status=Active&competitor=Wise&...&limit=200&offset=0

Reads data/ads.db (committed by the build), returns one JSON payload that
the dashboard can render in a single round-trip:

    {
      "ads":         [...],   # paginated rows in dashboard JSON shape
      "total":       <int>,   # rows after filters (before pagination)
      "stats":       {...},   # unfiltered top-card counts
      "breakdown":   {...},   # unfiltered per-competitor counts
      "competitors": [...],   # dropdown values
      "page":        {"limit": ..., "offset": ...}
    }

This is phase 5.2 of the SQLite migration. The dashboard still uses the
legacy ads_data.js script tag — phase 5.3 will switch it over.
"""

from __future__ import annotations

import json
import sqlite3
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse


# ─── Paths ────────────────────────────────────────────────────────────────────

# Try a few candidate paths because Vercel's runtime layout for Python
# functions isn't always `<repo>/api/ads.py` at /var/task. We pick the
# first one that exists.
_HERE = Path(__file__).resolve()
_CANDIDATE_DBS = [
    _HERE.parent.parent / "data" / "ads.db",          # local + standard layout
    Path("/var/task/data/ads.db"),                     # Vercel canonical
    Path.cwd() / "data" / "ads.db",                    # fallback
    _HERE.parent / "data" / "ads.db",                  # if function bundles flat
]


def _resolve_db_path() -> Path | None:
    for p in _CANDIDATE_DBS:
        if p.exists():
            return p
    return None


# ─── Field mapping (kept in sync with pipeline/sqlite_store.py) ──────────────

COLUMN_TO_JSON = {
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
}

INT_COLUMNS = {"seen_in_batches", "miss_streak", "schema_version", "retired"}


def _row_to_json(row: sqlite3.Row) -> dict:
    out = {}
    for col, json_key in COLUMN_TO_JSON.items():
        if col not in row.keys():
            continue
        v = row[col]
        if col == "retired":
            out[json_key] = bool(v)
        elif col in INT_COLUMNS:
            out[json_key] = int(v) if v is not None else 0
        else:
            out[json_key] = "" if v is None else v
    if row["regions_csv"]:
        out["Regions"] = [s.strip() for s in row["regions_csv"].split(",") if s.strip()]
    return out


# ─── Query construction ──────────────────────────────────────────────────────

# Allowed sort columns (whitelist; user input never reaches SQL directly)
SORT_WHITELIST = {
    "last_shown": "last_shown",
    "seen_in_batches": "seen_in_batches",
    "started_running": "started_running",
    "date_collected": "date_collected",
}


def _qp(params: dict, name: str, default: str = "") -> str:
    v = params.get(name)
    return (v[0] if isinstance(v, list) else v) if v else default


def _qp_int(params: dict, name: str, default: int) -> int:
    try:
        return int(_qp(params, name, str(default)))
    except (TypeError, ValueError):
        return default


def build_filter_sql(params: dict) -> tuple[str, list]:
    """Build the WHERE clause + bind list from query params."""
    where: list[str] = []
    binds: list = []

    def add(col: str, val: str):
        if val:
            where.append(f"{col} = ?")
            binds.append(val)

    add("status", _qp(params, "status"))
    add("competitor_name", _qp(params, "competitor"))
    add("platform", _qp(params, "platform"))
    add("ad_format", _qp(params, "format"))
    add("region", _qp(params, "region"))
    add("category", _qp(params, "category"))

    date_from = _qp(params, "from")
    if date_from:
        where.append("last_shown >= ?")
        binds.append(date_from)
    date_to = _qp(params, "to")
    if date_to:
        where.append("last_shown <= ?")
        binds.append(date_to)

    search = _qp(params, "search")
    if search:
        like = f"%{search.lower()}%"
        where.append(
            "(LOWER(competitor_name) LIKE ? OR LOWER(advertiser_name) LIKE ? "
            "OR LOWER(landing_page) LIKE ? OR LOWER(image_url) LIKE ?)"
        )
        binds.extend([like, like, like, like])

    sql = (" WHERE " + " AND ".join(where)) if where else ""
    return sql, binds


# ─── Stats / breakdown (unfiltered) ──────────────────────────────────────────

def compute_stats(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
    active = cur.execute("SELECT COUNT(*) FROM ads WHERE status='Active'").fetchone()[0]
    inactive = total - active
    by_plat = dict(cur.execute(
        "SELECT platform, COUNT(*) FROM ads WHERE status='Active' GROUP BY platform"
    ).fetchall())
    by_fmt = dict(cur.execute(
        "SELECT ad_format, COUNT(*) FROM ads WHERE status='Active' GROUP BY ad_format"
    ).fetchall())
    new_this_week = cur.execute(
        "SELECT COUNT(*) FROM ads WHERE new_this_week='NEW' AND status='Active'"
    ).fetchone()[0]
    last_collected = cur.execute(
        "SELECT MAX(date_collected) FROM ads WHERE date_collected != ''"
    ).fetchone()[0] or ""

    return {
        "total": total,
        "active": active,
        "inactive": inactive,
        "google": by_plat.get("Google Ads", 0),
        "meta": by_plat.get("Meta Ads", 0),
        "video": by_fmt.get("Video", 0),
        "image": by_fmt.get("Image", 0),
        "new_this_week": new_this_week,
        "last_scraped": last_collected,
    }


def compute_breakdown(conn: sqlite3.Connection) -> list[dict]:
    """Per-(competitor, platform) counts of active ads, sorted by total desc."""
    rows = conn.execute(
        """
        SELECT competitor_name, platform, category, ad_format, COUNT(*) AS n
          FROM ads
         WHERE status = 'Active'
         GROUP BY competitor_name, platform, ad_format
        """
    ).fetchall()
    by_key: dict[tuple, dict] = {}
    for r in rows:
        key = (r["competitor_name"], r["platform"])
        slot = by_key.setdefault(key, {
            "name": r["competitor_name"],
            "platform": r["platform"],
            "category": r["category"] or "",
            "video": 0, "image": 0, "total": 0,
        })
        if r["ad_format"] == "Video":
            slot["video"] += r["n"]
        elif r["ad_format"] == "Image":
            slot["image"] += r["n"]
        slot["total"] += r["n"]
    return sorted(by_key.values(), key=lambda s: -s["total"])


def compute_competitors(conn: sqlite3.Connection) -> list[str]:
    return [
        r[0] for r in conn.execute(
            "SELECT DISTINCT competitor_name FROM ads "
            "WHERE competitor_name != '' ORDER BY competitor_name"
        ).fetchall()
    ]


# ─── Main query ──────────────────────────────────────────────────────────────

def query(params: dict) -> dict:
    db_path = _resolve_db_path()
    if db_path is None:
        # Diagnostic — list directories so we can see what got bundled.
        diag = {}
        for label, path in [("here", _HERE.parent), ("repo", _HERE.parent.parent),
                             ("var_task", Path("/var/task")), ("cwd", Path.cwd())]:
            try:
                diag[label] = {
                    "path": str(path),
                    "exists": path.exists(),
                    "ls": sorted([str(p.name) for p in path.iterdir()])[:20] if path.exists() else None,
                }
            except OSError as e:
                diag[label] = {"path": str(path), "error": str(e)}
        return {"error": "db not found", "tried": [str(p) for p in _CANDIDATE_DBS],
                "diag": diag, "ads": [], "stats": {}, "breakdown": [],
                "competitors": [], "total": 0, "page": {"limit": 0, "offset": 0}}

    # Vercel's serverless filesystem is read-only outside /tmp; sqlite3's
    # default mode tries to create -journal/-wal sidecars next to the .db
    # file and fails with "unable to open database file". Open the URI in
    # explicit read-only mode — no sidecars, no writes.
    conn = sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row

    where_sql, binds = build_filter_sql(params)

    # Total (filtered)
    total = conn.execute(f"SELECT COUNT(*) FROM ads{where_sql}", binds).fetchone()[0]

    # Sort whitelist (default last_shown desc)
    sort_key = _qp(params, "sort", "last_shown")
    sort_col = SORT_WHITELIST.get(sort_key, "last_shown")
    sort_dir = "DESC" if _qp(params, "dir", "desc").lower() != "asc" else "ASC"

    limit = max(1, min(_qp_int(params, "limit", 200), 500))
    offset = max(0, _qp_int(params, "offset", 0))

    # Tiebreaker on creative_id keeps pagination stable
    rows = conn.execute(
        f"SELECT * FROM ads{where_sql} "
        f"ORDER BY {sort_col} {sort_dir}, creative_id "
        f"LIMIT ? OFFSET ?",
        binds + [limit, offset],
    ).fetchall()

    payload = {
        "ads": [_row_to_json(r) for r in rows],
        "total": total,
        "stats": compute_stats(conn),
        "breakdown": compute_breakdown(conn),
        "competitors": compute_competitors(conn),
        "page": {"limit": limit, "offset": offset},
    }
    conn.close()
    return payload


# ─── HTTP handler (Vercel signature) ─────────────────────────────────────────

class handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 — Vercel expects this name
        try:
            qs = urlparse(self.path).query
            params = parse_qs(qs)
            payload = query(params)
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=60")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            err = json.dumps({"error": str(e)}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(err)))
            self.end_headers()
            self.wfile.write(err)

    def do_OPTIONS(self):  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
