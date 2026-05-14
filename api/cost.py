"""
api/cost.py — Cost Console for the tabby-ad-intelligence pipeline.

GET /api/cost

Returns one JSON payload that powers /cost.html. Single round-trip.

Two-source design (see docs/COST_CONSOLE.md §1, modeled on the
social-listening tool's pattern):

  1. Apify REST API — `/v2/actor-runs` + `/v2/users/me` — ground-truth
     cost per run (same number that ends up on the monthly invoice).
     This is THE number to trust.

  2. metrics/spend_history.jsonl — append-only ledger our scrapers
     write when they invoke a paid actor. Carries OUR metadata: which
     competitor, items fetched, batch_id, our estimated cost.

Join key: `run_id` (Apify's ID, which we already store in the ledger
via observability.record_spend()'s extra={"run_id": ...}).

If Apify returns a run we don't have in the ledger → tagged `external`:
some other scraper on the same Apify account.

Requires APIFY_TOKEN as a Vercel project env var. Without it, the
endpoint runs in degraded mode (ledger-only — no ground truth).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import urllib.parse
import urllib.request
from collections import defaultdict
from http.server import BaseHTTPRequestHandler
from pathlib import Path

# ─── Paths ──────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve()
_CANDIDATE_LEDGERS = [
    _HERE.parent.parent / "metrics" / "spend_history.jsonl",
    Path("/var/task/metrics/spend_history.jsonl"),
    Path.cwd() / "metrics" / "spend_history.jsonl",
]
APIFY_API = "https://api.apify.com/v2"


def _resolve_ledger() -> Path | None:
    for p in _CANDIDATE_LEDGERS:
        if p.exists():
            return p
    return None


# ─── Apify REST ─────────────────────────────────────────────────────────────

def _http_get(url: str, token: str, timeout: int = 20) -> dict | None:
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def list_apify_runs(token: str, since_iso: str, max_pages: int = 5) -> list[dict]:
    """List Apify runs since `since_iso` across the whole account.

    Pages backward in time (`desc=1`) until we cover the window.
    Returns a list of {run_id, actor_id, actor_name, status, started_at,
    finished_at, cost_usd} dicts.
    """
    out: list[dict] = []
    seen_actor_names: dict[str, str] = {}

    for offset in range(0, max_pages * 1000, 1000):
        url = f"{APIFY_API}/actor-runs?limit=1000&offset={offset}&desc=1"
        payload = _http_get(url, token)
        if not payload:
            break
        items = (payload.get("data", {}) or {}).get("items", []) or []
        if not items:
            break
        oldest_in_page = None
        for run in items:
            started = run.get("startedAt") or ""
            if started and started < since_iso:
                continue
            oldest_in_page = started if oldest_in_page is None else min(oldest_in_page, started)
            actor_id = run.get("actId") or run.get("actorId") or ""
            usage = run.get("usageTotalUsd")
            if usage is None:
                # Some runs report cost differently; try usage breakdown
                usage = (run.get("usage") or {}).get("ACTOR_COMPUTE_UNITS_USD") or 0
            out.append({
                "run_id": run.get("id") or "",
                "actor_id": actor_id,
                "actor_name": "",  # resolved below
                "status": run.get("status") or "",
                "started_at": started,
                "finished_at": run.get("finishedAt") or "",
                "cost_usd": float(usage or 0),
            })
        # If we've paged back past `since_iso`, stop.
        if oldest_in_page and oldest_in_page < since_iso:
            break
        if len(items) < 1000:
            break

    # Resolve actor names (cached)
    for r in out:
        aid = r["actor_id"]
        if not aid:
            continue
        if aid in seen_actor_names:
            r["actor_name"] = seen_actor_names[aid]
            continue
        info = _http_get(f"{APIFY_API}/acts/{aid}", token)
        name = ""
        if info:
            d = info.get("data", {}) or {}
            name = d.get("title") or d.get("name") or aid
        seen_actor_names[aid] = name
        r["actor_name"] = name
    return out


def get_apify_account(token: str) -> dict:
    p = _http_get(f"{APIFY_API}/users/me", token) or {}
    d = (p.get("data") or {})
    plan = d.get("plan") or {}
    monthly_usage = d.get("monthlyUsageUsd")
    monthly_cap = (plan.get("monthlyMaxComputeUnitsUsd") or
                   d.get("monthlyServiceUsageInfo", {}).get("limitUsd"))
    return {
        "username": d.get("username") or "",
        "email": d.get("email") or "",
        "plan_id": plan.get("id") or "",
        "monthly_usage_usd": monthly_usage,
        "monthly_cap_usd": monthly_cap,
    }


# ─── Ledger ─────────────────────────────────────────────────────────────────

def load_ledger(since_iso: str) -> list[dict]:
    p = _resolve_ledger()
    if not p:
        return []
    out: list[dict] = []
    try:
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except ValueError:
                continue
            ts = e.get("ts") or ""
            if ts < since_iso:
                continue
            out.append(e)
    except OSError:
        pass
    return out


# ─── Aggregation ────────────────────────────────────────────────────────────

# Map raw actor strings → human-readable scraper names + which platform.
ACTOR_TO_LABEL = {
    "apify_google/crawlerbros":          ("Google Ads — crawlerbros", "Google Ads"),
    "apify_meta/curious_coder":          ("Meta Ads — Curious Coder", "Meta Ads"),
    "apify_linkedin/silva95gustavo":     ("LinkedIn Ads — silva95gustavo", "LinkedIn Ads"),
}


def _iso_today_minus(days: int) -> str:
    return (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=days)
            ).isoformat(timespec="seconds").replace("+00:00", "Z")


def _ts_today_midnight_iso() -> str:
    now = _dt.datetime.now(_dt.timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(
        timespec="seconds").replace("+00:00", "Z")


def aggregate(apify_runs: list[dict], ledger: list[dict]) -> dict:
    """Compute KPIs + per-actor + per-competitor + daily + recent + anomalies."""
    today_iso = _ts_today_midnight_iso()
    seven_ago = _iso_today_minus(7)
    thirty_ago = _iso_today_minus(30)

    # Index ledger by run_id for the join
    ledger_by_run = {e["run_id"]: e for e in ledger if e.get("run_id")}

    # Tag each Apify run with our metadata when available
    for r in apify_runs:
        meta = ledger_by_run.get(r["run_id"])
        r["is_ours"]      = meta is not None
        r["competitor"]   = (meta or {}).get("competitor", "")
        r["items"]        = (meta or {}).get("items_fetched", 0)
        r["actor_label"]  = ACTOR_TO_LABEL.get((meta or {}).get("actor", ""), (r["actor_name"], "Other"))[0]
        r["platform"]     = ACTOR_TO_LABEL.get((meta or {}).get("actor", ""), (r["actor_name"], "Other"))[1]
        r["batch_id"]     = (meta or {}).get("batch_id", "")

    # ─── KPIs (Apify ground truth) ──
    def sum_since(cutoff: str, predicate=lambda r: True) -> float:
        return sum(r["cost_usd"] for r in apify_runs
                   if r["started_at"] >= cutoff and predicate(r))

    today_total      = sum_since(today_iso)
    week_total       = sum_since(seven_ago)
    month_total      = sum_since(thirty_ago)
    today_ours       = sum_since(today_iso, lambda r: r["is_ours"])
    today_external   = today_total - today_ours
    month_ours       = sum_since(thirty_ago, lambda r: r["is_ours"])
    month_external   = month_total - month_ours
    burn_per_day     = round(week_total / 7, 4) if week_total else 0
    forecast_month   = round(burn_per_day * 30, 2)

    kpis = {
        "today_usd":           round(today_total, 4),
        "week_usd":            round(week_total, 4),
        "month_usd":           round(month_total, 4),
        "burn_per_day_usd":    burn_per_day,
        "forecast_month_usd":  forecast_month,
        "today_ours_usd":      round(today_ours, 4),
        "today_external_usd":  round(today_external, 4),
        "month_ours_usd":      round(month_ours, 4),
        "month_external_usd":  round(month_external, 4),
    }

    # ─── Per-actor (30d window) ──
    per_actor: dict[str, dict] = {}
    for r in apify_runs:
        if r["started_at"] < thirty_ago:
            continue
        aid = r["actor_id"] or "(unknown)"
        slot = per_actor.setdefault(aid, {
            "actor_id":     aid,
            "actor_name":   r["actor_name"] or aid,
            "today_usd":    0.0,
            "week_usd":     0.0,
            "month_usd":    0.0,
            "runs_30d":     0,
            "is_ours":      r["is_ours"],
        })
        slot["month_usd"] += r["cost_usd"]
        if r["started_at"] >= seven_ago:  slot["week_usd"]  += r["cost_usd"]
        if r["started_at"] >= today_iso:  slot["today_usd"] += r["cost_usd"]
        slot["runs_30d"] += 1
        slot["is_ours"] = slot["is_ours"] or r["is_ours"]
    per_actor_list = sorted(per_actor.values(),
                            key=lambda s: -s["month_usd"])
    # Round
    for s in per_actor_list:
        for k in ("today_usd", "week_usd", "month_usd"):
            s[k] = round(s[k], 4)

    # ─── Per-competitor (30d, ours only) ──
    per_comp: dict[str, dict] = {}
    for e in ledger:
        if (e.get("ts") or "") < thirty_ago:
            continue
        comp = e.get("competitor", "(none)") or "(none)"
        slot = per_comp.setdefault(comp, {
            "competitor":   comp,
            "runs":         0,
            "items":        0,
            "month_usd":    0.0,
        })
        slot["runs"]      += 1
        slot["items"]     += int(e.get("items_fetched", 0) or 0)
        slot["month_usd"] += float(e.get("est_cost_usd", 0) or 0)
    per_comp_list = sorted(per_comp.values(),
                           key=lambda s: -s["month_usd"])
    for s in per_comp_list:
        s["month_usd"] = round(s["month_usd"], 4)
        s["cost_per_item"] = (round(s["month_usd"] / s["items"], 6)
                              if s["items"] else None)

    # ─── Daily stacked-bar data (30d) ──
    by_day: dict[str, dict] = {}
    today_d = _dt.date.today()
    for i in range(30):
        d = (today_d - _dt.timedelta(days=29 - i)).isoformat()
        by_day[d] = {"date": d, "total": 0.0, "by_platform": {}}
    for r in apify_runs:
        if r["started_at"] < thirty_ago:
            continue
        d = r["started_at"][:10]
        slot = by_day.get(d)
        if slot is None:
            continue
        plat = r["platform"]
        slot["total"] += r["cost_usd"]
        slot["by_platform"][plat] = slot["by_platform"].get(plat, 0) + r["cost_usd"]
    for slot in by_day.values():
        slot["total"] = round(slot["total"], 4)
        slot["by_platform"] = {k: round(v, 4) for k, v in slot["by_platform"].items()}
    daily = list(by_day.values())  # already ordered oldest → newest

    # ─── Recent 60 runs ──
    recent = sorted(apify_runs, key=lambda r: r["started_at"], reverse=True)[:60]

    # ─── Anomaly flag (cost > 2× source mean) ──
    source_avg: dict[str, float] = {}
    grouped: dict[str, list[float]] = defaultdict(list)
    for r in apify_runs:
        if r["started_at"] < thirty_ago:
            continue
        grouped[r["actor_id"] or "(unknown)"].append(r["cost_usd"])
    for aid, costs in grouped.items():
        source_avg[aid] = sum(costs) / len(costs) if costs else 0

    for r in recent:
        avg = source_avg.get(r["actor_id"] or "(unknown)", 0)
        r["is_anomaly"] = bool(r["cost_usd"] > 0 and avg > 0 and r["cost_usd"] > avg * 2)
        r["cost_usd"] = round(r["cost_usd"], 4)

    # ─── Failures (7d) ──
    failures = [r for r in apify_runs
                if r["started_at"] >= seven_ago
                and r["status"] in ("FAILED", "ABORTED", "TIMED-OUT")]
    failures.sort(key=lambda r: r["started_at"], reverse=True)
    for r in failures:
        r["cost_usd"] = round(r["cost_usd"], 4)

    return {
        "kpis":         kpis,
        "per_actor":    per_actor_list,
        "per_competitor": per_comp_list,
        "daily":        daily,
        "recent":       recent,
        "failures":     failures[:30],
        "as_of":        _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        "window_days":  30,
    }


# ─── Handler ────────────────────────────────────────────────────────────────

def build_payload() -> dict:
    token = os.environ.get("APIFY_TOKEN", "")
    since = _iso_today_minus(30)
    ledger = load_ledger(since)

    if not token:
        # Degraded mode — synthesize a minimal payload from the ledger only.
        synthetic_runs = []
        for e in ledger:
            synthetic_runs.append({
                "run_id":     e.get("run_id", ""),
                "actor_id":   e.get("actor", ""),
                "actor_name": ACTOR_TO_LABEL.get(e.get("actor", ""), ("", ""))[0] or e.get("actor", ""),
                "status":     "SUCCEEDED",
                "started_at": e.get("ts", ""),
                "finished_at": e.get("ts", ""),
                "cost_usd":   float(e.get("est_cost_usd", 0) or 0),
            })
        payload = aggregate(synthetic_runs, ledger)
        payload["account"] = None
        payload["mode"] = "degraded — set APIFY_TOKEN on Vercel for ground-truth costs"
        return payload

    apify_runs = list_apify_runs(token, since)
    account = get_apify_account(token)
    payload = aggregate(apify_runs, ledger)
    payload["account"] = account
    payload["mode"] = "live"
    return payload


class handler(BaseHTTPRequestHandler):  # noqa: N801 — Vercel expects this name
    def do_GET(self):  # noqa: N802
        try:
            data = build_payload()
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            # Live numbers — no caching at any layer
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("CDN-Cache-Control", "no-store")
            self.send_header("Vercel-CDN-Cache-Control", "no-store")
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
        self.end_headers()
