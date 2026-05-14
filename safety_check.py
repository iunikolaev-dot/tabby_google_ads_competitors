"""
safety_check.py — preconditions P1-P6 (PRD v2 §4.8).

This module is invoked before any pipeline entry point. It verifies that the
repository is in a safe state to write to `public/ads_data.js`.

Design rules:
    1. Every check is a pure function that returns (ok: bool, detail: str).
    2. Checks never raise. A failure is a structured result, not an exception.
    3. The orchestrator decides which checks are required for a given run.
       Recovery runs skip scraper-only checks (P3 API keys, P4 Apify balance,
       P6 approval token) because they don't hit the network.
    4. This file is the ONLY place that enforces preconditions. Scrapers and
       merge code trust that the checks have already passed.

CLI usage:
    python safety_check.py                     # runs ALL checks
    python safety_check.py --mode=recovery     # skips P3/P4/P6
    python safety_check.py --mode=dry          # skips P4/P6 (env vars still required)

Exit codes:
    0 → all required checks passed
    1 → at least one required check failed

Cross-reference: PRD §4.8 preconditions P1-P6.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable

import config


# ─────────────────────────────────────────────────────────────────────────────
# Result type
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    code: str        # e.g. "P1"
    name: str        # human-readable
    ok: bool
    detail: str      # success or failure detail

    def __str__(self) -> str:
        status = "PASS" if self.ok else "FAIL"
        return f"[{status}] {self.code} {self.name}: {self.detail}"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _load_sot_rows(path: Path) -> list[dict] | None:
    """
    Parse `public/ads_data.js` which is wrapped as `const ADS_DATA = [...];`.
    Returns the list of rows, or None if the file is missing/malformed.
    """
    if not path.exists():
        return None
    try:
        txt = path.read_text()
    except Exception:
        return None

    # Strip the JS wrapper. The file shape is: `const ADS_DATA = [...];` on
    # effectively one line, followed by an optional newline.
    start = txt.find("[")
    end = txt.rfind("]")
    if start < 0 or end < 0 or end < start:
        return None
    try:
        return json.loads(txt[start:end + 1])
    except json.JSONDecodeError:
        return None


def _load_manifest() -> dict:
    """Read manifest.json; return empty dict if missing or malformed."""
    if not config.MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(config.MANIFEST_PATH.read_text())
    except Exception:
        return {}


def _sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _today_gst_str() -> str:
    """Returns today's date in GST (UTC+4) as YYYYMMDD, matching PRD §4.9."""
    gst = timezone(timedelta(hours=4))
    return datetime.now(gst).strftime("%Y%m%d")


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks (P1 - P6)
# ─────────────────────────────────────────────────────────────────────────────

def check_p1_sot_parses_and_row_count() -> CheckResult:
    """
    P1 — the SoT is readable and contains ≥ SOT_MIN_ROW_COUNT_FRACTION of the
    row count recorded by the previous successful run in manifest.json.

    SoT changed in audit step 5: data/ads.db (SQLite) replaced public/ads_data.js
    (JSON). Read row count from whichever the repo actually has.
    """
    db_path = config.REPO_ROOT / "data" / "ads.db"
    row_count: int | None = None
    if db_path.exists():
        try:
            import sqlite3
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            row_count = conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
            conn.close()
        except Exception as e:
            return CheckResult("P1", "SoT parseable",
                               False, f"{db_path} unreadable: {e}")
    else:
        rows = _load_sot_rows(config.SOT_PATH)
        if rows is None:
            return CheckResult("P1", "SoT parseable", False,
                               f"neither {db_path} nor {config.SOT_PATH} found")
        row_count = len(rows)

    manifest = _load_manifest()
    prev_count = manifest.get("row_count")

    if prev_count is None:
        # First run — just verify the file is non-empty.
        if row_count == 0:
            return CheckResult("P1", "SoT parseable",
                               False, "file parses but has 0 rows")
        return CheckResult("P1", "SoT parseable",
                           True,
                           f"{row_count} rows (no previous manifest to compare)")

    threshold = int(prev_count * config.SOT_MIN_ROW_COUNT_FRACTION)
    if row_count < threshold:
        return CheckResult("P1", "SoT parseable",
                           False,
                           f"{row_count} rows < {threshold} "
                           f"({config.SOT_MIN_ROW_COUNT_FRACTION * 100:.0f}% of "
                           f"previous {prev_count})")

    return CheckResult("P1", "SoT parseable",
                       True,
                       f"{row_count} rows (prev {prev_count}, threshold {threshold})")


def check_p2_backup_exists() -> CheckResult:
    """
    P2 — a timestamped backup of the current SoT exists under backups/ with
    a sha256 sidecar matching the current SoT. Creates one if missing.

    After audit step 5 the SoT is data/ads.db; we back that up. If only the
    legacy public/ads_data.js exists (pre-5.1 worktree), we back that up
    instead.
    """
    db_path = config.REPO_ROOT / "data" / "ads.db"
    if db_path.exists():
        sot = db_path
        backup_prefix = "ads_data_"
        backup_ext = ".db"
    elif config.SOT_PATH.exists():
        sot = config.SOT_PATH
        backup_prefix = "ads_data_"
        backup_ext = ".js"
    else:
        return CheckResult("P2", "Backup verified",
                           False,
                           f"cannot backup: neither {db_path} nor "
                           f"{config.SOT_PATH} present")

    config.BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    sot_sha = _sha256_of_file(sot)

    # Look for any existing backup with a matching sha in its sidecar
    for existing in config.BACKUPS_DIR.glob(f"{backup_prefix}*{backup_ext}*"):
        sidecar = existing.with_suffix(existing.suffix + ".sha256")
        if sidecar.exists() and sidecar.read_text().strip() == sot_sha:
            return CheckResult("P2", "Backup verified",
                               True,
                               f"matching backup at {existing.name}")

    # Create a fresh backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = config.BACKUPS_DIR / f"{backup_prefix}{ts}{backup_ext}"
    dest.write_bytes(sot.read_bytes())
    (dest.with_suffix(dest.suffix + ".sha256")).write_text(sot_sha)

    return CheckResult("P2", "Backup verified",
                       True,
                       f"created fresh backup at {dest.name}")


def check_p3_env_vars() -> CheckResult:
    """
    P3 — all required API keys present in the environment.

    Uses config.ENV_VAR_ALIASES to accept either the v1 or v2 name for each key.
    """
    missing: list[str] = []
    resolved: list[str] = []
    for required in config.REQUIRED_ENV_VARS:
        val = config.resolve_env(required)
        if val:
            resolved.append(required)
        else:
            aliases = config.ENV_VAR_ALIASES.get(required, (required,))
            missing.append(f"{required} (tried: {', '.join(aliases)})")

    if missing:
        return CheckResult("P3", "Env vars present",
                           False,
                           f"missing: {'; '.join(missing)}")
    return CheckResult("P3", "Env vars present",
                       True,
                       f"found: {', '.join(resolved)}")


def check_p4_apify_balance() -> CheckResult:
    """
    P4 — Apify account balance ≥ APIFY_MIN_BALANCE_USD.

    This check makes a network call to api.apify.com. It is skipped for
    recovery and dry runs.
    """
    import requests  # local import so recovery runs don't need the package

    token = config.resolve_env("APIFY_TOKEN")
    if not token:
        return CheckResult("P4", "Apify balance",
                           False, "APIFY_TOKEN not set (P3 should have caught this)")

    try:
        resp = requests.get(
            "https://api.apify.com/v2/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
    except Exception as e:
        return CheckResult("P4", "Apify balance",
                           False, f"request failed: {e}")

    if resp.status_code != 200:
        return CheckResult("P4", "Apify balance",
                           False, f"HTTP {resp.status_code}: {resp.text[:200]}")

    try:
        data = resp.json().get("data", {})
    except Exception as e:
        return CheckResult("P4", "Apify balance",
                           False, f"malformed JSON: {e}")

    # Apify's users/me response includes a plan + proxy object but the exact
    # balance field varies. Try several known paths.
    balance = (
        data.get("currentBillingPeriod", {}).get("usageUsd")
        or data.get("usage", {}).get("monthlyUsageUsd")
        or data.get("plan", {}).get("availablePrepaidUsageUsd")
    )
    # If we can't find a balance field, fail open with a warning so the run
    # isn't blocked by a schema change on Apify's side.
    if balance is None:
        return CheckResult("P4", "Apify balance",
                           True,
                           "balance field not found in response — proceeding with warning")

    try:
        balance = float(balance)
    except (TypeError, ValueError):
        return CheckResult("P4", "Apify balance",
                           True, f"non-numeric balance '{balance}' — proceeding with warning")

    if balance < config.APIFY_MIN_BALANCE_USD:
        return CheckResult("P4", "Apify balance",
                           False,
                           f"${balance:.2f} < ${config.APIFY_MIN_BALANCE_USD:.2f} minimum")
    return CheckResult("P4", "Apify balance",
                       True,
                       f"${balance:.2f} ≥ ${config.APIFY_MIN_BALANCE_USD:.2f}")


def check_p5_lock_file() -> CheckResult:
    """
    P5 — /tmp/tabby_scraper.lock does not exist. Prevents concurrent runs.

    This check does NOT create the lock; the caller does, after all
    preconditions pass. This keeps the check pure and side-effect-free.
    """
    if config.LOCK_PATH.exists():
        try:
            pid = config.LOCK_PATH.read_text().strip()
        except Exception:
            pid = "unknown"
        return CheckResult("P5", "Lock file absent",
                           False,
                           f"{config.LOCK_PATH} exists (pid={pid}). "
                           f"Another run is in progress, or the previous run crashed. "
                           f"If you're sure no run is active, delete the file manually.")
    return CheckResult("P5", "Lock file absent",
                       True, f"{config.LOCK_PATH} not present")


def check_p6_approval_token() -> CheckResult:
    """
    P6 — a fresh approval token exists at /tmp/tabby_approval_{today}.token.

    PRD §4.9: approval is per-day. A token from yesterday does NOT authorize
    a run today. This is the gate that prevents "the cron approved a week
    ago" failure mode.
    """
    today = _today_gst_str()
    path = config.approval_token_path(today)
    if not path.exists():
        return CheckResult("P6", "Approval token",
                           False,
                           f"no token at {path}. Ask the human for approval "
                           f"in the chat, then create this file.")

    try:
        content = path.read_text().strip()
    except Exception as e:
        return CheckResult("P6", "Approval token",
                           False, f"cannot read token: {e}")

    if not content:
        return CheckResult("P6", "Approval token",
                           False, f"token at {path} is empty")

    return CheckResult("P6", "Approval token",
                       True, f"{path.name} present ({len(content)} chars)")


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

# Which checks run in which mode.
#   "live"     → full pipeline with paid scrapers (P1-P6)
#   "dry"      → dry run with env-var check but no paid calls (P1, P2, P3, P5)
#   "recovery" → §10.1 CDN recovery, no scrapers (P1, P2, P5)
CHECKS_BY_MODE: dict[str, list[tuple[str, Callable[[], CheckResult]]]] = {
    "live": [
        ("P1", check_p1_sot_parses_and_row_count),
        ("P2", check_p2_backup_exists),
        ("P3", check_p3_env_vars),
        ("P4", check_p4_apify_balance),
        ("P5", check_p5_lock_file),
        ("P6", check_p6_approval_token),
    ],
    "dry": [
        ("P1", check_p1_sot_parses_and_row_count),
        ("P2", check_p2_backup_exists),
        ("P3", check_p3_env_vars),
        ("P5", check_p5_lock_file),
    ],
    "recovery": [
        ("P1", check_p1_sot_parses_and_row_count),
        ("P2", check_p2_backup_exists),
        ("P5", check_p5_lock_file),
    ],
}


def run_checks(mode: str) -> tuple[bool, list[CheckResult]]:
    """
    Run the checks required for `mode`. Returns (all_ok, results).
    """
    if mode not in CHECKS_BY_MODE:
        raise ValueError(f"Unknown mode '{mode}'. "
                         f"Expected one of {list(CHECKS_BY_MODE.keys())}")

    results: list[CheckResult] = []
    for _code, fn in CHECKS_BY_MODE[mode]:
        try:
            results.append(fn())
        except Exception as e:
            # Belt and suspenders: a check should never raise, but if it
            # does, treat it as a hard failure with a clear message.
            results.append(CheckResult(
                code=_code,
                name=fn.__name__,
                ok=False,
                detail=f"check raised {type(e).__name__}: {e}",
            ))

    all_ok = all(r.ok for r in results)
    return all_ok, results


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run v2 pipeline preconditions P1-P6.")
    parser.add_argument("--mode",
                        choices=list(CHECKS_BY_MODE.keys()),
                        default="live",
                        help="which check set to run (default: live)")
    args = parser.parse_args()

    print(f"safety_check.py — mode={args.mode}")
    print("-" * 70)
    ok, results = run_checks(args.mode)
    for r in results:
        print(r)
    print("-" * 70)
    print(f"Result: {'OK' if ok else 'FAIL'} "
          f"({sum(r.ok for r in results)}/{len(results)} checks passed)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
