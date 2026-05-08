"""
pipeline/observability.py — spend + per-run metrics for run_weekly.

Two artifacts get written to disk:

    metrics/spend_history.jsonl
        One JSON line per paid actor invocation. Append-only ledger of
        every dollar this pipeline spends. So we can audit cost without
        opening the Apify dashboard.

    metrics/run_<batch_id>.json
        Single-run summary: per-source items, per-competitor counts,
        merge stats, preview-coverage delta, total spend, runtime.

Both files are line-oriented or self-contained JSON — no schemas, no
migrations, nothing fancy. They get committed to git via .gitignore
exclusion (logs/ is ignored, but metrics/ is kept).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
METRICS_DIR = REPO / "metrics"
SPEND_LEDGER = METRICS_DIR / "spend_history.jsonl"


def _ensure_dir() -> None:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)


def record_spend(
    batch_id: str,
    actor: str,
    competitor: str,
    items_fetched: int,
    est_cost_usd: float,
    *,
    extra: dict | None = None,
) -> None:
    """Append one line to metrics/spend_history.jsonl. Never raises."""
    _ensure_dir()
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "batch_id": batch_id,
        "actor": actor,
        "competitor": competitor,
        "items_fetched": int(items_fetched or 0),
        "est_cost_usd": round(float(est_cost_usd or 0.0), 4),
    }
    if extra:
        entry.update(extra)
    try:
        with SPEND_LEDGER.open("a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass  # observability must not break the pipeline


def write_run_metrics(batch_id: str, metrics: dict) -> Path:
    """Write metrics/run_<batch_id>.json. Returns the path."""
    _ensure_dir()
    path = METRICS_DIR / f"run_{batch_id}.json"
    try:
        path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2))
    except OSError as e:
        # Fallback: still try to write the path, but truncate if needed
        path.write_text(json.dumps({"error": str(e), "batch_id": batch_id}))
    return path


def total_spend_today() -> float:
    """Sum est_cost_usd from today's ledger entries. For sanity prints."""
    if not SPEND_LEDGER.exists():
        return 0.0
    today = datetime.now(timezone.utc).date().isoformat()
    total = 0.0
    try:
        for line in SPEND_LEDGER.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if (e.get("ts") or "").startswith(today):
                total += float(e.get("est_cost_usd") or 0.0)
    except OSError:
        return 0.0
    return total


class RunTimer:
    """Context-managed timer. `with RunTimer() as t: ...; t.elapsed_s`."""
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *a):
        self.elapsed_s = time.time() - self.start
