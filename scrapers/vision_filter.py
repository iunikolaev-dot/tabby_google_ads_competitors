"""
scrapers/vision_filter.py — OpenAI Vision brand filter for Cash App.

Problem: Block, Inc. is the parent of Cash App, Square, and BitKey and
shares ONE advertiser ID in Google Transparency Center
(AR14896030700992987137). No scraper can distinguish the three brands
by metadata alone. Image classification is the only signal.

This module does one thing: given an image URL, classify it as CASHAPP,
SQUARE, BITKEY, or UNKNOWN via `gpt-4o-mini`. The caller decides what to
do with the classification (typically: keep only CASHAPP, drop the rest).

Cost contract (PRD §4.3, §5.2):
    ~$0.0002 per image. Hard cap MAX_VISION_CALLS_PER_RUN=500 enforced by
    caller. This module itself does no cap enforcement.

The function is safe to call with a None or empty URL (returns UNKNOWN
without an API call).

Also exported: classify_preview() for §4.10 preview integrity sampling.
Same model, different prompt. The caller is responsible for rate limiting
and capping.

Cross-references:
    PRD §4.3    OpenAI cost contract
    PRD §4.10   Preview validator check 3
    PRD §5.2    MAX_VISION_CALLS_PER_RUN cap
"""

from __future__ import annotations

import base64
import logging
from typing import Literal

import requests

import config

log = logging.getLogger("scrapers.vision_filter")

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"
REQUEST_TIMEOUT_S = 30

BrandLabel = Literal["CASHAPP", "SQUARE", "BITKEY", "UNKNOWN"]
PreviewLabel = Literal["RENDERABLE", "ERROR", "BLANK", "UNRELATED", "UNKNOWN"]

_BRAND_PROMPT = (
    "Classify this advertising image as one of: CASHAPP, SQUARE, BITKEY, "
    "UNKNOWN. Cash App is a consumer P2P money app owned by Block, Inc. "
    "Square is a merchant payments/POS product. BitKey is a Bitcoin "
    "hardware wallet. Answer with one word only."
)

_PREVIEW_PROMPT = (
    "Is this a renderable advertising image or video frame, or is it an "
    "error page, blank image, or unrelated content? Answer with one word "
    "only: RENDERABLE, ERROR, BLANK, or UNRELATED."
)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP layer
# ─────────────────────────────────────────────────────────────────────────────

def _call_openai_vision(
    image_url: str,
    prompt: str,
    api_key: str,
) -> tuple[bool, str, str]:
    """
    POST to OpenAI chat completions with a vision prompt.

    Returns (ok, label_text, error). `label_text` is the raw assistant
    response, upper-cased and stripped. Empty on failure.
    """
    payload = {
        "model": OPENAI_MODEL,
        "max_tokens": 10,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
    }
    try:
        resp = requests.post(
            OPENAI_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=REQUEST_TIMEOUT_S,
        )
    except requests.RequestException as e:
        return False, "", f"request exception: {e}"
    if resp.status_code != 200:
        return False, "", f"HTTP {resp.status_code}: {resp.text[:200]}"
    try:
        msg = resp.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, ValueError) as e:
        return False, "", f"malformed response: {e}"
    return True, msg.strip().upper(), ""


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def classify_brand(image_url: str) -> BrandLabel:
    """
    Return the brand label for a Cash App / Square / BitKey image.

    Returns UNKNOWN without an API call if the URL is empty.
    Returns UNKNOWN on any API error (fail-open — we do not want a
    transient OpenAI outage to nuke all Cash App ads from the merge).
    """
    if not image_url:
        return "UNKNOWN"

    api_key = config.resolve_env("OPENAI_KEY")
    if not api_key:
        log.warning("OPENAI_KEY not set; returning UNKNOWN")
        return "UNKNOWN"

    ok, label, err = _call_openai_vision(image_url, _BRAND_PROMPT, api_key)
    if not ok:
        log.warning(f"classify_brand failed for {image_url[:80]}: {err}")
        return "UNKNOWN"

    if label in ("CASHAPP", "SQUARE", "BITKEY", "UNKNOWN"):
        return label  # type: ignore[return-value]
    # Sometimes the model returns "CASH APP" with a space.
    compact = label.replace(" ", "")
    if compact in ("CASHAPP", "SQUARE", "BITKEY"):
        return compact  # type: ignore[return-value]
    return "UNKNOWN"


def filter_cashapp_rows(rows: list[dict]) -> tuple[list[dict], dict]:
    """
    Given a list of Cash App Google Ads rows (output of a scraper), remove
    rows classified as SQUARE or BITKEY. UNKNOWN rows are KEPT (fail-open)
    to avoid data loss on transient OpenAI failures.

    Returns (filtered_rows, stats).
    """
    from collections import Counter

    counts: Counter = Counter()
    kept: list[dict] = []

    for row in rows:
        image_url = row.get("Image URL", "")
        label = classify_brand(image_url)
        counts[label] += 1
        if label in ("SQUARE", "BITKEY"):
            continue  # dropped
        kept.append(row)

    return kept, {
        "total": len(rows),
        "kept": len(kept),
        "dropped": len(rows) - len(kept),
        "by_label": dict(counts),
    }


def classify_preview(asset_url: str) -> PreviewLabel:
    """
    For the §4.10 preview integrity validator: classify whether an image
    or video-frame URL actually renders as an ad.

    Returns UNKNOWN on API errors — the caller can decide whether to treat
    unknown as broken or as unverified.
    """
    if not asset_url:
        return "UNKNOWN"

    api_key = config.resolve_env("OPENAI_KEY")
    if not api_key:
        return "UNKNOWN"

    ok, label, err = _call_openai_vision(asset_url, _PREVIEW_PROMPT, api_key)
    if not ok:
        log.warning(f"classify_preview failed for {asset_url[:80]}: {err}")
        return "UNKNOWN"

    if label in ("RENDERABLE", "ERROR", "BLANK", "UNRELATED"):
        return label  # type: ignore[return-value]
    return "UNKNOWN"


def estimate_cost_usd(calls: int) -> float:
    """~$0.0002 per image call per PRD §4.3."""
    return calls * 0.0002
