"""
scrapers/blob_uploader.py — Mirror remote images to Vercel Blob.

Why: Meta's CDN (scontent.fbcdn.net) signs Image URLs with `oe=` tokens that
expire in 5–14 days. The dashboard breaks the moment they rot. Mirroring to
Vercel Blob gives us permanent URLs that don't depend on token freshness.

Google CDNs (ytimg, simgad, lh3, tpc.googlesyndication) are stable and not
mirrored — wasted Blob bandwidth.

Setup: requires `BLOB_READ_WRITE_TOKEN` in env. Acquire via:
    Vercel Dashboard → Storage → Create Database → Blob → connect to project.
    Then `vercel env pull .env.local` (or copy from the Blob store's env tab).

Raw HTTP API contract (extracted from the @vercel/blob-py wrapper, since the
official SDK is JS-only and the Python community wrapper requires py3.10+
which conflicts with this pipeline's py3.9):

    PUT https://blob.vercel-storage.com/?pathname=<path>
    Headers:
        Authorization: Bearer <BLOB_READ_WRITE_TOKEN>
        x-api-version: 10
        access: public
        x-content-type: <mime>
        x-cache-control-max-age: 31536000
        x-allow-overwrite: 1     (so re-running a scrape doesn't 409)
    Body: raw image bytes
    Response 200: { "url": "https://<store>.public.blob.vercel-storage.com/<path>", ... }
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests

log = logging.getLogger("scrapers.blob_uploader")

BLOB_API = "https://blob.vercel-storage.com"
API_VERSION = "10"
DEFAULT_CACHE_MAX_AGE = "31536000"  # 1 year


def _ext_from_url(url: str) -> str:
    u = url.lower()
    if ".png" in u:
        return "png"
    if ".webp" in u:
        return "webp"
    if ".gif" in u:
        return "gif"
    return "jpg"


def _mime_for_ext(ext: str) -> str:
    return {"png": "image/png", "webp": "image/webp", "gif": "image/gif"}.get(ext, "image/jpeg")


def fetch_image(remote_url: str, timeout: int = 15) -> Optional[bytes]:
    """Download a remote image. Returns bytes or None on failure."""
    if not remote_url:
        return None
    try:
        r = requests.get(
            remote_url,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
    except requests.RequestException as e:
        log.warning(f"fetch_image failed: {e}")
        return None
    if r.status_code != 200 or len(r.content) < 500:
        return None
    return r.content


def upload(
    pathname: str,
    data: bytes,
    *,
    content_type: str = "image/jpeg",
    token: Optional[str] = None,
    timeout: int = 30,
) -> Optional[str]:
    """PUT bytes to Vercel Blob at the given pathname. Returns the public URL or None."""
    token = token or os.environ.get("BLOB_READ_WRITE_TOKEN", "")
    if not token:
        log.error("BLOB_READ_WRITE_TOKEN not set")
        return None
    headers = {
        "access": "public",
        "authorization": f"Bearer {token}",
        "x-api-version": API_VERSION,
        "x-content-type": content_type,
        "x-cache-control-max-age": DEFAULT_CACHE_MAX_AGE,
        "x-allow-overwrite": "1",
    }
    try:
        r = requests.put(
            f"{BLOB_API}/?pathname={pathname}",
            data=data,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as e:
        log.warning(f"blob PUT failed: {e}")
        return None
    if r.status_code != 200:
        log.warning(f"blob PUT status {r.status_code}: {r.text[:200]}")
        return None
    try:
        return r.json().get("url")
    except ValueError:
        log.warning("blob PUT: response was not JSON")
        return None


def mirror_image(
    remote_url: str,
    creative_id: str,
    *,
    folder: str = "meta_images",
    token: Optional[str] = None,
) -> Optional[str]:
    """
    Fetch a remote image and mirror it to Blob at <folder>/<creative_id>.<ext>.

    Returns the permanent Blob URL on success, None on failure (caller should
    keep the original `remote_url` so the dashboard still has *something*).
    """
    if not remote_url or not creative_id:
        return None
    data = fetch_image(remote_url)
    if not data:
        return None
    ext = _ext_from_url(remote_url)
    pathname = f"{folder}/{creative_id}.{ext}"
    return upload(pathname, data, content_type=_mime_for_ext(ext), token=token)
