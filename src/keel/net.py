"""Plain HTTPS GET for public reference feeds (standard library only).

Every request asks for gzip and decompresses the reply. Polymarket's Gamma API
refuses requests without `Accept-Encoding: gzip` (HTTP 406 "gzip_required",
found by `keel doctor --probe-references` on 2026-11-24), and compression also cuts
Kalshi's ~3.7 MB responses substantially.
"""
from __future__ import annotations

import gzip
import ssl
import urllib.request
import zlib
from typing import Dict, Optional, Tuple

_SSL = ssl.create_default_context()


def get(url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 15.0) -> Tuple[str, int, int]:
    """Returns (decoded body text, bytes on the wire, HTTP status). Raises urllib errors."""
    hdrs = dict(headers or {})
    if not any(k.lower() == "accept-encoding" for k in hdrs):
        hdrs["accept-encoding"] = "gzip"
    req = urllib.request.Request(url, headers=hdrs)
    ctx = _SSL if url.startswith("https") else None
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        raw = resp.read()
        enc = (resp.headers.get("content-encoding") or "").lower()
        status = resp.status
    if "gzip" in enc:
        body = gzip.decompress(raw)
    elif "deflate" in enc:
        body = zlib.decompress(raw)
    else:
        body = raw
    return body.decode("utf-8"), len(raw), status
