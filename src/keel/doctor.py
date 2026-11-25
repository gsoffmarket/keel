"""`keel doctor`: environment and connectivity checks before recording."""
from __future__ import annotations

import asyncio
import json
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

from keel import net
from keel.config import HTTP_HEADERS, RecorderConfig
from keel.hl.info import InfoClient, InfoError
from keel.hl.ws import WebSocket


PROBE_PROFILES = {
    "keel-default": HTTP_HEADERS,
    "keel-no-gzip": {**HTTP_HEADERS, "accept-encoding": "identity"},
    "python-default": {},
    "json-accept": {"user-agent": HTTP_HEADERS["user-agent"], "accept": "application/json"},
    "curl-like": {"user-agent": "curl/8.7.1", "accept": "*/*"},
    "browser-like": {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
    },
}


def probe_url(url: str) -> List[Dict[str, Any]]:
    """Diagnose a refused reference feed: one GET per header profile, status and a
    short body excerpt each. Read-only."""
    out = []
    for name, hdrs in PROBE_PROFILES.items():
        try:
            body, wire, status = net.get(url, hdrs)
            out.append({"profile": name, "status": status, "bytes": wire, "server": None, "excerpt": body[:80]})
        except urllib.error.HTTPError as exc:
            text = exc.read()[:200].decode(errors="replace")
            out.append({"profile": name, "status": exc.code, "server": exc.headers.get("server") if exc.headers else None, "excerpt": text})
        except Exception as exc:  # network
            out.append({"profile": name, "status": None, "excerpt": repr(exc)[:200]})
    return out


async def run_checks(cfg: RecorderConfig, network: bool = True) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def add(name: str, ok: str, detail: str) -> None:
        out.append({"check": name, "status": ok, "detail": detail})

    v = sys.version_info
    add("Python >= 3.9", "PASS" if v >= (3, 9) else "FAIL", f"{v.major}.{v.minor}.{v.micro}")
    root = Path(cfg.data_root)
    try:
        root.mkdir(parents=True, exist_ok=True)
        probe = root / ".write-probe"
        probe.write_text("ok")
        probe.unlink()
        add("Data folder writable", "PASS", str(root.resolve()))
    except OSError as exc:
        add("Data folder writable", "FAIL", f"{root}: {exc}")
    free = shutil.disk_usage(root if root.exists() else Path.cwd()).free / 1e9
    add("Free disk >= 20 GB", "PASS" if free >= 20 else ("WARN" if free >= 5 else "FAIL"), f"{free:.1f} GB free; budget ~1 GB/day compressed")
    add("No keys used", "PASS", "the recorder has no signing code and never calls the exchange endpoint")
    if not network:
        return out

    info = InfoClient(cfg.info_url, budget_share=0.1)
    try:
        t0 = time.time_ns()
        meta = await info.outcome_meta()
        t1 = time.time_ns()
        n = len(meta.get("outcomes", []))
        add("Hyperliquid info API (outcomeMeta)", "PASS" if n else "WARN", f"{n} live outcomes; {(t1 - t0) / 1e6:.0f} ms")
        coins = ["#" + str(10 * int(o["outcome"])) for o in meta.get("outcomes", [])][:5]
        offsets = []
        for c in coins:
            a = time.time_ns()
            b = await info.l2_book(c)
            z = time.time_ns()
            if b and b.get("time"):
                offsets.append(((a + z) / 2) / 1e6 - int(b["time"]))
        if offsets:
            off = sorted(offsets)[len(offsets) // 2]
            status = "PASS" if abs(off) <= 250 else "WARN"
            add("Clock vs exchange time", status, f"local minus exchange ~{off:+.0f} ms (median of {len(offsets)}; includes exchange-side delay). PRD limit 250 ms")
    except InfoError as exc:
        add("Hyperliquid info API (outcomeMeta)", "FAIL", str(exc))

    try:
        ws = await WebSocket.connect(cfg.ws_url, timeout=10)
        await ws.send_text('{"method":"ping"}')
        msg = await asyncio.wait_for(ws.recv(), 10)
        await ws.close()
        add("Hyperliquid WebSocket", "PASS" if "pong" in msg else "WARN", msg[:80])
    except Exception as exc:
        add("Hyperliquid WebSocket", "FAIL", repr(exc)[:200])

    for ref in cfg.references:
        try:
            loop = asyncio.get_running_loop()
            body, wire, _ = await loop.run_in_executor(None, net.get, ref["url"], ref.get("headers") or HTTP_HEADERS)
            json.loads(body)
            add(f"Reference feed {ref['name']}", "PASS", f"{len(body):,} bytes ({wire:,} on the wire)")
        except Exception as exc:
            add(f"Reference feed {ref['name']}", "WARN", f"{exc!r}"[:200] + " (recording continues without it)")
    return out
