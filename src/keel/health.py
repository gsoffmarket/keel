"""Health page (PRD M0 stub): a read-only, localhost-only view of the recorder.

It reads only files the recorder already writes (meta/status.json, the event log and
the latest quality reports) and never talks to an exchange. It binds to 127.0.0.1 and
refuses any other address; reach it from another machine with an SSH tunnel
(ssh -L 8787:127.0.0.1:8787 host). Only GET is served. The full Health screen of the
v1 dashboard (M4) replaces it.
"""
from __future__ import annotations

import html
import json
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

LOOPBACK = {"127.0.0.1", "::1", "localhost"}
NOTABLE = {
    "session_start", "session_end", "connected", "disconnected", "connect_failed", "pm_connected",
    "pm_disconnected", "pm_connect_failed", "recorder_stalled", "disk_low", "disk_critical",
    "budget_enforced", "budget_exceeded", "budget_ok", "task_error", "rest_error", "ws_error_message",
    "subscription_refused", "outcome_settled", "outcome_settlement_missing", "mapping_changed",
    "mapping_removed", "reference_error", "meta_parse_error", "settlement_parse_error",
}
STALE_AFTER_S = 30


def _tail_events(root: Path, n: int = 40, max_bytes: int = 2_000_000) -> List[Dict[str, Any]]:
    """Last notable events, reading only the end of the newest event files."""
    base = root / "meta" / "events"
    out: List[Dict[str, Any]] = []
    if not base.exists():
        return out
    for p in sorted(base.glob("*.jsonl"), reverse=True):
        size = p.stat().st_size
        with open(p, "rb") as fh:
            fh.seek(max(0, size - max_bytes))
            lines = fh.read().decode("utf-8", "replace").splitlines()
        if size > max_bytes:
            lines = lines[1:]  # first line may be partial
        for ln in reversed(lines):
            try:
                e = json.loads(ln)
            except ValueError:
                continue
            if e.get("kind") in NOTABLE:
                out.append(e)
                if len(out) >= n:
                    return out
    return out


def _latest(paths: List[Path]) -> Optional[Dict[str, Any]]:
    for p in sorted(paths, key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            d = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        d["_path"] = str(p)
        return d
    return None


def snapshot(root: Path, now_ns: Optional[int] = None) -> Dict[str, Any]:
    """Everything the page shows, as one JSON-able dict (also served at /health.json)."""
    root = Path(root)
    now_ns = now_ns or time.time_ns()
    sp = root / "meta" / "status.json"
    status = None
    if sp.exists():
        try:
            status = json.loads(sp.read_text())
        except ValueError:
            status = None
    age = (now_ns - status["now_ns"]) / 1e9 if status else None
    if status is None:
        state = "no data"
    elif age > STALE_AFTER_S:
        state = "not running"
    elif not status.get("healthy", True):
        state = "degraded"
    else:
        state = "running"
    rep = root / "reports"
    quality = _latest(list(rep.glob("quality-*.json"))) if rep.exists() else None
    daily = _latest(list((rep / "daily").glob("daily-*.json"))) if (rep / "daily").exists() else None
    return {
        "generated_ns": now_ns,
        "state": state,
        "status_age_s": round(age, 1) if age is not None else None,
        "status": status,
        "quality": {"path": quality["_path"], "span": quality.get("span"), "checks": quality.get("checks"), "storage_bytes_span_days": quality.get("storage_bytes_span_days")} if quality else None,
        "daily": daily,
        "events": _tail_events(root),
    }


def _iso(ns: Optional[int]) -> str:
    if not ns:
        return "—"
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _e(x: Any) -> str:
    return html.escape("—" if x is None else str(x))


def render(snap: Dict[str, Any], refresh_s: int = 15) -> str:
    s = snap.get("status") or {}
    state = snap["state"]
    tone = {"running": "ok", "degraded": "warn", "not running": "bad", "no data": "muted"}[state]
    rows = [
        ("Session", s.get("session")),
        ("Up since", _iso(s.get("started_ns"))),
        ("Last status", f"{snap['status_age_s']} s ago" if snap["status_age_s"] is not None else None),
        ("Connected", s.get("connected")),
        ("Reconnects", s.get("reconnects")),
        ("Live outcomes", s.get("live_outcomes")),
        ("Fast stream", f"{s.get('fast_book_coins')} of {s.get('fast_eligible')} eligible · {s.get('tier_changes')} tier changes" if s else None),
        ("Live references", (lambda r: f"{r.get('usable_now')} usable now of {r.get('mapped_outcomes')} mapped · Polymarket {'connected' if r.get('pm_connected') else 'disconnected'}, {r.get('pm_reconnects')} reconnects" if isinstance(r, dict) else r)(s.get("references_live")) if s else None),
        ("Median book age", f"{s.get('book_age_s_p50')} s" if s.get("book_age_s_p50") is not None else None),
        ("Hyperliquid info latency", f"p50 {s.get('hl_info_latency_ms_p50')} ms · p95 {s.get('hl_info_latency_ms_p95')} ms" if s else None),
        ("Hyperliquid WS ping", f"p50 {s.get('hl_ws_ping_rtt_ms_p50')} ms · p95 {s.get('hl_ws_ping_rtt_ms_p95')} ms" if s else None),
        ("Data folder", f"{s.get('data_gb')} GB of {s.get('disk_budget_gb')} GB budget" + (" · broad polls paused" if s.get("broad_refs_paused") else "") if s else None),
        ("Disk free", f"{s.get('disk_free_gb')} GB" if s else None),
        ("Errors", ", ".join(f"{k} {v}" for k, v in (s.get("errors") or {}).items()) or "none" if s else None),
    ]
    status_html = "".join(f"<tr><th>{_e(k)}</th><td>{_e(v)}</td></tr>" for k, v in rows)

    q = snap.get("quality")
    if q and q.get("checks"):
        span = q.get("span") or {}
        checks = "".join(f"<tr><td class='st {_e(str(c['status']).lower().replace(' ', '-'))}'>{_e(c['status'])}</td><td>{_e(c['check'])}</td><td class='dim'>{_e(c['detail'])}</td></tr>" for c in q["checks"])
        quality_html = f"<p class='dim'>Latest report: {_e(Path(q['path']).name)} · {_e(span.get('hours'))} h scored</p><table class='checks'>{checks}</table>"
    else:
        quality_html = "<p class='dim'>No quality report yet. Run <code>keel quality</code> or wait for the daily job.</p>"

    d = snap.get("daily")
    daily_html = (f"<p>{_e(d.get('day'))}: " + ", ".join(f"{_e(k)} <b>{_e(v)}</b>" for k, v in (d.get("checks") or {}).items()) + "</p>") if d else "<p class='dim'>No daily report yet.</p>"

    ev = snap.get("events") or []
    events_html = "".join(
        f"<tr><td class='dim'>{_e(_iso(e.get('ns')))}</td><td>{_e(e.get('kind'))}</td><td class='dim'>{_e(json.dumps({k: v for k, v in e.items() if k not in ('ns', 'kind', 'session')}, default=str)[:180])}</td></tr>"
        for e in ev
    ) or "<tr><td class='dim'>No notable events.</td></tr>"

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="{int(refresh_s)}"><title>Keel · Health</title>
<style>
:root{{--bg:#0e1013;--panel:#15181d;--line:#242932;--text:#e6e8eb;--dim:#8b93a1;--ok:#3fb27f;--warn:#d9a441;--bad:#e5534b;--muted:#6b7280}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}}
header{{display:flex;align-items:center;gap:12px;padding:14px 24px;border-bottom:1px solid var(--line)}}
h1{{font-size:15px;font-weight:600;margin:0}}h2{{font-size:12px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--dim);margin:0 0 10px}}
.pill{{padding:2px 10px;border-radius:999px;font-size:12px;font-weight:600;border:1px solid currentColor}}
.ok{{color:var(--ok)}}.warn{{color:var(--warn)}}.bad{{color:var(--bad)}}.muted{{color:var(--muted)}}
main{{display:grid;grid-template-columns:minmax(320px,1fr) minmax(420px,1.6fr);gap:16px;padding:16px 24px;max-width:1400px}}
section{{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:16px}}section.wide{{grid-column:1/-1}}
table{{border-collapse:collapse;width:100%}}th,td{{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}}
th{{font-weight:500;color:var(--dim);width:42%}}.dim{{color:var(--dim)}}code{{font:12px ui-monospace,Menlo,monospace}}
td.st{{font-weight:600;white-space:nowrap;width:1%}}.pass{{color:var(--ok)}}.fail{{color:var(--bad)}}.review,.pending{{color:var(--warn)}}.not-yet{{color:var(--muted)}}
.foot{{padding:0 24px 20px;color:var(--dim);font-size:12px}}
@media (max-width:900px){{main{{grid-template-columns:1fr}}}}
</style></head><body>
<header><h1>Keel · Health</h1><span class="pill {tone}">{_e(state)}</span><span class="dim">read-only recorder · refreshes every {int(refresh_s)} s</span></header>
<main>
<section><h2>Recorder</h2><table>{status_html}</table></section>
<section><h2>Data quality</h2>{quality_html}<h2 style="margin-top:16px">Daily job</h2>{daily_html}</section>
<section class="wide"><h2>Recent events</h2><table>{events_html}</table></section>
</main>
<p class="foot">Generated {_e(_iso(snap['generated_ns']))}. Same data as JSON at <code>/health.json</code>. No exchange is contacted by this page.</p>
</body></html>"""


class _Handler(BaseHTTPRequestHandler):
    root: Path = Path("data")
    refresh_s: int = 15
    server_version = "keel-health"
    sys_version = ""

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path in ("/", "/health"):
            self._send(200, render(snapshot(self.root), self.refresh_s).encode(), "text/html; charset=utf-8")
        elif path == "/health.json":
            self._send(200, json.dumps(snapshot(self.root), default=str).encode(), "application/json")
        else:
            self._send(404, b"not found", "text/plain; charset=utf-8")

    do_HEAD = do_GET

    def _refuse(self) -> None:
        self._send(405, b"read-only", "text/plain; charset=utf-8")

    do_POST = do_PUT = do_DELETE = do_PATCH = _refuse

    def log_message(self, fmt: str, *args: Any) -> None:  # keep the terminal quiet
        pass


def make_server(root: Path, host: str = "127.0.0.1", port: int = 8787, refresh_s: int = 15) -> ThreadingHTTPServer:
    if host not in LOOPBACK:
        raise ValueError(f"the health page binds to loopback only, not {host!r}; use an SSH tunnel for remote viewing")
    handler = type("Handler", (_Handler,), {"root": Path(root), "refresh_s": refresh_s})
    return ThreadingHTTPServer((host, port), handler)
