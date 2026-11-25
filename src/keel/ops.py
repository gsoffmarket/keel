"""Operations: retention, the daily job, and the host benchmark.

- `retain`: deletes raw day folders older than the retention window, but only days whose
  compacted tables exist and verify against their manifest. Bulky broad reference polls
  get a shorter window. Never touches tables, events or reports.
- `daily`: compact yesterday (UTC), write its quality report, apply retention.
- `bench`: run the recorder for a while on this host and measure what it costs here:
  CPU, memory, disk growth, download volume, latency to Hyperliquid and Polymarket,
  reconnects. Output is a report used to choose (or keep) the recording host.
"""
from __future__ import annotations

import asyncio
import json
import os
import platform
import resource
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from keel.compact import compact_day, verify_day

BROAD_REFERENCE_SOURCES = ("ref_polymarket_sports", "ref_kalshi_events")


def _day_of(name: str) -> Optional[datetime]:
    try:
        return datetime.strptime(name, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def retain(root: Path, raw_days: int = 14, broad_reference_days: int = 7, dry_run: bool = False, now: Optional[datetime] = None) -> Dict[str, Any]:
    root = Path(root)
    now = now or datetime.now(tz=timezone.utc)
    deleted: List[str] = []
    kept_unverified: List[str] = []
    freed = 0
    base = root / "raw"
    if not base.exists():
        return {"deleted": [], "kept_unverified": [], "freed_bytes": 0, "dry_run": dry_run}
    for src_dir in sorted(p for p in base.iterdir() if p.is_dir()):
        limit = broad_reference_days if src_dir.name in BROAD_REFERENCE_SOURCES else raw_days
        for day_dir in sorted(p for p in src_dir.iterdir() if p.is_dir()):
            d = _day_of(day_dir.name)
            if d is None or now - d < timedelta(days=limit + 1):
                continue
            try:
                verify_day(root, day_dir.name)
            except Exception:
                kept_unverified.append(str(day_dir.relative_to(root)))
                continue
            size = sum(f.stat().st_size for f in day_dir.glob("*"))
            if not dry_run:
                shutil.rmtree(day_dir)
            deleted.append(str(day_dir.relative_to(root)))
            freed += size
    return {"deleted": deleted, "kept_unverified": kept_unverified, "freed_bytes": freed, "dry_run": dry_run}


def data_bytes(root: Path) -> int:
    return _dir_bytes(Path(root))


def enforce_budget(root: Path, budget_bytes: int, keep_days: int = 1, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Bring the data folder under `budget_bytes` by deleting the oldest raw days first,
    all sources of a day together, and only days whose tables verify. The newest
    `keep_days` days are never deleted. Returns what it did and whether it succeeded."""
    root = Path(root)
    now = now or datetime.now(tz=timezone.utc)
    used = data_bytes(root)
    out: Dict[str, Any] = {"budget_bytes": budget_bytes, "used_before": used, "deleted_days": [], "skipped_unverified": []}
    base = root / "raw"
    if used <= budget_bytes or not base.exists():
        out.update(used_after=used, within_budget=used <= budget_bytes)
        return out
    days = sorted({p.name for src in base.iterdir() if src.is_dir() for p in src.iterdir() if p.is_dir() and _day_of(p.name)})
    cutoff = (now - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    for day in days:
        if used <= budget_bytes or day >= cutoff:
            break
        try:
            verify_day(root, day)
        except Exception:
            out["skipped_unverified"].append(day)
            continue
        for src in base.iterdir():
            d = src / day
            if d.is_dir():
                used -= sum(f.stat().st_size for f in d.glob("*"))
                shutil.rmtree(d)
        out["deleted_days"].append(day)
    out.update(used_after=used, within_budget=used <= budget_bytes)
    return out


def daily(root: Path, day: Optional[str] = None, raw_days: int = 14, broad_reference_days: int = 7, budget_gb: Optional[float] = None) -> Dict[str, Any]:
    from keel.quality import Window, write_report

    root = Path(root)
    day = day or (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    out: Dict[str, Any] = {"day": day}
    try:
        m = compact_day(root, day)
        out["compaction"] = {k: v["rows"] for k, v in m["tables"].items()}
    except FileNotFoundError as exc:
        out["compaction"] = {"error": str(exc)}
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    w = Window(int(start.timestamp()) * 10**9, int((start + timedelta(days=1)).timestamp()) * 10**9)
    jp, mp, r = write_report(root, root / "reports" / "daily", w)
    out["report"] = str(mp)
    out["checks"] = {c["check"]: c["status"] for c in r.get("checks", [])}
    out["retention"] = retain(root, raw_days, broad_reference_days)
    if budget_gb:
        out["budget"] = enforce_budget(root, int(budget_gb * 1e9))
    (root / "reports" / "daily").mkdir(parents=True, exist_ok=True)
    (root / "reports" / "daily" / f"daily-{day}.json").write_text(json.dumps(out, indent=2, default=str))
    return out


# ---------------- benchmark ----------------

def _rss_mb() -> Optional[float]:
    try:
        with open("/proc/self/status") as fh:
            for ln in fh:
                if ln.startswith("VmRSS:"):
                    return int(ln.split()[1]) / 1024
    except OSError:
        pass
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1024 * 1024) if sys.platform == "darwin" else r / 1024  # max RSS as a fallback


def _mem_total_gb() -> Optional[float]:
    try:
        with open("/proc/meminfo") as fh:
            for ln in fh:
                if ln.startswith("MemTotal:"):
                    return round(int(ln.split()[1]) / 1024 / 1024, 2)
    except OSError:
        pass
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9, 2)
    except (ValueError, OSError, AttributeError):
        return None


def _dir_bytes(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def system_info(root: Path) -> Dict[str, Any]:
    du = shutil.disk_usage(root if root.exists() else Path.cwd())
    try:
        load = os.getloadavg()
    except OSError:
        load = None
    return {
        "host": platform.node(),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
        "mem_total_gb": _mem_total_gb(),
        "disk_total_gb": round(du.total / 1e9, 1),
        "disk_free_gb": round(du.free / 1e9, 1),
        "loadavg": load,
    }


async def bench(cfg, duration_s: float, sample_every: float = 10.0) -> Dict[str, Any]:
    from keel.recorder.app import Recorder

    root = Path(cfg.data_root)
    root.mkdir(parents=True, exist_ok=True)
    before = _dir_bytes(root)
    rec = Recorder(cfg)
    samples: List[Dict[str, Any]] = []
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    t0 = time.monotonic()

    async def sampler():
        last_cpu = ru0.ru_utime + ru0.ru_stime
        last_t = t0
        while True:
            await asyncio.sleep(sample_every)
            ru = resource.getrusage(resource.RUSAGE_SELF)
            cpu = ru.ru_utime + ru.ru_stime
            now = time.monotonic()
            hl_in = rec.stream.ws.bytes_in if rec.stream.ws else 0
            samples.append({
                "t_s": round(now - t0, 1),
                "cpu_pct": round(100 * (cpu - last_cpu) / max(1e-9, now - last_t), 1),
                "rss_mb": round(_rss_mb() or 0, 1),
                "disk_bytes": _dir_bytes(root) - before,
                "hl_ws_bytes": hl_in,
                "pm_ws_bytes": rec.pm.bytes_in,
                "ref_wire_bytes": sum(rec.wire_bytes.values()),
                "connected": rec.stream.connected.is_set(),
            })
            last_cpu, last_t = cpu, now

    task = asyncio.ensure_future(sampler())
    final = await rec.run(duration=duration_s)
    task.cancel()
    elapsed = time.monotonic() - t0
    disk = _dir_bytes(root) - before
    last = samples[-1] if samples else {}
    # the HL stream connection may have been replaced; use the larger of the sampled counters
    hl_bytes = max([s["hl_ws_bytes"] for s in samples] + [0])
    down = hl_bytes + rec.pm.bytes_in + sum(rec.wire_bytes.values())
    per_day = 86400 / max(1.0, elapsed)
    cpus = [s["cpu_pct"] for s in samples]
    out = {
        "system": system_info(root),
        "duration_s": round(elapsed, 1),
        "config": {"book_mode": cfg.book_mode, "fast_venues": cfg.fast_venues, "fast_categories": cfg.fast_categories, "pm_live": cfg.pm_live},
        "cpu_pct_mean": round(sum(cpus) / len(cpus), 1) if cpus else None,
        "cpu_pct_max": max(cpus) if cpus else None,
        "rss_mb_max": max((s["rss_mb"] for s in samples), default=None),
        "disk_bytes": disk,
        "disk_gb_per_day": round(disk * per_day / 1e9, 2),
        "download_bytes": down,
        "download_gb_per_day": round(down * per_day / 1e9, 2),
        "download_kb_s": round(down / max(1.0, elapsed) / 1e3, 1),
        "note_download": "WebSocket payload plus reference HTTP bytes; excludes TLS/TCP overhead (~3-8%) and REST info replies",
        "hl_info_latency_ms_p50": final.get("hl_info_latency_ms_p50"),
        "hl_info_latency_ms_p95": final.get("hl_info_latency_ms_p95"),
        "hl_ws_ping_rtt_ms_p50": final.get("hl_ws_ping_rtt_ms_p50"),
        "hl_ws_ping_rtt_ms_p95": final.get("hl_ws_ping_rtt_ms_p95"),
        "pm_ws_ping_rtt_ms_p50": final.get("pm_ws_ping_rtt_ms_p50"),
        "reconnects": final.get("reconnects"),
        "pm_reconnects": (final.get("references_live") or {}).get("pm_reconnects"),
        "fast_book_coins": final.get("fast_book_coins"),
        "live_outcomes": final.get("live_outcomes"),
        "mapped_outcomes": (final.get("references_live") or {}).get("mapped_outcomes"),
        "errors": final.get("errors"),
        "exit_code": final.get("exit_code"),
        "samples": samples,
    }
    rep = root / "reports"
    rep.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (rep / f"bench-{stamp}.json").write_text(json.dumps(out, indent=2, default=str))
    (rep / f"bench-{stamp}.md").write_text(render_bench(out))
    out["report"] = str(rep / f"bench-{stamp}.md")
    return out


def render_bench(b: Dict[str, Any]) -> str:
    s = b["system"]
    L = [
        "# Keel host benchmark",
        "",
        f"Host {s['host']} ({s['os']}, Python {s['python']}): {s['cpus']} CPUs, {s['mem_total_gb']} GB RAM, {s['disk_free_gb']} of {s['disk_total_gb']} GB disk free, load {s['loadavg']}.",
        f"Run: {b['duration_s']} s, book mode {b['config']['book_mode']} for {b['config']['fast_venues']} / {b['config']['fast_categories']}, live references {b['config']['pm_live']}.",
        "",
        "| Measure | Value |",
        "| --- | --- |",
        f"| CPU (one process, % of one core) | mean {b['cpu_pct_mean']}, max {b['cpu_pct_max']} |",
        f"| Memory (RSS) | max {b['rss_mb_max']} MB |",
        f"| Disk growth | {b['disk_bytes'] / 1e6:.1f} MB in the run, {b['disk_gb_per_day']} GB/day projected |",
        f"| Download | {b['download_kb_s']} KB/s, {b['download_gb_per_day']} GB/day projected ({b['note_download']}) |",
        f"| Hyperliquid info API latency | p50 {b['hl_info_latency_ms_p50']} ms, p95 {b['hl_info_latency_ms_p95']} ms |",
        f"| Hyperliquid WebSocket ping RTT | p50 {b['hl_ws_ping_rtt_ms_p50']} ms, p95 {b['hl_ws_ping_rtt_ms_p95']} ms |",
        f"| Polymarket WebSocket ping RTT | p50 {b['pm_ws_ping_rtt_ms_p50']} ms |",
        f"| Reconnects (Hyperliquid / Polymarket) | {b['reconnects']} / {b['pm_reconnects']} |",
        f"| Outcomes live / on fast stream / with a live reference | {b['live_outcomes']} / {b['fast_book_coins']} / {b['mapped_outcomes']} |",
        f"| Errors | {b['errors'] or 'none'} |",
        "",
    ]
    return "\n".join(L)
