"""keel command line: read-only recorder commands. Nothing here signs or sends orders."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from keel import __version__
from keel.config import load_config, write_default_config
from keel.units import parse_duration

EXIT_OK, EXIT_REFUSED, EXIT_IO, EXIT_INTERNAL = 0, 2, 4, 5


def _cfg(args):
    cfg = load_config(Path(args.config) if args.config else None)
    if getattr(args, "data", None):
        cfg.data_root = args.data
    return cfg


def cmd_init(args) -> int:
    p = Path(args.config or "keel.json")
    if p.exists() and not args.force:
        print(f"{p} already exists (use --force to overwrite)")
        return EXIT_REFUSED
    write_default_config(p)
    cfg = load_config(p)
    for d in ("raw", "meta/events", "reports", "tables"):
        (Path(cfg.data_root) / d).mkdir(parents=True, exist_ok=True)
    print(f"Wrote {p} and created {cfg.data_root}/. Next: keel doctor, then keel record.")
    return EXIT_OK


def cmd_doctor(args) -> int:
    from keel.doctor import run_checks

    cfg = _cfg(args)
    if args.probe_references:
        from keel.doctor import probe_url

        for ref in cfg.references:
            print(f"{ref['name']}: {ref['url']}")
            for r in probe_url(ref["url"]):
                print(f"  {r['profile']:<15} {r['status']}  server={r.get('server')}  {r['excerpt'][:100]!r}")
        return EXIT_OK
    checks = asyncio.run(run_checks(cfg, network=not args.offline))
    width = max(len(c["check"]) for c in checks)
    for c in checks:
        print(f"{c['status']:<5}  {c['check']:<{width}}  {c['detail']}")
    return EXIT_REFUSED if any(c["status"] == "FAIL" for c in checks) else EXIT_OK


def cmd_record(args) -> int:
    from keel.recorder.app import Recorder

    cfg = _cfg(args)
    dur = parse_duration(args.duration) if args.duration else None
    print(f"keel-recorder {__version__}: recording to {Path(cfg.data_root).resolve()} " + (f"for {args.duration}" if dur else "until Ctrl-C"))
    async def go():
        # built inside the running loop: asyncio primitives bind to it on Python 3.9
        return await Recorder(cfg).run(duration=dur)

    final = asyncio.run(go())
    print(json.dumps(final, indent=2))
    # 3 = stalled, 4 = disk critical: non-zero so a supervisor (systemd/launchd) restarts or alerts
    return int(final.get("exit_code") or EXIT_OK)


def cmd_status(args) -> int:
    cfg = _cfg(args)
    p = Path(cfg.data_root) / "meta" / "status.json"
    if not p.exists():
        print("No status yet: the recorder has not run with this data folder.")
        return EXIT_IO
    s = json.loads(p.read_text())
    import time

    age = (time.time_ns() - s["now_ns"]) / 1e9
    state = "running" if age < 30 else f"not running (last status {age:.0f} s ago)"
    print(f"Recorder {state}. Session {s['session']}, up {s['uptime_s']} s, connected={s['connected']}, reconnects={s['reconnects']}.")
    print(f"Live outcomes {s['live_outcomes']}, subscriptions {s['subscriptions']} (refused {s['subscriptions_refused']}).")
    print(f"Messages: {s['ws_counts']}. REST requests: {s['rest_requests']}. Errors: {s['errors'] or 'none'}.")
    return EXIT_OK


def cmd_quality(args) -> int:
    from keel.quality import Window, write_report

    cfg = _cfg(args)
    window = None
    if args.session:
        from keel.recorder.store import iter_events

        ev = [e for e in iter_events(Path(cfg.data_root)) if e.get("session") == args.session and e["kind"] in ("session_start", "session_end")]
        if not ev:
            print(f"No recorder session {args.session!r} in {cfg.data_root}.")
            return EXIT_IO
        window = Window(min(e["ns"] for e in ev), max(e["ns"] for e in ev) + 1)
    elif args.day:
        d0 = datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        window = Window(int(d0.timestamp()) * 10**9, int((d0 + timedelta(days=1)).timestamp()) * 10**9)
    elif args.hours:
        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(hours=float(args.hours))
        window = Window(int(start.timestamp() * 1e9), int(end.timestamp() * 1e9))
    jp, mp, r = write_report(Path(cfg.data_root), Path(args.out) if args.out else None, window)
    if r.get("empty"):
        print(f"No recorded data under {cfg.data_root}.")
        return EXIT_IO
    for c in r["checks"]:
        print(f"{c['status']:<8} {c['check']}: {c['detail']}")
    print(f"\nReport: {mp}\nData:   {jp}")
    return EXIT_OK


def cmd_compact(args) -> int:
    from keel.compact import compact_day, days_with_raw

    cfg = _cfg(args)
    root = Path(cfg.data_root)
    now = datetime.now(tz=timezone.utc)
    if args.all:
        days = days_with_raw(root)
    elif args.day:
        days = [args.day]
    else:
        days = [(now if args.include_today else now - timedelta(days=1)).strftime("%Y-%m-%d")]
    rc = EXIT_OK
    for d in days:
        try:
            m = compact_day(root, d, force_jsonl=args.jsonl)
            print(f"{d}: {m['format']} " + ", ".join(f"{k} {v['rows']:,}" for k, v in m["tables"].items()) + " (verified)")
        except FileNotFoundError as exc:
            print(str(exc))
            rc = EXIT_IO
    return rc


def cmd_settlements(args) -> int:
    """Fetch official results for removed outcomes that have none recorded yet
    (e.g. the recorder stopped before its retries finished)."""
    from keel.hl.info import InfoClient, InfoError
    from keel.recorder.store import RawStore, iter_events

    cfg = _cfg(args)
    root = Path(cfg.data_root)
    events = list(iter_events(root))
    done = {e["instance_id"] for e in events if e["kind"] == "outcome_settled"}
    todo = [e for e in events if e["kind"] == "outcome_removed" and e["instance_id"] not in done]
    if not todo:
        print("Every removed outcome already has an official result.")
        return EXIT_OK

    async def go():
        info = InfoClient(cfg.info_url, budget_share=0.2)
        store = RawStore(root, session="settlements-backfill")
        got = 0
        for e in todo:
            oid = int(e["coin"][1:]) // 10
            body = {"type": "settledOutcome", "outcome": oid}
            try:
                text, _ = await info.post_raw(body)
            except InfoError as exc:
                print(f"{e['coin']}: {exc}")
                continue
            import time as _t

            store.write("hl_rest", text, _t.time_ns(), req=body)
            doc = json.loads(text)
            if not doc:
                print(f"{e['coin']}: no result yet")
                continue
            store.event("outcome_settled", {"instance_id": e["instance_id"], "coin": e["coin"], "outcome": oid, "settle_fraction": float(doc["settleFraction"]), "details": doc.get("details"), "description_at_settlement": (doc.get("spec") or {}).get("description"), "waited_s": None, "last_mid": e.get("last_mid"), "backfilled": True})
            got += 1
        store.close()
        return got

    got = asyncio.run(go())
    print(f"Recorded {got} of {len(todo)} official settlement results.")
    return EXIT_OK


def cmd_retain(args) -> int:
    from keel.ops import retain

    cfg = _cfg(args)
    r = retain(Path(cfg.data_root), int(args.raw_days or cfg.raw_retention_days), int(args.reference_days or cfg.broad_reference_retention_days), dry_run=args.dry_run)
    verb = "Would delete" if args.dry_run else "Deleted"
    print(f"{verb} {len(r['deleted'])} raw day folders ({r['freed_bytes'] / 1e9:.2f} GB). Kept {len(r['kept_unverified'])} not yet compacted and verified.")
    for d in r["deleted"]:
        print(f"  {d}")
    return EXIT_OK


def cmd_daily(args) -> int:
    from keel.ops import daily

    cfg = _cfg(args)
    out = daily(Path(cfg.data_root), args.day, int(args.raw_days or cfg.raw_retention_days), int(args.reference_days or cfg.broad_reference_retention_days), cfg.disk_budget_gb)
    print(json.dumps(out, indent=2, default=str))
    failed = [k for k, v in out.get("checks", {}).items() if v == "FAIL"]
    if cfg.health_fail_url and failed:
        from keel import net
        from keel.config import HTTP_HEADERS

        try:
            net.get(cfg.health_fail_url, HTTP_HEADERS, 10.0)
        except Exception:
            pass
    return EXIT_OK if not failed else EXIT_REFUSED


def cmd_bench(args) -> int:
    from keel.ops import bench

    cfg = _cfg(args)
    if not args.data:
        cfg.data_root = str(Path(cfg.data_root) / "bench")
    out = asyncio.run(bench(cfg, parse_duration(args.duration)))
    print(Path(out["report"]).read_text())
    print(f"Report: {out['report']}")
    return EXIT_OK


def cmd_health(args) -> int:
    from keel.health import make_server, render, snapshot

    cfg = _cfg(args)
    if args.once:
        out = Path(args.once)
        out.write_text(render(snapshot(Path(cfg.data_root))))
        print(f"Wrote {out}")
        return EXIT_OK
    try:
        srv = make_server(Path(cfg.data_root), args.host, args.port)
    except ValueError as exc:
        print(exc)
        return EXIT_REFUSED
    print(f"Keel health page (read-only) on http://{args.host}:{args.port}/ for {Path(cfg.data_root).resolve()}. Ctrl-C to stop.")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="keel", description="Keel: read-only recorder for Outcome (HIP-4). No keys, no orders.")
    p.add_argument("--version", action="version", version=f"keel {__version__}")
    p.add_argument("--config", help="path to keel.json (default: built-in defaults)")
    p.add_argument("--data", help="override the data folder")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("init", help="write keel.json and create the data folder")
    s.add_argument("--force", action="store_true")
    s.set_defaults(fn=cmd_init)
    s = sub.add_parser("doctor", help="check Python, disk, clock and connectivity")
    s.add_argument("--offline", action="store_true", help="skip network checks")
    s.add_argument("--probe-references", action="store_true", help="diagnose refused reference feeds: one GET per header profile")
    s.set_defaults(fn=cmd_doctor)
    s = sub.add_parser("record", help="run the recorder")
    s.add_argument("--duration", help="stop after e.g. 30m, 2h (default: run until Ctrl-C)")
    s.set_defaults(fn=cmd_record)
    s = sub.add_parser("status", help="show the running recorder's status")
    s.set_defaults(fn=cmd_status)
    s = sub.add_parser("quality", help="write the data-quality and first-census report")
    s.add_argument("--hours", help="only the last N hours")
    s.add_argument("--session", help="only one recorder session (its id is in keel status and the event log)")
    s.add_argument("--day", help="only one UTC day, YYYY-MM-DD")
    s.add_argument("--out", help="output folder (default: <data>/reports)")
    s.set_defaults(fn=cmd_quality)
    s = sub.add_parser("retain", help="delete raw days past retention (only days whose tables verify)")
    s.add_argument("--raw-days", help="default: raw_retention_days in keel.json (14)")
    s.add_argument("--reference-days", help="retention for the broad Polymarket/Kalshi polls (default 7)")
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(fn=cmd_retain)
    s = sub.add_parser("daily", help="compact yesterday, write its quality report, apply retention")
    s.add_argument("--day", help="YYYY-MM-DD (default: yesterday UTC)")
    s.add_argument("--raw-days")
    s.add_argument("--reference-days")
    s.set_defaults(fn=cmd_daily)
    s = sub.add_parser("bench", help="measure what recording costs on this host (CPU, memory, disk, download, latency)")
    s.add_argument("--duration", default="10m")
    s.set_defaults(fn=cmd_bench)
    s = sub.add_parser("settlements", help="fetch official results for removed outcomes that lack one")
    s.set_defaults(fn=cmd_settlements)
    s = sub.add_parser("compact", help="compact raw data into daily tables (default: yesterday UTC)")
    s.add_argument("--day", help="YYYY-MM-DD")
    s.add_argument("--all", action="store_true", help="every day with raw data")
    s.add_argument("--include-today", action="store_true")
    s.add_argument("--jsonl", action="store_true", help="write gzip JSON lines even if pyarrow is installed")
    s.set_defaults(fn=cmd_compact)
    s = sub.add_parser("health", help="serve the read-only Health page on 127.0.0.1 (use an SSH tunnel to view remotely)")
    s.add_argument("--host", default="127.0.0.1", help="loopback only: 127.0.0.1, ::1 or localhost")
    s.add_argument("--port", type=int, default=8787)
    s.add_argument("--once", metavar="FILE", help="write the page to FILE once instead of serving it")
    s.set_defaults(fn=cmd_health)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.fn(args)
    except KeyboardInterrupt:
        return EXIT_OK
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_REFUSED


if __name__ == "__main__":
    raise SystemExit(main())
