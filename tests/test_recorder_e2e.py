"""End-to-end: recorder against local fakes fed with real payloads, then the
quality report and compaction over what it wrote."""
import asyncio
import json

from fakes import FakeInfo, FakeWS
from keel.cli import main
from keel.compact import compact_day, days_with_raw, read_table, verify_day
from keel.config import RecorderConfig
from keel.quality import build_report, render_markdown
from keel.recorder.app import Recorder, infer_settlement
from keel.recorder.store import iter_events, iter_raw


def _cfg(tmp_path, info_url, ws_url, **kw):
    base = dict(
        data_root=str(tmp_path / "data"), info_url=info_url, ws_url=ws_url, meta_every="1s", audit_every="1s",
        audit_per_cycle=3, mirror_audit_every="1s", perp_ctx_every="1s", status_every="1s", flush_every="1s",
        unsubscribe_grace="1s", settlement_checks=["0s", "1s"], pm_live=False, book_mode="standard", deployers=None, references=[{"name": "dead", "url": "https://127.0.0.1:1/x", "every": "1s"}],
    )
    base.update(kw)
    return RecorderConfig(**base)


def _run(cfg, info, seconds, during=None, ws_kwargs=None):
    async def go():
        ws = await FakeWS(**(ws_kwargs or {})).start()
        cfg.ws_url = ws.url
        rec = Recorder(cfg)
        task = asyncio.ensure_future(rec.run(duration=seconds))
        if during:
            await during(rec)
        final = await task
        await ws.stop()
        return final, ws

    return asyncio.run(go())


def test_record_settlement_report_and_compaction(tmp_path):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "")

        async def settle_one(rec):
            await asyncio.sleep(2.5)
            info.remove_outcome(43639)  # e.g. Galatasaray market settles and disappears

        final, ws = _run(cfg, info, 7.0, during=settle_one, ws_kwargs={"mid": "0.995"})

    root = tmp_path / "data"
    assert final["live_outcomes"] == 7
    assert final["ws_counts"]["l2Book"] > 20 and final["ws_counts"]["trades"] >= 8
    kinds = [e["kind"] for e in iter_events(root)]
    for k in ("session_start", "ws_connected", "outcome_added", "outcome_removed", "unsubscribed", "reference_error", "session_end"):
        assert k in kinds, k
    removed = [e for e in iter_events(root) if e["kind"] == "outcome_removed"][0]
    assert removed["coin"] == "#436390" and removed["settle_fraction"] == 1.0 and removed["settle_source"] == "final_mid_inferred"
    assert removed["last_book_recv_ns"] is not None
    settled = [e for e in iter_events(root) if e["kind"] == "outcome_settled"]
    assert len(settled) == 1 and settled[0]["settle_fraction"] == 1.0 and settled[0]["outcome"] == 43639
    # the recorder only ever asked for public read-only info types
    assert {r["type"] for r in info.requests} <= {"outcomeMeta", "l2Book", "recentTrades", "metaAndAssetCtxs", "settledOutcome"}
    srcs = {r["src"] for r in iter_raw(root)}
    assert srcs == {"hl_ws", "hl_rest"}

    r = build_report(root)
    checks = {c["check"]: c["status"] for c in r["checks"]}
    assert checks["No crossed books"] == "PASS"
    assert checks["All prices on the 0.00001 tick"] == "PASS"
    assert checks["Side-1 trades mirror side-0"] == "PASS"
    assert checks["100% settlement capture"] == "PASS"
    assert checks["Official settlement result for every removal"] == "PASS"
    assert r["coverage"]["reconnects"] == 0
    assert checks["No parse failures"] == "PASS"
    assert r["rest_audit"]["compared"] > 0 and r["rest_audit"]["agree_pct"] == 100.0
    assert r["trades"]["duplicates_dropped"] == 0 and r["trades"]["unique"] == 16  # 2 trades x 8 coins, deduped by tid
    assert r["census"]["out/sports"]["median_spread_c"] == 0.001
    assert r["settlements"]["capture_pct"] == 100.0
    md = render_markdown(r)
    assert "# Keel data-quality report" in md and "out/sports" in md

    days = days_with_raw(root)
    m = compact_day(root, days[0], force_jsonl=True)
    verify_day(root, days[0])
    books = read_table(root / m["tables"]["books"]["file"])
    assert books and books[0]["bid_px7"] == 3_183_400 and books[0]["ask_px7"] == 3_183_500
    trades = read_table(root / m["tables"]["trades"]["file"])
    assert len({(t["coin"], t["tid"]) for t in trades}) == len(trades)


def test_reconnects_are_masked_and_trade_backfill_deduped(tmp_path):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "", references=[])
        final, ws = _run(cfg, info, 4.0, ws_kwargs={"drop_after": 1.2})
    assert ws.connections >= 2
    r = build_report(tmp_path / "data")
    assert r["coverage"]["reconnects"] >= 1
    # the fake re-sends the same trade backfill on every reconnect: kept once
    assert r["trades"]["duplicates_dropped"] > 0
    assert r["trades"]["unique"] == 16


def test_rest_failures_do_not_stop_recording(tmp_path):
    with FakeInfo() as info:
        info.fail_types = {"l2Book", "metaAndAssetCtxs"}
        cfg = _cfg(tmp_path, info.url, "", references=[])
        final, _ = _run(cfg, info, 2.5)
    assert final["errors"].get("rest_l2Book_audit", 0) >= 1
    assert final["ws_counts"]["l2Book"] > 10


def test_infer_settlement_is_conservative():
    assert infer_settlement(None)["settle_fraction"] is None
    assert infer_settlement(0.5)["settle_source"] == "unknown_mid_not_extreme"
    assert infer_settlement(0.004)["settle_fraction"] == 0.0


def test_cli_init_status_quality_compact(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert main(["init"]) == 0
    assert main(["init"]) == 2  # refuses to overwrite
    cfg = json.loads((tmp_path / "keel.json").read_text())
    assert cfg["max_subscriptions"] == 900
    assert main(["--config", "keel.json", "quality"]) == 4  # no data yet
    with FakeInfo() as info:
        c = _cfg(tmp_path, info.url, "", references=[])
        c.data_root = "data"
        _run(c, info, 2.0)
    assert main(["status"]) == 0
    assert main(["quality"]) == 0
    assert main(["compact", "--all", "--jsonl"]) == 0
    assert main(["doctor", "--offline"]) in (0, 2)
    out = capsys.readouterr().out
    assert "Report:" in out and "verified" in out


def test_config_validation(tmp_path):
    import pytest

    from keel.config import load_config

    p = tmp_path / "k.json"
    p.write_text(json.dumps({"meta_every": 60}))
    with pytest.raises(ValueError):
        load_config(p)
    p.write_text(json.dumps({"surprise": 1}))
    with pytest.raises(ValueError):
        load_config(p)
    p.write_text(json.dumps({"max_subscriptions": 5000}))
    with pytest.raises(ValueError):
        load_config(p)


def test_fast_book_mode_stores_only_changes_and_heartbeats(tmp_path):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "", references=[], book_mode="fast", book_heartbeat="1s", fast_venues=["out"], fast_categories=["sports"], fast_tiering=False)
        final, ws = _run(cfg, info, 3.0, ws_kwargs={"tick": 0.05})
    fast_subs = [s for s in ws.subscribe_msgs if s.get("fast")]
    # Outcome-deployed sports outcomes in the fixture; the fallback outcome is "other"
    assert {s["coin"] for s in fast_subs} == {"#436390", "#436840", "#436850", "#436860"}
    assert all(s["type"] == "l2Book" for s in fast_subs)
    assert final["fast_book_coins"] == len({s["coin"] for s in fast_subs})
    skipped = final["ws_counts"].get("l2Book_unchanged_not_stored", 0)
    assert skipped > 0
    # identical books: per fast coin, stored snapshots are about one per heartbeat (first + ~1/s)
    from collections import Counter
    stored = Counter(r["msg"]["data"]["coin"] for r in iter_raw(tmp_path / "data") if isinstance(r["msg"], dict) and r["msg"].get("channel") == "l2Book")
    for s in fast_subs:
        assert 1 <= stored[s["coin"]] <= 5
    r = build_report(tmp_path / "data")
    assert r["books"]["mode"] == "fast" and r["books"]["unchanged_fast_snapshots_not_stored"] == skipped


def test_settlement_backfill_command(tmp_path, monkeypatch):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "", references=[], settlement_checks=["30m"])  # recorder stops before checking

        async def settle_one(rec):
            await asyncio.sleep(1.5)
            info.remove_outcome(43639, settle="0.0")

        _run(cfg, info, 3.0, during=settle_one)
        root = tmp_path / "data"
        assert not [e for e in iter_events(root) if e["kind"] == "outcome_settled"]
        (tmp_path / "k.json").write_text(json.dumps({"data_root": str(root), "info_url": info.url, "references": []}))
        assert main(["--config", str(tmp_path / "k.json"), "settlements"]) == 0
    s = [e for e in iter_events(root) if e["kind"] == "outcome_settled"]
    assert len(s) == 1 and s[0]["settle_fraction"] == 0.0 and s[0]["backfilled"] is True
    r = build_report(root)
    assert {c["check"]: c["status"] for c in r["checks"]}["Official settlement result for every removal"] == "PASS"
    # records written later by other commands must not stretch the scored span
    # (regression from the first live run, where a back-fill 2 h later made coverage 21%)
    from keel.recorder.store import RawStore
    import time as _t

    later = RawStore(root, session="later-backfill")
    later.write("hl_rest", "null", _t.time_ns() + 2 * 3600 * 10**9, req={"type": "settledOutcome", "outcome": 1})
    later.close()
    assert build_report(root)["span"]["hours"] < 0.1


def test_reference_probe_reports_each_header_profile():
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from keel.doctor import PROBE_PROFILES, probe_url

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            ok = "curl" in (self.headers.get("user-agent") or "")
            self.send_response(200 if ok else 406)
            self.end_headers()
            self.wfile.write(b"[]" if ok else b"not acceptable")

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        res = {r["profile"]: r["status"] for r in probe_url(f"http://127.0.0.1:{srv.server_address[1]}/events")}
    finally:
        srv.shutdown()
    assert set(res) == set(PROBE_PROFILES)
    assert res["curl-like"] == 200 and res["keel-default"] == 406


def test_reference_feed_requiring_gzip_is_decompressed_and_stored(tmp_path):
    """Regression from the live run: Polymarket returns 406 'gzip_required' without
    Accept-Encoding: gzip."""
    import gzip as _gz
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from keel import net

    payload = json.dumps([{"title": "Brentford FC vs. Everton FC", "tags": [{"slug": "sports"}]}] * 50).encode()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if "gzip" not in (self.headers.get("accept-encoding") or ""):
                self.send_response(406)
                self.end_headers()
                self.wfile.write(b'{"error":"gzip_required"}')
                return
            body = _gz.compress(payload)
            self.send_response(200)
            self.send_header("content-encoding", "gzip")
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}/events"
    try:
        text, wire, status = net.get(url)
        assert status == 200 and json.loads(text)[0]["title"].startswith("Brentford") and wire < len(payload)

        async def go():
            rec = Recorder(_cfg(tmp_path, "http://127.0.0.1:9/info", "ws://127.0.0.1:9/ws", references=[]))
            await rec.poll_reference({"name": "pm", "url": url, "every": "60s"})
            rec.store.close()
            return rec

        rec = asyncio.run(go())
    finally:
        srv.shutdown()
    recs = [r for r in iter_raw(tmp_path / "data") if r["src"] == "ref_pm"]
    assert len(recs) == 1 and recs[0]["msg"][0]["tags"][0]["slug"] == "sports"
    assert recs[0]["req"]["wire_bytes"] < len(payload)


def test_stalled_stream_exits_with_code_3(tmp_path):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "", references=[], stall_exit_after="2s")
        final, _ = _run(cfg, info, 15.0, ws_kwargs={"tick": 1000})  # server never pushes data
    assert final["exit_code"] == 3
    assert "recorder_stalled" in [e["kind"] for e in iter_events(tmp_path / "data")]


def test_disk_guard_stops_cleanly_with_code_4(tmp_path):
    with FakeInfo() as info:
        cfg = _cfg(tmp_path, info.url, "", references=[], stall_exit_after="2s", disk_warn_gb=1e12, disk_stop_gb=1e11)
        final, _ = _run(cfg, info, 15.0)
    assert final["exit_code"] == 4
    kinds = [e["kind"] for e in iter_events(tmp_path / "data")]
    assert "disk_critical" in kinds and kinds[-1] == "session_end"
