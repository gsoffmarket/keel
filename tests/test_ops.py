import asyncio
import json
from datetime import datetime, timezone

from fakes import FakeInfo, FakeWS
from keel.compact import compact_day
from keel.config import RecorderConfig
from keel.ops import bench, daily, retain
from keel.recorder.store import RawStore, raw_files

DAY1 = 1795000000 * 10**9  # 2026-11-18
DAY2 = DAY1 + 86400 * 10**9


def _write(root, ns, src="hl_ws"):
    st = RawStore(root, session=f"s{ns}")
    st.write(src, json.dumps({"channel": "l2Book", "data": {"coin": "#10", "time": ns // 10**6, "levels": [[{"px": "0.5", "sz": "1", "n": 1}], [{"px": "0.51", "sz": "1", "n": 1}]]}}), ns)
    st.close()


def test_retention_deletes_only_verified_old_days(tmp_path):
    _write(tmp_path, DAY1)
    _write(tmp_path, DAY2)
    _write(tmp_path, DAY1, src="ref_polymarket_sports")
    compact_day(tmp_path, "2026-11-18", force_jsonl=True)  # day 2 deliberately not compacted
    now = datetime(2026, 12, 30, tzinfo=timezone.utc)
    dry = retain(tmp_path, raw_days=30, broad_reference_days=14, dry_run=True, now=now)
    assert sorted(dry["deleted"]) == ["raw/hl_ws/2026-11-18", "raw/ref_polymarket_sports/2026-11-18"]
    assert dry["kept_unverified"] == ["raw/hl_ws/2026-11-19"]
    assert len(raw_files(tmp_path)) == 3  # dry run touched nothing
    r = retain(tmp_path, raw_days=30, broad_reference_days=14, now=now)
    assert r["freed_bytes"] > 0 and [p.parts[-2] for p in raw_files(tmp_path)] == ["2026-11-19"]
    assert (tmp_path / "tables" / "manifest-2026-11-18.json").exists()


def test_daily_job_compacts_and_reports(tmp_path):
    _write(tmp_path, DAY1)
    out = daily(tmp_path, "2026-11-18")
    assert out["compaction"]["books"] == 1
    assert (tmp_path / "reports" / "daily" / "daily-2026-11-18.json").exists()


def test_bench_reports_resources_and_projections(tmp_path):
    with FakeInfo() as info:
        async def go():
            ws = await FakeWS(tick=0.05).start()
            cfg = RecorderConfig(data_root=str(tmp_path / "b"), info_url=info.url, ws_url=ws.url, references=[], pm_live=False,
                                 meta_every="1s", audit_every="1s", status_every="1s", flush_every="1s", book_heartbeat="1s")
            out = await bench(cfg, 3.0, sample_every=0.5)
            await ws.stop()
            return out

        out = asyncio.run(go())
    assert out["system"]["cpus"] and out["duration_s"] >= 2.5
    assert out["download_bytes"] > 0 and out["disk_bytes"] > 0 and out["rss_mb_max"] > 0
    assert out["cpu_pct_mean"] is not None and len(out["samples"]) >= 3
    assert "# Keel host benchmark" in open(out["report"]).read()


def test_budget_deletes_oldest_verified_days_first(tmp_path):
    from keel.ops import data_bytes, enforce_budget

    d3 = DAY2 + 86400 * 10**9  # 2026-11-20
    for ns in (DAY1, DAY2, d3):
        for i in range(30):
            _write(tmp_path, ns + i)
    compact_day(tmp_path, "2026-11-18", force_jsonl=True)
    compact_day(tmp_path, "2026-11-20", force_jsonl=True)  # 11-19 left unverified
    now = datetime(2026, 11, 22, 12, tzinfo=timezone.utc)
    res = enforce_budget(tmp_path, budget_bytes=1, now=now, keep_days=1)
    assert res["deleted_days"] == ["2026-11-18", "2026-11-20"] and res["skipped_unverified"] == ["2026-11-19"]
    assert not res["within_budget"]  # unverified raw is never deleted, even over budget
    assert [p.parts[-2] for p in raw_files(tmp_path)] == ["2026-11-19"] * 30
    roomy = enforce_budget(tmp_path, budget_bytes=data_bytes(tmp_path) + 1, now=now)
    assert roomy["within_budget"] and roomy["deleted_days"] == []


def test_activity_tiering_promotes_demotes_and_logs(tmp_path):
    import time

    from keel.recorder.app import Recorder
    from keel.recorder.store import iter_events

    async def go():
        cfg = RecorderConfig(data_root=str(tmp_path / "d"), info_url="http://127.0.0.1:9/info", ws_url="ws://127.0.0.1:9/ws", references=[], pm_live=False,
                             fast_idle="60s", fast_min_dwell="10s", fast_window="6h")
        rec = Recorder(cfg)
        coin = "#10"
        rec.event_ns[coin] = None
        rec.book_subs[coin] = rec._book_sub(coin, "out", "sports")
        assert rec.tier[coin] == "standard" and coin in rec.eligible_fast  # outside its window: starts standard
        # subscription backfill (an old trade) does not count as activity
        rec._mark_active(coin, "trade", '{"channel":"trades","data":[{"coin":"#10","time":1700000000000}]}')
        await asyncio.sleep(0.05)
        assert rec.tier[coin] == "standard"
        # a live trade promotes it, subscribing to the fast stream before dropping the standard one
        rec._mark_active(coin, "trade", '{"channel":"trades","data":[{"coin":"#10","time":%d}]}' % int(time.time() * 1000))
        await asyncio.sleep(0.05)
        assert rec.tier[coin] == "fast" and coin in rec.fast_coins
        subs = list(rec.stream.subs.values())
        assert {"type": "l2Book", "coin": coin, "fast": True} in subs and {"type": "l2Book", "coin": coin} not in subs
        # idle beyond fast_idle and past the minimum dwell: demoted
        rec.activity[coin] = time.monotonic() - 120
        rec.tier_since[coin] = time.monotonic() - 60
        await rec.retier()
        assert rec.tier[coin] == "standard" and coin not in rec.fast_coins
        # entering the event window promotes without activity
        rec.event_ns[coin] = time.time_ns() + 3600 * 10**9
        await rec.retier()
        assert rec.tier[coin] == "fast"
        rec.store.close()

    asyncio.run(go())
    changes = [(e["tier"], e["reason"]) for e in iter_events(tmp_path / "d") if e["kind"] == "tier_change"]
    assert changes == [("standard", "initial"), ("fast", "trade"), ("standard", "idle"), ("fast", "window")]
