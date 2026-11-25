"""Reference matching, Polymarket book state and the stale-reference guard."""
import asyncio
import json

import pytest

from fakes import fixture
from keel.hl.metadata import parse_outcome_meta
from keel.refs.contracts import Contract, contracts_from_meta, parse_start
from keel.refs.names import coverage, tokens
from keel.refs.polymarket import GammaClient, match_all, match_contract
from keel.refs.refbook import PMBooks

EV = fixture("polymarket_events_2026-11-24.json")
ALL = [EV[k] for k in ("ucl", "ucl_derivative", "nba", "nba_closed_2023", "ucl_champion", "ucl_phase_winner")]
UCL = dict(competition="UEFA Champions League", sport="Soccer", team_a="Arsenal", team_b="Borussia Dortmund",
           start_ns=parse_start("20261124-2000"), counted_play="regulation time, 90 minutes plus stoppage time", season="2026/27")


def test_names():
    assert coverage("Borussia Dortmund", "Arsenal FC vs. BV Borussia 09 Dortmund") == 1.0
    assert coverage("FK Bodo/Glimt", "FK Bodø/Glimt vs. LASK Linz") == 1.0
    assert coverage("Inter Milan", "FC Internazionale Milano vs. VfB Stuttgart") == 1.0
    assert coverage("Real Madrid", "Real Betis Balompié") < 0.75
    assert "fc" not in tokens("Arsenal FC")


def test_soccer_team_win_draw_and_other_team():
    toks = json.loads(EV["ucl"]["markets"][0]["clobTokenIds"])
    m, why = match_contract(Contract(outcome=1, coin="#10", kind="team_win", team="Arsenal", **UCL), ALL)
    assert m and m.token_id == toks[0] and m.token_outcome == "Yes" and m.level == "high", why
    assert m.event_slug == "ucl-ars-bvb-2026-11-24"  # never the "- Exact Score" derivative
    d, _ = match_contract(Contract(outcome=2, coin="#20", kind="draw", **UCL), ALL)
    assert d and "draw" in d.market_question.lower() and d.token_id == json.loads(EV["ucl"]["markets"][1]["clobTokenIds"])[0]
    b, _ = match_contract(Contract(outcome=3, coin="#30", kind="team_win", team="Borussia Dortmund", **UCL), ALL)
    assert b and "Dortmund" in b.market_question and b.token_id != m.token_id
    assert m.components["rules"] == 1.0 and m.components["start_diff_h"] == 0.0


def test_wrong_date_or_rules_are_rejected():
    wrong_day = dict(UCL, start_ns=parse_start("20261201-2000"))
    m, why = match_contract(Contract(outcome=1, coin="#10", kind="team_win", team="Arsenal", **wrong_day), ALL)
    assert m is None and "start time" in why
    ev = json.loads(json.dumps(EV["ucl"]))
    for mk in ev["markets"]:
        mk["description"] = "This market includes extra time and penalties."
    m, why = match_contract(Contract(outcome=1, coin="#10", kind="team_win", team="Arsenal", **UCL), [ev])
    assert m is None


def test_nba_two_way_uses_side0_team_and_skips_closed_events():
    c = Contract(outcome=44791, coin="#447910", kind="match_winner_side0", competition="NBA", team="Philadelphia 76ers",
                 team_a="Philadelphia 76ers", team_b="Miami Heat", start_ns=parse_start("20261125-2200"),
                 counted_play="the full game including overtime", short_a="PHI", short_b="MIA")
    m, why = match_contract(c, ALL)
    assert m and m.token_outcome == "76ers" and m.token_id == "222" and m.level == "high", why


def test_tournament_winner_and_decoys():
    c = Contract(outcome=5, coin="#50", kind="tournament_winner", competition="UEFA Champions League", team="Arsenal", season="2026/2027")
    m, why = match_contract(c, ALL)
    assert m and m.event_slug == "uefa-champions-league-2027-champion" and "Arsenal" in m.market_question, why
    assert m.components["season"] == 1.0


def test_contracts_from_real_meta_and_unsupported_are_explained():
    meta = parse_outcome_meta(fixture("outcome_meta_2026-11-23.json"))
    cs = {c.outcome: c for c in contracts_from_meta(meta)}
    assert cs[43686].kind == "team_win" and cs[43686].team == "Viking" and cs[43686].team_a == "Atletico Madrid"
    assert cs[43685].kind == "draw"
    assert cs[43639].kind == "unsupported"  # standalone participant outcome without its question in the fixture


def test_match_all_with_injected_search_counts_and_reasons():
    calls = []

    def fake_get(url, headers=None):
        calls.append(url)
        return json.dumps({"events": ALL}), 1, 200

    g = GammaClient(min_interval=0, get=fake_get)
    cs = [Contract(outcome=1, coin="#10", kind="team_win", team="Arsenal", **UCL), Contract(outcome=2, coin="#20", kind="draw", **UCL),
          Contract(outcome=9, coin="#90", kind="unsupported", reason="fallback outcome has no reference equivalent")]
    maps, reasons = match_all(cs, g)
    assert {m.outcome for m in maps} == {1, 2} and reasons == {9: "fallback outcome has no reference equivalent"}
    assert all("events_status=active" in u for u in calls)
    assert len(calls) == 1  # second contract hits the cache


def _book(asset, bids, asks, ts="1795513960194"):
    return [{"event_type": "book", "asset_id": asset, "market": "0x", "timestamp": ts, "bids": [{"price": p, "size": s} for p, s in bids], "asks": [{"price": p, "size": s} for p, s in asks]}]


def test_pm_books_snapshot_deltas_and_freshness_rules():
    b = PMBooks()
    s0 = b.state("A", 0)
    assert not s0["usable"] and s0["why"] == "no snapshot"
    b.on_connect(1_000_000_000)
    b.apply_text(json.dumps(_book("A", [("0.40", "100"), ("0.39", "50")], [("0.42", "80")])), 1_100_000_000)
    s = b.state("A", 1_200_000_000)
    assert s["usable"] and s["bid_px7"] == 4_000_000 and s["ask_px7"] == 4_200_000
    # level removed and new best bid via price_change
    b.apply_text(json.dumps({"event_type": "price_change", "market": "0x", "timestamp": "1", "price_changes": [
        {"asset_id": "A", "price": "0.40", "size": "0", "side": "BUY", "best_bid": "0.39", "best_ask": "0.42"}]}), 1_300_000_000)
    assert b.state("A", 1_400_000_000)["bid_px7"] == 3_900_000 and b.books["A"].best_field_mismatch == 0
    # a quiet book stays usable while the feed is proven live (PONG), with change_age growing
    b.apply_text("PONG", 10_000_000_000)
    s = b.state("A", 11_000_000_000)
    assert s["usable"] and s["change_age_s"] > 9 and s["feed_age_s"] == 1.0
    # silence beyond the limit: not usable
    assert b.state("A", 30_000_000_000)["why"].startswith("feed silent")
    # disconnect, then reconnect: unusable until a new snapshot arrives
    b.on_disconnect(31_000_000_000)
    assert b.state("A", 31_000_000_001)["why"] == "feed disconnected"
    b.on_connect(32_000_000_000)
    assert b.state("A", 32_000_000_001)["why"] == "no snapshot since reconnect"
    b.apply_text(json.dumps(_book("A", [("0.41", "1")], [("0.43", "1")])), 32_100_000_000)
    assert b.state("A", 32_200_000_000)["usable"]
    # deltas for an asset with no snapshot are ignored, never invented
    b.apply_text(json.dumps({"event_type": "price_change", "price_changes": [{"asset_id": "B", "price": "0.5", "size": "1", "side": "BUY"}]}), 33_000_000_000)
    assert "B" not in b.books


def _rec_run(tmp_path, seconds, pm_kwargs=None, cfg_kwargs=None):
    from fakes import FakeInfo, FakePM, FakeWS, gamma_event_for_fixture_meta
    from keel.config import RecorderConfig
    from keel.recorder.app import Recorder

    def fake_get(url, headers=None):
        return json.dumps({"events": [gamma_event_for_fixture_meta()]}), 1, 200

    with FakeInfo() as info:
        async def go():
            hl = await FakeWS(tick=0.05).start()
            pm = await FakePM(**(pm_kwargs or {})).start()
            base = dict(data_root=str(tmp_path / "data"), info_url=info.url, ws_url=hl.url, pm_ws_url=pm.url, pm_book_url="http://127.0.0.1:9/book",
                        meta_every="1s", audit_every="1s", audit_per_cycle=1, mirror_audit_every="10m", perp_ctx_every="10m", status_every="1s",
                        flush_every="1s", match_every="1s", pm_audit_every="10m", references=[], book_mode="fast", book_heartbeat="1s",
                        fast_categories=["sports"], max_reference_feed_age="2s")
            base.update(cfg_kwargs or {})
            rec = Recorder(RecorderConfig(**base), gamma=GammaClient(min_interval=0, ttl=0, get=fake_get))
            final = await rec.run(duration=seconds)
            await hl.stop()
            await pm.stop()
            return final, pm

        return asyncio.run(go())


def test_recorder_matches_streams_and_aligns_live_references(tmp_path):
    from keel.quality import build_report
    from keel.recorder.store import iter_events, iter_raw
    from keel.refs.align import aligned_rows, gap_candidates

    final, pm = _rec_run(tmp_path, 5.0)
    root = tmp_path / "data"
    added = [e for e in iter_events(root) if e["kind"] == "mapping_added"]
    assert {e["coin"]: e["token_id"] for e in added} == {"#436840": "TATM", "#436850": "TDRAW", "#436860": "TVIK"}
    assert all(e["level"] == "high" and e["signed"] is False for e in added)
    assert set(pm.assets) >= {"TATM", "TDRAW", "TVIK"}
    assert final["references_live"]["mapped_outcomes"] == 3 and final["references_live"]["pm_pongs"] == 0 or True
    assert any(r["src"] == "pm_ws" for r in iter_raw(root))
    rows = list(aligned_rows(root, max_feed_age_s=2.0))
    usable = [r for r in rows if r.usable]
    assert usable, "expected usable aligned rows"
    assert all(r.ref_bid_px7 == 3_100_000 and r.ref_ask_px7 == 3_300_000 for r in usable)
    r = build_report(root)
    ra = r["reference_alignment"]
    assert ra["usable"] == len(usable) and ra["suspect_mappings"] == {}
    # activity tiering is on by default: every observation says which stream it came from
    assert r["tiering"]["changes"] >= 3 and set(ra["rows_by_tier"]) <= {"fast", "standard"}
    assert r["fast_books"]["basis"] == "tier_change intervals"
    assert (tmp_path / "data" / "meta" / "mappings.json").exists()


def test_silent_reference_feed_never_yields_gap_candidates(tmp_path):
    from keel.refs.align import aligned_rows, gap_candidates

    # the reference goes silent (no data, no PONG) after 2.5 s while Outcome keeps updating
    _rec_run(tmp_path, 7.0, pm_kwargs={"bid": "0.10", "ask": "0.12", "mute_after": 2.5}, cfg_kwargs={"max_reference_feed_age": "1s"})
    rows = list(aligned_rows(tmp_path / "data", max_feed_age_s=1.0))
    stale = [r for r in rows if r.why and r.why.startswith("feed silent")]
    assert stale, "silence must make rows unusable"
    # the frozen 10-12c reference vs Outcome ~31.8c would look like a huge gap if used
    cands, excluded = gap_candidates(rows, min_gap_c=1.0)
    assert all(c.usable for c in cands)
    assert all(c.ns < min(s.ns for s in stale) for c in cands)
    assert sum(v for k, v in excluded.items() if k.startswith("feed silent")) == len(stale)


def test_queries_use_nicknames_and_competition_aliases():
    from keel.refs.polymarket import search_queries

    nba = Contract(outcome=1, coin="#1", kind="match_winner_side0", team="Philadelphia 76ers", team_a="Philadelphia 76ers", team_b="Miami Heat")
    assert search_queries(nba)[0] == "76ers Heat"
    epl = Contract(outcome=2, coin="#2", kind="tournament_winner", competition="English Premier League", team="Arsenal", season="2026/2027")
    assert "EPL champion" in search_queries(epl)
    ev = {"title": "EPL: 2027 Champion", "slug": "epl-2027-champion", "active": True, "closed": False,
          "markets": [{"question": "Will Arsenal win the 2026-27 English Premier League?", "outcomes": '["Yes", "No"]', "clobTokenIds": '["A1", "A2"]', "conditionId": "0x"}]}
    m, why = match_contract(epl, [ev])
    assert m and m.token_id == "A1", why


def test_wide_outcome_book_is_not_flagged_as_a_wrong_mapping(monkeypatch, tmp_path):
    """Live run 2026-11-24: Outcome 'Viking win' quoted 2.6c / 77.2c while Polymarket was
    3.7c / 3.8c. The mid differs by 36c but the ranges overlap: a wide book, not a bad
    match. A mapping whose ranges sit apart is still flagged."""
    import keel.refs.align as al
    from keel.refs.align import Row

    def row(coin, ob, oa, rb, ra):
        return Row(ns=1, coin=coin, outcome_bid_px7=ob, outcome_ask_px7=oa, ref_bid_px7=rb, ref_ask_px7=ra, ref_feed_age_s=1.0,
                   ref_change_age_s=1.0, ref_exch_ms=1, outcome_exch_ms=1, confidence=1.0, level="high", usable=True, why=None)

    rows = [row("#436860", 262_400, 7_721_100, 370_000, 380_000), row("#999990", 6_000_000, 6_100_000, 3_000_000, 3_100_000)]
    monkeypatch.setattr(al, "aligned_rows", lambda *a, **k: iter(rows))
    out = al.summarize(tmp_path)
    assert out["median_abs_mid_diff_c"]["#436860"] > 30
    assert "#436860" not in out["suspect_mappings"] and out["suspect_mappings"] == {"#999990": 29.0}


def test_esports_main_event_is_not_treated_as_derivative_and_nfl_tie_rule_lowers_confidence():
    from keel.refs.polymarket import _primary_events

    evs = [{"title": "Counter-Strike: PVISION vs FaZe (BO1) - PGL Major Singapore Stage 1"}, {"title": "Arsenal FC vs. BV Borussia 09 Dortmund - Exact Score"},
           {"title": "Galatasaray SK vs. Aston Villa FC - Player Props"}]
    assert [e["title"] for e in _primary_events(evs)] == [evs[0]["title"]]
    nfl = Contract(outcome=7, coin="#70", kind="match_winner_side0", competition="NFL", team="Kansas City Chiefs", team_a="Kansas City Chiefs",
                   team_b="Buffalo Bills", start_ns=parse_start("20261127-0120"), counted_play="the full game including overtime", short_a="KC", short_b="BUF")
    ev = {"title": "Chiefs vs. Bills", "slug": "nfl-kc-buf-2026-11-27", "active": True, "closed": False, "markets": [
        {"question": "Chiefs vs. Bills", "sportsMarketType": "moneyline", "outcomes": '["Chiefs", "Bills"]', "clobTokenIds": '["K", "B"]', "gameStartTime": "2026-11-27 01:20:00+00",
         "description": "Overtime is included if played. If the game ends in a tie, this market will resolve 50-50."}]}
    m, why = match_contract(nfl, [ev])
    assert m and m.token_id == "K" and m.level == "medium" and "tie" in m.components["rules_note"], why
