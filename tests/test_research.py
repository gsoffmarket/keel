"""M1 research engine: MDE maths, snapshots, universe clustering, census reproducibility,
maker census, simulator mechanics and registry rules."""
import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from keel.research import mde as M

DAY = "2026-11-25"
T0 = 1795564800 * 10**9  # 2026-11-25 00:00:00 UTC
S = 10**9


# ---------------------------------------------------------------- helpers

def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _write_table(root: Path, name: str, rows):
    p = root / "tables" / name / f"date={DAY}" / "part-0.jsonl.gz"
    p.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(p, "wt") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    return p


def _levels(bids, asks):
    return json.dumps([[{"px": f"{p:.5f}", "sz": f"{s:.2f}", "n": 1} for p, s in bids], [{"px": f"{p:.5f}", "sz": f"{s:.2f}", "n": 1} for p, s in asks]])


def _book(coin, t_s, bids, asks):
    ns = T0 + int(t_s * S)
    return {"recv_ns": ns + 50 * 10**6, "exch_ms": ns // 10**6, "coin": coin,
            "bid_px7": int(round(bids[0][0] * 1e7)) if bids else None, "bid_sz6": None,
            "ask_px7": int(round(asks[0][0] * 1e7)) if asks else None, "ask_sz6": None,
            "n_bid_levels": len(bids), "n_ask_levels": len(asks), "levels_json": _levels(bids, asks)}


def _trade(coin, t_s, px, sz, aggressor, tid, buyer="0xb", seller="0xs"):
    ns = T0 + int(t_s * S)
    return {"recv_ns": ns + 50 * 10**6, "exch_ms": ns // 10**6, "coin": coin, "tid": tid, "aggressor": aggressor,
            "px7": int(round(px * 1e7)), "sz6": int(round(sz * 1e6)), "hash": None, "buyer": buyer, "seller": seller}


META = {
    "outcomes": [
        {"outcome": 100, "name": "template:sportsContestParticipant2", "description": "participant:Alpha", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"},
        {"outcome": 101, "name": "template:sportsContestParticipant2", "description": "participant:Beta", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"},
        {"outcome": 102, "name": "template fallback", "description": "other", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"},
        {"outcome": 200, "name": "template:binaryPrice", "description": "perp:BTC|threshold:100000|time:20261125-1200", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"},
        {"outcome": 201, "name": "template:binaryPrice", "description": "perp:BTC|threshold:101000|time:20261125-1200", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"},
        {"outcome": 202, "name": "template:binaryPrice", "description": "perp:ETH|threshold:4000|time:20261125-1200", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"},
        {"outcome": 300, "name": "template:sportsContestWinner", "description": "competition:NBA|contestType:Match|participantA:Heat|participantB:76ers|scheduledStart:20261125-2200", "sideSpecs": [{"name": "Heat"}, {"name": "76ers"}], "venue": "out"},
    ],
    "questions": [{"question": 9, "name": "template:sportsContestResult", "description": "competition:UCL|contestType:Match|participantA:Alpha|participantB:Beta|scheduledStart:20261125-2000|sport:Soccer", "fallbackOutcome": 102, "namedOutcomes": [100, 101], "settledNamedOutcomes": []}],
    "deployers": [],
}


def make_world(root: Path, books=(), trades=(), events=(), refs=(), meta_times=(0, 3600)):
    root = Path(root)
    tables = {
        "books": sorted(books, key=lambda r: (r["coin"], r["recv_ns"])),
        "trades": sorted(trades, key=lambda r: (r["coin"], r["recv_ns"])),
        "mids": [], "audits": [],
        "meta": [{"recv_ns": T0 + int(t * S), "json": json.dumps(META)} for t in meta_times],
        "refs": sorted(refs, key=lambda r: (r["coin"], r["recv_ns"])),
    }
    out = {}
    for name, rows in tables.items():
        p = _write_table(root, name, rows)
        out[name] = {"file": str(p.relative_to(root)), "rows": len(rows), "sha256": _sha(p)}
    (root / "tables" / f"manifest-{DAY}.json").write_text(json.dumps({"day": DAY, "format": "jsonl.gz", "sources": [], "tables": out}))
    ev = root / "meta" / "events" / f"{DAY}.jsonl"
    ev.parent.mkdir(parents=True, exist_ok=True)
    base = [{"ns": T0 + 1, "kind": "session_start", "session": "s"}, {"ns": T0 + 86399 * S, "kind": "session_end", "session": "s"}]
    ev.write_text("".join(json.dumps(e) + "\n" for e in sorted(base + list(events), key=lambda e: e["ns"])))
    return root


# ---------------------------------------------------------------- MDE

def test_mde_reproduces_o9_tables():
    assert round(M.mde(50, M.clusters(25, 30)), 1) == 12.2
    assert round(M.mde(50, M.clusters(50, 90)), 1) == 5.0
    assert round(M.mde(50, M.clusters(100, 90)), 1) == 3.5
    assert round(M.mde(50, M.clusters(200, 60)), 1) == 3.0
    # O9 rounded (z + z)^2 to 6.18; the exact value gives counts within 0.1%
    for delta, o9 in ((2, 3978), (3, 1768), (5, 637), (10, 159)):
        assert abs(M.n_required(50, delta) - o9) <= max(1, 0.001 * o9) + 1
    assert round(M.mde_sharpe(84), 3) == 0.275 and round(M.mde_sharpe(30), 2) == 0.46
    assert round(M.mde(50, 1000, k=3) / M.mde(50, 1000), 2) == 1.19
    assert M.design_effect(20, 0.3) == pytest.approx(6.7)


def test_registration_check_rejects_edges_below_mde():
    ok = M.registration_check(plausible_edge=6.0, sigma=50, per_week=100, days=90)
    bad = M.registration_check(plausible_edge=2.0, sigma=50, per_week=100, days=90)
    assert ok["testable"] and not bad["testable"] and "below the MDE" in bad["reason"]


# ---------------------------------------------------------------- universe and snapshot

def test_cluster_keys_follow_o9_units_of_independence():
    from keel.hl.metadata import parse_outcome_meta
    from keel.research.universe import cluster_keys, segment

    m = parse_outcome_meta(META)
    q = m.questions[9]
    keys = {oid: cluster_keys(o, m.questions.get(o.question)) for oid, o in m.outcomes.items()}
    assert keys[100] == keys[101] == keys[102] == ("q9", "q9")  # one contest, all outcomes
    assert keys[200][0] == keys[201][0] != keys[202][0]  # strict: underlying x expiry
    assert keys[200][1] == keys[202][1]  # directional: one expiry
    assert segment(m.outcomes[102], q) == "sports_match" and segment(m.outcomes[200], None) == "price"
    assert segment(m.outcomes[300], None) == "sports_match" and keys[300][0].startswith("match|NBA|")


def test_snapshot_id_is_stable_and_detects_changes(tmp_path):
    from keel.research import snapshot as SN

    make_world(tmp_path, books=[_book("#1000", 10, [(0.4, 100)], [(0.42, 100)])])
    a, b = SN.build(tmp_path), SN.build(tmp_path)
    assert a.id == b.id and len(a.id) == 64
    SN.verify(tmp_path, {"id": a.id, **a.describe()})
    with open(tmp_path / "meta" / "events" / f"{DAY}.jsonl", "a") as fh:
        fh.write(json.dumps({"ns": T0 + 5 * S, "kind": "outcome_removed", "coin": "#1000"}) + "\n")
    with pytest.raises(ValueError):
        SN.verify(tmp_path, {"id": a.id, **a.describe()})
    (tmp_path / "tables" / "books" / f"date={DAY}" / "part-0.jsonl.gz").write_bytes(b"tampered")
    with pytest.raises(ValueError):
        SN.build(tmp_path)  # manifest hash check


def test_icc_estimator():
    from keel.research.census import icc_anova

    assert icc_anova({"a": [1, 1, 1], "b": [5, 5, 5], "c": [9, 9, 9]}) == pytest.approx(1.0)
    assert icc_anova({"a": [1, 5, 9], "b": [1, 5, 9], "c": [9, 5, 1]}) == 0.0
    assert icc_anova({"a": [1.0]}) is None


# ---------------------------------------------------------------- census

def _census_world(tmp_path):
    books, trades = [], []
    for i in range(0, 600, 5):  # 10 minutes of books on the question's three outcomes and one price market
        drift = 0.001 * (i // 60)
        books.append(_book("#1000", i, [(0.40 + drift, 500)], [(0.401 + drift, 500)]))
        books.append(_book("#1010", i, [(0.30, 500)], [(0.301, 500)]))
        books.append(_book("#2000", i, [(0.60, 50), (0.55, 100)], [(0.62, 50)]))
    tid = 0
    for i in range(10, 540, 7):
        tid += 1
        trades.append(_trade("#1000", i, 0.401, 20, "B", tid, buyer=f"0xt{tid % 3}", seller="0xmaker"))
        tid += 1
        trades.append(_trade("#2000", i, 0.60, 30, "A", tid, buyer="0xmaker2", seller=f"0xt{tid % 4}"))
    refs = []
    for i in range(0, 200):
        # Polymarket mid 0.43 while Outcome ask is 0.401: a 2.9c buy gap from t=100 to t=160
        rb, ra = (0.429, 0.431) if 100 <= i < 160 else (0.40, 0.402)
        refs.append({"recv_ns": T0 + i * S, "coin": "#1000", "exch_ms": (T0 + i * S) // 10**6, "bid_px7": 4000000, "ask_px7": 4010000, "bid_sz6": 10**9, "ask_sz6": 10**9,
                     "ref_bid_px7": int(rb * 1e7), "ref_ask_px7": int(ra * 1e7), "ref_exch_ms": None, "ref_feed_age_s": 1.0, "ref_change_age_s": 1.0,
                     "confidence": 0.95, "level": "high", "usable": i != 130, "why": None if i != 130 else "feed stale", "tier": "fast"})
    events = [{"ns": T0 + 590 * S, "kind": "outcome_removed", "coin": "#2000"}, {"ns": T0 + 595 * S, "kind": "outcome_settled", "coin": "#2000", "settle_fraction": 1.0}]
    return make_world(tmp_path, books, trades, events, refs, meta_times=(0, 300, 580))


def test_census_is_deterministic_and_verifies(tmp_path):
    from keel.research import census as C
    from keel.research import snapshot as SN

    root = _census_world(tmp_path)
    snap = SN.build(root)
    r1, e1 = C.build(snap)
    r2, e2 = C.build(SN.build(root))
    assert C.canonical(r1) == C.canonical(r2) and e1 == e2
    u = r1["universe"]
    assert u["sports_match"]["clusters_seen"] == 2  # the soccer question and the NBA game
    assert u["price"]["settled_official"] == 1 and u["price"]["clusters_resolved"] == 1
    assert r1["status"] == "complete" or r1["status"] in ("indicative", "insufficient span")
    mk = r1["markouts_c"]["sports_match"]["1m"]
    assert mk["trades"] > 0 and mk["mean_c"] > 0  # buyer at 0.401 marked against a rising mid
    st = r1["markouts_c"]["price"]["settlement"]
    assert st["mean_c"] == pytest.approx(-40.0)  # aggressive sellers at 0.60 of a market that settled at 1
    ep = r1["episodes"]
    assert ep["rows_excluded"] == {"feed stale": 1}
    two = ep["by_threshold"]["2c"]
    assert two["episodes"] == 2  # the stale row splits the gap into two episodes
    assert two["closed_by"].get("reference unusable") == 1 and two["closed_by"].get("reference moved") == 1
    jp, mp, epp = C.write(r1, e1, tmp_path / "out")
    jp2, _, epp2 = C.write(r2, e2, tmp_path / "out2")
    assert jp.read_bytes() == jp2.read_bytes() and epp.read_bytes() == epp2.read_bytes()
    assert "Reference-gap episodes" in mp.read_text()


def test_census_refuses_rates_from_short_coverage(tmp_path):
    from keel.research import census as C
    from keel.research import snapshot as SN

    root = _census_world(tmp_path)
    ev = root / "meta" / "events" / f"{DAY}.jsonl"
    lines = [json.loads(l) for l in ev.read_text().splitlines()]
    for e in lines:
        if e["kind"] == "session_end":
            e["ns"] = T0 + 3600 * S  # recorder ran one hour only
    ev.write_text("".join(json.dumps(e) + "\n" for e in lines))
    r, _ = C.build(SN.build(root))
    assert r["status"] == "insufficient span"
    assert all(v["clusters_resolved_per_week"] is None for v in r["universe"].values())
    assert all(x["hold_to_resolution_mde_c_sigma50"] is None for x in r["mde"]["table"])


# ---------------------------------------------------------------- maker census

def test_maker_census_accounts_settlement_and_kill_rule(tmp_path):
    from keel.research import makers as K
    from keel.research import snapshot as SN

    r = K.build(SN.build(_census_world(tmp_path)), min_maker_fills=10)
    # 0xmaker2 bought #2000 passively at 0.60 and the market settled at 1.0: +0.40 a share
    top = {m["address"]: m for m in r["top_makers"]}
    m2 = top["0xmaker2"]
    assert m2["of_which_settled"] == pytest.approx(0.40 * 30 * m2["maker_fills"], rel=1e-6)
    assert r["segments"]["price"]["verdict"].startswith("insufficient")  # one cluster: no verdict
    assert K.cluster_upper_bound({"a": -5.0, "b": -6.0, "c": -4.0})[1] < 0


# ---------------------------------------------------------------- simulator

class _Script:
    """Submit given intents at given seconds (local decision time)."""

    def __init__(self, plan):
        self.plan = plan

    def start(self, sim):
        for t, kind, x in self.plan:
            sim.schedule(T0 + int(t * S), lambda s, kind=kind, x=x: s.submit(x) if kind == "submit" else s.cancel(x))

    def on_book(self, sim, coin, book):
        pass

    def on_trade(self, sim, coin, trade):
        pass

    def finish(self, sim):
        pass


def _sim(tmp_path, books, trades, plan, latency_ms=100.0, seed=0, **kw):
    from keel.research import snapshot as SN
    from keel.research.sim import LatencyModel, Sim

    root = make_world(tmp_path, books, trades, kw.pop("events", ()))
    sim = Sim(SN.build(root), ["#1000"], latency=LatencyModel(samples_ms=[latency_ms]), seed=seed, **kw)
    return sim.run(_Script(plan))


def test_ioc_uses_the_arrival_time_book_and_consumes_liquidity(tmp_path):
    from keel.research.sim import SimIntent

    books = [_book("#1000", 1, [(0.40, 100)], [(0.41, 30), (0.42, 100)]), _book("#1000", 2, [(0.45, 100)], [(0.46, 100)])]
    plan = [(1.5, "submit", SimIntent("#1000", True, 4200000, 50 * 10**6, "ioc", "a")),  # arrives 1.6 s: book at t=1
            (1.6, "submit", SimIntent("#1000", True, 4200000, 100 * 10**6, "ioc", "b")),  # arrives 1.7 s: what is left
            (2.5, "submit", SimIntent("#1000", True, 4200000, 50 * 10**6, "ioc", "c"))]  # arrives after the t=2 book: 0.46 ask, no fill
    res = _sim(tmp_path, books, [], plan)
    a = [(f.px7, f.sz6) for f in res.fills if f.cloid == "a"]
    b = [(f.px7, f.sz6) for f in res.fills if f.cloid == "b"]
    assert a == [(4100000, 30 * 10**6), (4200000, 20 * 10**6)]
    assert b == [(4200000, 80 * 10**6)]  # 100 at 0.42 minus the 20 order a took
    assert not [f for f in res.fills if f.cloid == "c"]


def test_validation_rejects(tmp_path):
    from keel.research.sim import Ledger, SimIntent

    books = [_book("#1000", 1, [(0.40, 100)], [(0.41, 100)])]
    plan = [(1.5, "submit", SimIntent("#1000", True, 4100050, 50 * 10**6, "ioc", "off")),
            (1.5, "submit", SimIntent("#1000", True, 4100000, 10 * 10**6, "ioc", "small")),
            (1.5, "submit", SimIntent("#1000", True, 4100000, 50 * 10**6, "alo", "cross"))]
    res = _sim(tmp_path, books, [], plan)
    assert res.rejects == {"below minimum notional": 1, "off tick or out of range": 1, "post-only would cross": 1}
    res = _sim(tmp_path, books, [], plan[:1] * 3, ledger=Ledger(initial=2))
    assert res.rejects["request ledger exhausted"] == 1


def test_resting_order_queue_and_cancel_priority(tmp_path):
    from keel.research.sim import SimIntent

    books = [_book("#1000", 1, [(0.40, 100)], [(0.41, 100)])]
    trades = [_trade("#1000", 3, 0.40, 60, "A", 1), _trade("#1000", 4, 0.40, 70, "A", 2), _trade("#1000", 6, 0.39, 500, "A", 3)]
    # joins behind 100 displayed at 0.40: the first 60 + 40 of the second trade clear the
    # queue, 30 fill us; the trade through 0.39 fills the rest
    res = _sim(tmp_path, books, trades, [(1.5, "submit", SimIntent("#1000", True, 4000000, 50 * 10**6, "gtc", "r"))])
    assert [(f.sz6, f.liquidity) for f in res.fills] == [(30 * 10**6, "maker"), (20 * 10**6, "maker")]
    # a cancel arriving in the same block as a trade is processed first
    res = _sim(tmp_path, books, [_trade("#1000", 3, 0.39, 500, "A", 9)], [(1.5, "submit", SimIntent("#1000", True, 4000000, 50 * 10**6, "gtc", "r")), (2.9, "cancel", "r")])
    assert not res.fills and any('"cancelled"' in l for l in res.log)


def test_fees_only_on_reducing_fills_and_settlement(tmp_path):
    from keel.research.sim import FeeModel, SimIntent

    books = [_book("#1000", 1, [(0.40, 1000)], [(0.41, 1000)]), _book("#1000", 3, [(0.50, 1000)], [(0.51, 1000)])]
    plan = [(1.5, "submit", SimIntent("#1000", True, 4100000, 100 * 10**6, "ioc", "buy")), (3.5, "submit", SimIntent("#1000", False, 5000000, 60 * 10**6, "ioc", "sell"))]
    ev = [{"ns": T0 + 10 * S, "kind": "outcome_removed", "coin": "#1000"}, {"ns": T0 + 11 * S, "kind": "outcome_settled", "coin": "#1000", "settle_fraction": 1.0}]
    res = _sim(tmp_path, books, [], plan, fees=FeeModel(taker_bps=10, settlement_bps=0), events=ev)
    buy, sell = res.fills
    assert buy.fee_usdc == 0 and sell.fee_usdc == pytest.approx(60 * 0.50 * 0.001)
    # bought 100 @ 0.41, sold 60 @ 0.50, 40 settled at 1.0
    assert res.pnl_usdc == pytest.approx(-41 + 30 + 40 - 0.03, abs=1e-6) and not res.positions


def test_side1_intent_maps_to_merged_book():
    from keel.research.sim import SimIntent

    it = SimIntent.side1("#1000", buy_side1=True, px7_side1=6000000, sz6=10)
    assert (it.is_buy, it.px7) == (False, 4000000)


def test_replay_is_byte_identical(tmp_path):
    from keel.research.sim import LatencyModel, Sim, SimIntent
    from keel.research import snapshot as SN

    books = [_book("#1000", t, [(0.40, 100)], [(0.41, 100)]) for t in range(1, 30)]
    root = make_world(tmp_path, books, [])
    plan = [(t + 0.5, "submit", SimIntent("#1000", t % 2 == 0, 4100000 if t % 2 == 0 else 4000000, 30 * 10**6, "ioc", f"x{t}")) for t in range(1, 25)]

    def go(seed):
        return Sim(SN.build(root), ["#1000"], latency=LatencyModel(200, 800), seed=seed).run(_Script(plan))

    a, b, c = go(5), go(5), go(6)
    assert a.log == b.log and a.log_sha256 == b.log_sha256
    assert a.log_sha256 != c.log_sha256


# ---------------------------------------------------------------- registry

SPEC = {
    "name": "O-S7-L", "mechanism": "Outcome sports books lag Polymarket after news; takers who are fast enough are paid by stale resting quotes",
    "universe_rule": "Outcome sports_match instances with a high/medium Polymarket mapping at decision time", "cluster_key": "question or match",
    "configs": [{"threshold_c": 0.5, "horizon_s": 60}], "primary_statistic": "net edge per share at the arrival-time book after fees",
    "alpha": 0.05, "power": 0.8, "guardrails": {"max_race_loss": 0.5}, "live_loss_budget_usdc": 150, "calendar_cap_days": 90,
    "plausible_edge": 0.8, "sigma": 1.0, "clusters_per_week": 100, "participation": 0.5, "budget_days": 45,
}


def test_registry_rules(tmp_path):
    from keel.research.registry import Registry, RegistrationError
    from keel.research import snapshot as SN

    reg = Registry(tmp_path / "r.sqlite")
    with pytest.raises(RegistrationError, match="missing"):
        reg.register({k: v for k, v in SPEC.items() if k != "mechanism"})
    with pytest.raises(RegistrationError, match="untestable"):
        reg.register({**SPEC, "name": "tiny", "plausible_edge": 0.05})
    assert reg.get("tiny")["stage"] == "REJECTED"  # kept in the graveyard
    h = reg.register(SPEC)
    assert h["version"] == 1 and h["stage"] == "REGISTERED"
    with pytest.raises(sqlite3.DatabaseError):
        reg.db.execute("UPDATE hypotheses SET spec_json='{}' WHERE id=?", (h["id"],))
    with pytest.raises(sqlite3.DatabaseError):
        reg.db.execute("DELETE FROM hypotheses")

    root = _census_world(tmp_path / "w")
    snap = SN.build(root)

    def boom(s, c, seed):
        raise RuntimeError("evaluator failed")

    def fine(s, c, seed):
        return {"x": 1}

    boom.evidence_kind = fine.evidence_kind = "simulation"
    with pytest.raises(RuntimeError):
        reg.run("O-S7-L", "boom", snap, evaluators={"boom": boom})
    run = reg.run("O-S7-L", "fine", snap, seed=3, evaluators={"fine": fine})
    assert reg.get("O-S7-L")["trials"] == 2  # the failed run still counts
    assert run["snapshot_id"] == snap.id and run["seed"] == 3 and run["env_sha256"]
    with pytest.raises(RegistrationError, match="can only kill"):
        reg.verdict(run["id"], "GO")
    reg.verdict(run["id"], "NO-GO", "sim screen")
    assert reg.get("O-S7-L")["stage"] == "REJECTED"
    with pytest.raises(RegistrationError):
        reg.run("O-S7-L", "fine", snap, evaluators={"fine": fine})
    assert reg.register({**SPEC, "configs": [{"threshold_c": 1.0, "horizon_s": 60}]})["version"] == 2  # a redesign is a new version


def test_reference_gap_screen_runs_end_to_end(tmp_path):
    from keel.research import evaluators as E
    from keel.research import snapshot as SN

    snap = SN.build(_census_world(tmp_path))
    a = E.reference_gap_taker(snap, {"threshold_c": 2.0, "horizon_s": 30, "size_usdc": 20, "latency_p50_ms": 100, "latency_p95_ms": 200}, 1)
    b = E.reference_gap_taker(snap, {"threshold_c": 2.0, "horizon_s": 30, "size_usdc": 20, "latency_p50_ms": 100, "latency_p95_ms": 200}, 1)
    assert a == b and a["episodes"] == 2 and a["attempted"] >= 1
    assert a["net_edge_c_per_share"]["n"] >= 1


def test_cli_research(tmp_path, monkeypatch, capsys):
    from keel.cli import main

    root = _census_world(tmp_path / "data")
    monkeypatch.chdir(tmp_path)
    assert main(["--data", str(root), "research", "census"]) == 0
    rep = next((root / "reports" / "census").glob("census-*.json"))
    assert main(["--data", str(root), "research", "census", "--verify", str(rep)]) == 0
    assert main(["--data", str(root), "research", "makers", "--min-fills", "5"]) == 0
    assert main(["research", "mde"]) == 0
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps(SPEC))
    assert main(["--data", str(root), "research", "register", str(spec)]) == 0
    spec.write_text(json.dumps({**SPEC, "name": "small", "plausible_edge": 0.01}))
    assert main(["--data", str(root), "research", "register", str(spec)]) == 2
    assert main(["--data", str(root), "research", "run", "O-S7-L", "--evaluator", "reference_gap_taker"]) == 0
    assert main(["--data", str(root), "research", "verdict", "1", "GO"]) == 2
    assert main(["--data", str(root), "research", "list"]) == 0
    out = capsys.readouterr().out
    assert "Reproduced byte for byte" in out and "Refused" in out and "2 hypotheses, 1 trials" in out


def test_validation_recovers_planted_edge_and_rejects_null():
    from keel.research import validate as V

    r = V.planted_edge(minutes=16)
    assert r["pass"], r
    assert abs(r["edge"]["net_edge_c"] - r["planted"]["expected_net_edge_c"]) < 0.1 and r["null"]["upper95"] < 0


def test_validation_on_census_world(tmp_path, monkeypatch, capsys):
    from keel.cli import main
    from keel.research import snapshot as SN
    from keel.research import validate as V

    root = _census_world(tmp_path / "data")
    snap = SN.build(root)
    c = V.conservation(snap)
    assert c["pass"] and c["residual_usdc"] == 0
    # a market whose prints all happened at the recorded touch agrees fully; one whose
    # prints are away from the book (the census world's drifting #1000) does not
    w = make_world(tmp_path / "w", [_book("#1000", t, [(0.40, 500)], [(0.41, 500)]) for t in range(0, 60, 1)],
                   [_trade("#1000", t + 0.5, 0.41, 10, "B", t) for t in range(1, 50, 3)] + [_trade("#1000", t + 0.5, 0.40, 10, "A", 100 + t) for t in range(2, 50, 3)])
    s = V.sim_vs_exchange(SN.build(w))
    assert s["orders"] == 33 and s["by_book_age"]["<= 1 s"]["agree_pct"] == 100.0 and s["pass"]
    s2 = V.sim_vs_exchange(snap)
    assert s2["orders"] > 0 and min(x["agree_pct"] for x in s2["by_book_age"].values()) < 100
    monkeypatch.setattr(V, "planted_edge", lambda minutes=60: {"pass": True})
    assert main(["--data", str(root), "research", "validate"]) == 2  # the drifting world fails check 1
    assert main(["--data", str(w), "research", "validate"]) == 0
    out = capsys.readouterr().out
    assert "FAIL  sim vs exchange" in out and "PASS  sim vs exchange" in out
