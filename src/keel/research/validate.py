"""Functional validation of the research engine against ground truth (`keel research
validate`). Unit tests show the code is consistent; these checks show its outputs are
right, using facts it cannot manufacture:

1. Simulator vs the exchange. Every real aggressive order in the snapshot (prints that
   share a transaction hash) is replayed through the simulator as an IOC at its block,
   against the book recorded just before that block, with zero latency. The simulator
   must fill the same size at the same average price the exchange printed. Agreement is
   reported by the age of the book the order met, since a stale book cannot agree.
2. Accounting conservation. Every trade has a buyer and a seller valued at the same
   mark, so across all addresses the maker census's P&L must equal minus its fees.
3. Planted edge. A synthetic market whose Outcome book follows a reference with a known
   lag and jump size must be found by the episode scanner and earn about the planted edge
   less spread and fees in the simulator screen; a market with no edge must not.
4. Mutations. Changing fees, latency or thresholds must move the screen's results in the
   expected direction (a check that the screen is sensitive to what it claims to model).
5. Independent recomputation. One-minute markouts are recomputed straight from the raw
   WebSocket messages with separate code and compared with the census.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import tempfile
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from keel import __version__
from keel.compact import iter_table, table_paths
from keel.recorder.store import iter_raw
from keel.research import census as C
from keel.research import evaluators as E
from keel.research import makers as K
from keel.research import snapshot as SN
from keel.research.sim import PX, SZ, LatencyModel, Ledger, Sim, SimIntent, Strategy
from keel.research.universe import build_index

MS = 10**6
TICK = 100


# ---------------------------------------------------------------- 1. simulator vs exchange

class _Replay(Strategy):
    def __init__(self, orders: List[Dict[str, Any]]):
        self.orders = orders

    def start(self, sim: Sim) -> None:
        for i, o in enumerate(self.orders):
            sim.schedule(o["block_ns"] - 1, lambda s, o=o, i=i: s.submit(SimIntent(o["coin"], o["is_buy"], o["limit_px7"], o["sz6"], "ioc", f"r{i}")))


def sim_vs_exchange(snap, max_orders: int = 20000) -> Dict[str, Any]:
    idx = build_index(snap)
    cov = C.Coverage(C.coverage_intervals(snap))
    groups: Dict[Tuple, Dict[str, Any]] = {}
    seen = set()
    outside = 0
    for path in table_paths(snap.root, "trades", snap.days):
        for t in iter_table(path, ["exch_ms", "coin", "tid", "px7", "sz6", "aggressor", "hash", "buyer", "seller"]):
            if (t["coin"], t["tid"]) in seen or not t.get("exch_ms"):
                continue
            seen.add((t["coin"], t["tid"]))
            ns = int(t["exch_ms"]) * MS
            if not (snap.start_ns <= ns < snap.end_ns):
                continue
            inst = idx.at(t["coin"], ns)
            if inst is None or inst.venue != "out":
                continue
            if not cov.same(ns, ns):
                outside += 1  # replayed on subscribe from before the recorder was running
                continue
            taker = t["buyer"] if t["aggressor"] == "B" else t["seller"]
            key = (t["coin"], t.get("hash") or "", int(t["exch_ms"]), t["aggressor"], taker)
            g = groups.setdefault(key, {"coin": t["coin"], "block_ns": ns, "is_buy": t["aggressor"] == "B", "prints": []})
            g["prints"].append((int(t["px7"]), int(t["sz6"])))
    orders = []
    for g in sorted(groups.values(), key=lambda g: (g["block_ns"], g["coin"])):
        pxs = [p for p, _ in g["prints"]]
        g["sz6"] = sum(s for _, s in g["prints"])
        g["limit_px7"] = max(pxs) if g["is_buy"] else min(pxs)
        g["vwap"] = sum(p * s for p, s in g["prints"]) / g["sz6"]
        orders.append(g)
    orders = orders[:max_orders]
    windows: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
    for o in orders:
        windows[o["coin"]].append((o["block_ns"] - 30 * 10**9, o["block_ns"] + 10**9))
    sim = Sim(snap, windows.keys(), latency=LatencyModel(samples_ms=[0.0]), ledger=Ledger(enabled=False), min_notional_usdc=0.0, windows=windows, index=idx)
    # book age at each order: the recorded book in force just before its block
    book_t: Dict[str, List[int]] = defaultdict(list)
    for path in table_paths(snap.root, "books", snap.days):
        for r in iter_table(path, ["exch_ms", "coin"]):
            if r["coin"] in windows:
                book_t[r["coin"]].append(int(r["exch_ms"]) * MS)
    for v in book_t.values():
        v.sort()
    res = sim.run(_Replay(orders))
    fills: Dict[str, List] = defaultdict(list)
    for f in res.fills:
        fills[f.cloid].append(f)
    buckets = {"<= 1 s": [0, 0, []], "1-5 s": [0, 0, []], "5-15 s": [0, 0, []], "> 15 s": [0, 0, []], "no book recorded yet": [0, 0, []]}
    for i, o in enumerate(orders):
        bt = book_t.get(o["coin"], [])
        j = bisect_right(bt, o["block_ns"] - 1) - 1
        age = (o["block_ns"] - bt[j]) / 1e9 if j >= 0 else None
        if age is None:
            b = "no book recorded yet"
        else:
            b = "<= 1 s" if age <= 1 else "1-5 s" if age <= 5 else "5-15 s" if age <= 15 else "> 15 s"
        got = fills.get(f"r{i}", [])
        q = sum(f.sz6 for f in got)
        vwap = sum(f.px7 * f.sz6 for f in got) / q if q else None
        size_ok = abs(q - o["sz6"]) <= max(1, 0.01 * o["sz6"])
        px_ok = vwap is not None and abs(vwap - o["vwap"]) <= TICK
        buckets[b][0] += 1
        buckets[b][1] += int(size_ok and px_ok)
        if vwap is not None:
            buckets[b][2].append(abs(vwap - o["vwap"]) / 1e5)
    out = {}
    for b, (n, ok, diffs) in buckets.items():
        if n:
            out[b] = {"orders": n, "agree": ok, "agree_pct": round(100 * ok / n, 1), "median_abs_vwap_diff_c": round(sorted(diffs)[len(diffs) // 2], 4) if diffs else None}
    fresh = out.get("<= 1 s", {})
    allin = sum(x["orders"] for x in out.values())
    agree_all = sum(x["agree"] for x in out.values())
    return {"orders": len(orders), "trades_outside_recording_excluded": outside, "agree_pct_all_recorded": round(100 * agree_all / allin, 1) if allin else None, "by_book_age": out, "pass": bool(fresh) and fresh["agree_pct"] >= 90.0, "criterion": "at least 90% of orders that met a book under 1 s old fill the printed size at the printed average price (within one tick)"}


# ---------------------------------------------------------------- 2. conservation

def conservation(snap) -> Dict[str, Any]:
    r = K.build(snap, min_maker_fills=0)
    c = r["conservation"]
    tol = max(0.01, 1e-6 * sum(m["maker_volume_usdc"] for m in r["top_makers"]))
    return {**c, "trades": r["trades"], "addresses": r["addresses"], "pass": abs(c["residual_usdc"]) <= tol, "criterion": f"sum of P&L + sum of fees = 0 within {tol:.4g} USDC"}


# ---------------------------------------------------------------- 3. planted edge

N_PLANTED = 20


def _meta(n: int) -> Dict[str, Any]:
    outs, qs = [], []
    for k in range(n):
        oid = 100 + k
        outs.append({"outcome": oid, "name": "template:sportsContestParticipant2", "description": f"participant:Team{k}", "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "venue": "out"})
        qs.append({"question": 9000 + k, "name": "template:sportsContestResult", "description": f"competition:Test|contestType:Match|participantA:Team{k}|participantB:Other{k}|scheduledStart:20991231-2000|sport:Soccer", "fallbackOutcome": None, "namedOutcomes": [oid], "settledNamedOutcomes": []})
    return {"outcomes": outs, "questions": qs, "deployers": []}


def _planted_world(root: Path, lag_s: float, jump_c: float, follows: bool, minutes: int = 60, spread_c: float = 0.1, n_markets: int = N_PLANTED) -> None:
    """`n_markets` independent contests. Each market's fair value jumps by `jump_c` and
    back every two minutes (phase-shifted per market); the reference shows it at once.
    With `follows`, Outcome's book reaches the new value `lag_s` later; without, Outcome
    never moves, so the reference moves are noise for it."""
    day = "2026-01-02"
    t0 = 1767312000 * 10**9
    step = 0.5
    books, refs = [], []
    n = int(minutes * 60 / step)
    for k in range(n_markets):
        coin = f"#{10 * (100 + k)}"
        phase = 7.0 * k

        def true_mid(t: float) -> float:
            return 0.50 + (jump_c / 100) * (int(max(0.0, t + phase) // 120) % 2)

        for i in range(n):
            t = i * step
            ref = true_mid(t)
            om = true_mid(max(0.0, t - lag_s)) if follows else 0.50
            ob, oa = round(om - spread_c / 200, 5), round(om + spread_c / 200, 5)
            ns = t0 + int(t * 1e9) + k * 1000
            lv = json.dumps([[{"px": f"{ob:.5f}", "sz": "100000", "n": 1}], [{"px": f"{oa:.5f}", "sz": "100000", "n": 1}]])
            books.append({"recv_ns": ns + 5 * MS, "exch_ms": ns // MS, "coin": coin, "bid_px7": int(round(ob * PX)), "ask_px7": int(round(oa * PX)), "bid_sz6": 10**11, "ask_sz6": 10**11, "n_bid_levels": 1, "n_ask_levels": 1, "levels_json": lv})
            refs.append({"recv_ns": ns + 10 * MS, "coin": coin, "exch_ms": ns // MS, "bid_px7": int(round(ob * PX)), "ask_px7": int(round(oa * PX)), "bid_sz6": 10**11, "ask_sz6": 10**11,
                         "ref_bid_px7": int(round((ref - 0.0005) * PX)), "ref_ask_px7": int(round((ref + 0.0005) * PX)), "ref_exch_ms": None, "ref_feed_age_s": 0.5, "ref_change_age_s": 0.5,
                         "confidence": 0.99, "level": "high", "usable": True, "why": None, "tier": "fast"})
    books.sort(key=lambda r: (r["coin"], r["recv_ns"]))
    refs.sort(key=lambda r: (r["coin"], r["recv_ns"]))
    tables = {"books": books, "trades": [], "mids": [], "audits": [], "refs": refs, "meta": [{"recv_ns": t0 + 1, "json": json.dumps(_meta(n_markets))}]}
    man = {}
    for name, rows in tables.items():
        p = root / "tables" / name / f"date={day}" / "part-0.jsonl.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.GzipFile(p, "wb", mtime=0) as fh:
            for r in rows:
                fh.write((json.dumps(r) + "\n").encode())
        man[name] = {"file": str(p.relative_to(root)), "rows": len(rows), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
    (root / "tables" / f"manifest-{day}.json").write_text(json.dumps({"day": day, "format": "jsonl.gz", "sources": [], "tables": man}))
    ev = root / "meta" / "events" / f"{day}.jsonl"
    ev.parent.mkdir(parents=True, exist_ok=True)
    ev.write_text(json.dumps({"ns": t0, "kind": "session_start"}) + "\n" + json.dumps({"ns": t0 + minutes * 60 * 10**9 + 10**9, "kind": "session_end"}) + "\n")


def planted_edge(minutes: int = 60) -> Dict[str, Any]:
    jump, lag, spread, fee_bps = 2.0, 10.0, 0.1, 8.75
    cfg = {"threshold_c": 0.5, "horizon_s": 60, "size_usdc": 50, "latency_p50_ms": 300, "latency_p95_ms": 600, "taker_fee_bps": fee_bps}
    expected = jump - spread - (fee_bps / 1e4) * 51  # exit fee on about 0.51 a share, in cents
    out = {}
    with tempfile.TemporaryDirectory() as d:
        for name, follows in (("edge", True), ("null", False)):
            root = Path(d) / name
            _planted_world(root, lag, jump, follows, minutes=minutes)
            r = E.reference_gap_taker(SN.build(root), cfg, 1)
            out[name] = {"episodes": r["episodes"], "filled": r["filled"], "net_edge_c": r["net_edge_c_per_share"]["mean"], "lower95": r["net_edge_c_per_share"]["lower95"], "upper95": r["net_edge_c_per_share"]["upper95"]}
    e, n = out["edge"], out["null"]
    ok = (e["net_edge_c"] is not None and e["lower95"] is not None and abs(e["net_edge_c"] - expected) <= 0.1 and e["lower95"] > 0) and (n["net_edge_c"] is not None and n["upper95"] is not None and n["upper95"] < 0)
    return {"planted": {"jump_c": jump, "lag_s": lag, "spread_c": spread, "taker_fee_bps": fee_bps, "expected_net_edge_c": round(expected, 4)}, **out, "pass": ok,
            "criterion": "edge world: mean net edge within 0.1c of jump - spread - fee and lower 95% bound > 0; null world: upper 95% bound < 0"}


# ---------------------------------------------------------------- 4. mutations

def mutations(snap, threshold_c: float = 0.5) -> Dict[str, Any]:
    base_cfg = {"threshold_c": threshold_c, "horizon_s": 60, "size_usdc": 25}
    base = E.reference_gap_taker(snap, base_cfg, 1)
    if not base["filled"]:
        return {"pass": None, "note": "no filled episodes in this snapshot: mutations not testable"}
    nofee = E.reference_gap_taker(snap, {**base_cfg, "taker_fee_bps": 0}, 1)
    slow = E.reference_gap_taker(snap, {**base_cfg, "latency_p50_ms": 20000, "latency_p95_ms": 40000}, 1)
    none = E.reference_gap_taker(snap, {**base_cfg, "threshold_c": 50}, 1)
    b, f = base["net_edge_c_per_share"]["mean"], nofee["net_edge_c_per_share"]["mean"]
    checks = {
        "removing fees raises the edge": f is not None and b is not None and f > b,
        "20 s latency changes the replay": slow["sim_log_sha256"] != base["sim_log_sha256"],
        "a 50c threshold finds no episodes": none["episodes"] == 0,
    }
    return {"base_edge_c": b, "no_fee_edge_c": f, "fee_effect_c": round(f - b, 4) if f is not None and b is not None else None, "slow_edge_c": slow["net_edge_c_per_share"]["mean"], "checks": checks, "pass": all(checks.values())}


# ---------------------------------------------------------------- 5. independent markouts

def independent_markouts(snap, horizon_s: int = 60) -> Dict[str, Any]:
    idx = build_index(snap)
    days = snap.days
    mids: Dict[str, List[Tuple[int, Optional[float]]]] = defaultdict(list)
    trades: Dict[Tuple[str, Any], Tuple[int, int, str]] = {}
    for day in days:
        for rec in iter_raw(snap.root, src="hl_ws", day=day):
            msg = rec.get("msg")
            if not isinstance(msg, dict):
                continue
            ch, data = msg.get("channel"), msg.get("data")
            if ch == "l2Book" and isinstance(data, dict):
                lv = data.get("levels") or [[], []]
                m = (float(lv[0][0]["px"]) + float(lv[1][0]["px"])) / 2 if lv[0] and lv[1] else None
                mids[data["coin"]].append((int(data["time"]) * MS, m))
            elif ch == "trades":
                for t in data or []:
                    trades.setdefault((t["coin"], t["tid"]), (int(t["time"]) * MS, round(float(t["px"]) * PX), t["side"]))
    for v in mids.values():
        v.sort(key=lambda x: x[0])
    times = {c: [t for t, _ in v] for c, v in mids.items()}
    cov = C.Coverage(C.coverage_intervals(snap))
    seg_vals: Dict[str, List[float]] = defaultdict(list)
    for (coin, _tid), (ns, px, side) in trades.items():
        if not (snap.start_ns <= ns < snap.end_ns):
            continue
        inst = idx.at(coin, ns)
        if inst is None or inst.venue != "out":
            continue
        t2 = ns + horizon_s * 10**9
        if (inst.removed_ns and t2 > inst.removed_ns) or not cov.same(ns, t2) or coin not in times:
            continue
        j = bisect_right(times[coin], t2) - 1
        if j < 0 or mids[coin][j][1] is None:
            continue
        seg_vals[inst.segment].append((1 if side == "B" else -1) * (mids[coin][j][1] * PX - px) / 1e5)
    report, _ = C.build(snap)
    rows, ok = {}, True
    for seg, xs in sorted(seg_vals.items()):
        cen = (report["markouts_c"].get(seg) or {}).get("1m") or {}
        mean = sum(xs) / len(xs)
        sd = math.sqrt(sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)) if len(xs) > 1 else None
        same_n = abs(len(xs) - cen.get("trades", 0)) <= max(2, 0.01 * len(xs))
        same_mean = cen.get("mean_c") is not None and abs(mean - cen["mean_c"]) <= max(0.005, 0.05 * abs(mean))
        rows[seg] = {"independent": {"trades": len(xs), "mean_c": round(mean, 4), "sigma_c": round(sd, 4) if sd else None}, "census": {"trades": cen.get("trades"), "mean_c": cen.get("mean_c"), "sigma_c": cen.get("sigma_c") or cen.get("sigma_c_low_n")}, "agree": same_n and same_mean}
        ok = ok and rows[seg]["agree"]
    if not trades:
        return {"horizon_s": horizon_s, "segments": {}, "pass": None, "note": "no raw WebSocket data for these days (raw retention may have removed it)", "criterion": "trade count within 1% and mean within 5% (or 0.005c) of the census, per segment"}
    return {"horizon_s": horizon_s, "segments": rows, "pass": ok and bool(rows), "criterion": "trade count within 1% and mean within 5% (or 0.005c) of the census, per segment"}


def run(snap) -> Dict[str, Any]:
    out = {"kind": "keel.research.validation", "keel_version": __version__, "snapshot": snap.id}
    out["1_sim_vs_exchange"] = sim_vs_exchange(snap)
    out["2_accounting_conservation"] = conservation(snap)
    out["3_planted_edge"] = planted_edge()
    out["4_mutations"] = mutations(snap)
    out["5_independent_markouts"] = independent_markouts(snap)
    out["pass"] = all(v.get("pass") is not False for k, v in out.items() if k[0].isdigit())
    return out


def render_markdown(r: Dict[str, Any]) -> str:
    def mark(v):
        return {True: "PASS", False: "FAIL", None: "NOT TESTABLE"}[v]

    L = [f"# Keel research validation — snapshot {r['snapshot'][:12]}", "", f"Overall: **{mark(r['pass'])}**. Keel {r['keel_version']}.", ""]
    a = r["1_sim_vs_exchange"]
    L += [f"## 1. Simulator vs the exchange: {mark(a['pass'])}", "", f"{a['trades_outside_recording_excluded']:,} trades that happened while the recorder was off (replayed on reconnect) are excluded. Over all {a['orders']:,} orders while recording, {a['agree_pct_all_recorded']}% agree.", "", f"{a['orders']:,} real aggressive orders replayed as zero-latency IOCs against the book recorded just before their block. Criterion: {a['criterion']}.", "",
          "| Age of the book the order met | Orders | Same size and price | Median VWAP difference (¢) |", "|---|---|---|---|"]
    for b, x in a["by_book_age"].items():
        L.append(f"| {b} | {x['orders']:,} | {x['agree_pct']}% | {x['median_abs_vwap_diff_c']} |")
    L += ["", "Agreement falls as the book the order met gets older: simulated fills are strongest evidence where the recorder had a fresh book (the fast stream).", ""]
    c = r["2_accounting_conservation"]
    L += [f"## 2. Accounting conservation: {mark(c['pass'])}", "", f"{c['trades']:,} trades between {c['addresses']:,} addresses: sum of P&L {c['sum_pnl_usdc']} USDC, sum of fees {c['sum_fees_usdc']} USDC, residual {c['residual_usdc']} USDC.", ""]
    p = r["3_planted_edge"]
    L += [f"## 3. Planted edge: {mark(p['pass'])}", ""]
    if "planted" in p:
        L += [f"Planted: {p['planted']['jump_c']}¢ jumps that Outcome follows {p['planted']['lag_s']} s late, {p['planted']['spread_c']}¢ spread, {p['planted']['taker_fee_bps']} bps fee; expected net edge {p['planted']['expected_net_edge_c']}¢.", "",
              f"- Edge world: {p['edge']['episodes']} episodes, net edge {p['edge']['net_edge_c']}¢ (95% {p['edge']['lower95']} to {p['edge']['upper95']}).",
              f"- No-edge world: {p['null']['episodes']} episodes, net edge {p['null']['net_edge_c']}¢ (upper 95% {p['null']['upper95']}).", ""]
    m = r["4_mutations"]
    L += [f"## 4. Mutations: {mark(m['pass'])}", ""]
    for k, v in (m.get("checks") or {}).items():
        L.append(f"- {k}: {'yes' if v else 'NO'}")
    if m.get("fee_effect_c") is not None:
        L.append(f"- Fee effect on the screen: {m['fee_effect_c']}¢ per share.")
    L.append("")
    i = r["5_independent_markouts"]
    L += [f"## 5. Independent recomputation of 1-minute markouts: {mark(i['pass'])}", "", i.get("note", ""), "", "| Segment | Trades (raw / census) | Mean ¢ (raw / census) | σ ¢ (raw / census) |", "|---|---|---|---|"]
    for seg, x in i["segments"].items():
        L.append(f"| {seg} | {x['independent']['trades']} / {x['census']['trades']} | {x['independent']['mean_c']} / {x['census']['mean_c']} | {x['independent']['sigma_c']} / {x['census']['sigma_c']} |")
    L.append("")
    return "\n".join(L)
