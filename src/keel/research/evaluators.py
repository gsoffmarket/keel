"""Evaluators that `keel research run` can execute against a snapshot. Each returns a
JSON-able metrics dict and declares its evidence kind; simulation and census evidence
can only kill a hypothesis (the registry refuses GO on them).
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable, Dict, List

from keel.research import census as C
from keel.research.sim import PX, SZ, TICK_PX7, FeeModel, LatencyModel, Ledger, Sim, SimIntent, Strategy
from keel.research.universe import build_index

Z95 = 1.6448536269514722


def _evidence(kind: str) -> Callable:
    def deco(fn: Callable) -> Callable:
        fn.evidence_kind = kind  # type: ignore[attr-defined]
        return fn
    return deco


def _cluster_stats(per_cluster: Dict[str, List[float]]) -> Dict[str, Any]:
    """Mean per observation with a cluster-robust standard error (CR0 on cluster sums)."""
    xs = [x for v in per_cluster.values() for x in v]
    n, g = len(xs), len(per_cluster)
    if n == 0:
        return {"n": 0, "clusters": 0, "mean": None, "se": None, "upper95": None, "lower95": None}
    mean = sum(xs) / n
    if g < 2:
        return {"n": n, "clusters": g, "mean": round(mean, 6), "se": None, "upper95": None, "lower95": None}
    resid = [sum(x - mean for x in v) for v in per_cluster.values()]
    se = math.sqrt(g / (g - 1) * sum(r * r for r in resid)) / n
    return {"n": n, "clusters": g, "mean": round(mean, 6), "se": round(se, 6), "upper95": round(mean + Z95 * se, 6), "lower95": round(mean - Z95 * se, 6)}


@_evidence("census")
def markout(snapshot, cfg: Dict[str, Any], seed: int) -> Dict[str, Any]:
    """Aggressor markout at a horizon for one segment (cents per share)."""
    seg, h = cfg["segment"], cfg.get("horizon", "1m")
    idx = build_index(snapshot)
    cov = C.Coverage(C.coverage_intervals(snapshot))
    mids, _ = C.scan_books(snapshot, idx, cfg.get("venues", ["out"]))
    tr = C.scan_trades(snapshot, idx, mids, cov, cfg.get("venues", ["out"]))
    return {"segment": seg, "horizon": h, **(tr["markouts"].get(seg, {}).get(h) or {"trades": 0})}


class _GapTaker(Strategy):
    """O-S7-L screen: on each reference-gap episode, send an IOC through Outcome's book
    at no worse than the reference mid less the threshold; exit with an IOC after the
    holding horizon (or hold into settlement if it comes first)."""

    def __init__(self, episodes: List[Dict[str, Any]], cfg: Dict[str, Any]):
        self.eps = episodes
        self.cfg = cfg
        self.trades: List[Dict[str, Any]] = []

    def start(self, sim: Sim) -> None:
        busy_until: Dict[str, int] = {}
        hold = int(float(self.cfg.get("horizon_s", 60)) * 1e9)
        for i, ep in enumerate(self.eps):
            coin, t0 = ep["coin"], int(ep["start_ns"])
            if busy_until.get(coin, 0) > t0:
                continue  # one position per outcome at a time
            busy_until[coin] = t0 + hold + 10**9
            sim.schedule(t0, lambda s, ep=ep, i=i: self._enter(s, ep, i))

    def _enter(self, sim: Sim, ep: Dict[str, Any], i: int) -> None:
        bk = sim.book(ep["coin"])
        bb, ba = bk.best()
        if bb is None or ba is None:
            return
        th = float(self.cfg["threshold_c"]) * 1e5
        rec = {"episode": i, "coin": ep["coin"], "cluster": ep["cluster"] or ep["coin"], "side": ep["side"], "decision_ns": sim.now, "start_edge_c": ep["start_edge_c"]}
        # the reference mid at the signal, reconstructed from the recorded edge
        if ep["side"] == "buy":
            ref_mid = ba + ep["start_edge_c"] * 1e5
            limit = int((ref_mid - th) // TICK_PX7 * TICK_PX7)
        else:
            ref_mid = bb - ep["start_edge_c"] * 1e5
            limit = int(-(-(ref_mid + th) // TICK_PX7) * TICK_PX7)
        if not 0 < limit < PX:
            return
        sz6 = int(float(self.cfg.get("size_usdc", 25)) / (limit / PX) * SZ)
        cloid = f"e{i}"
        before = len(sim.fills)
        sim.submit(SimIntent(ep["coin"], ep["side"] == "buy", limit, sz6, "ioc", cloid))
        rec["limit_px7"] = limit
        rec["ref_mid_px7"] = ref_mid
        self.trades.append(rec)
        hold = int(float(self.cfg.get("horizon_s", 60)) * 1e9)
        sim.schedule(sim.now + hold, lambda s, rec=rec, cloid=cloid: self._exit(s, rec, cloid))

    def _exit(self, sim: Sim, rec: Dict[str, Any], cloid: str) -> None:
        got = [f for f in sim.fills if f.cloid == rec.get("cloid_override", cloid)]
        filled = sum(f.sz6 for f in got)
        rec["filled_sz6"] = filled
        if not filled:
            return
        rec["entry_px7"] = sum(f.px7 * f.sz6 for f in got) / filled
        pos = sim.position(rec["coin"])
        if pos == 0:
            return  # settled while held
        buy = pos < 0
        bb, ba = sim.book(rec["coin"]).best()
        slip = int(float(self.cfg.get("exit_slippage_c", 2.0)) * 1e5)
        if buy:
            ref = ba if ba is not None else (bb or PX // 2)
            px = min(PX - TICK_PX7, (ref + slip) // TICK_PX7 * TICK_PX7)
        else:
            ref = bb if bb is not None else (ba or PX // 2)
            px = max(TICK_PX7, -(-(ref - slip) // TICK_PX7) * TICK_PX7)
        sim.submit(SimIntent(rec["coin"], buy, int(px), abs(pos), "ioc", cloid + "x"))
        rec["exit_cloid"] = cloid + "x"

    def finish(self, sim: Sim) -> None:
        pass


@_evidence("simulation")
def reference_gap_taker(snapshot, cfg: Dict[str, Any], seed: int) -> Dict[str, Any]:
    """S1c simulator screen for O-S7-L. Kill rule (PRD S1c): neutral core P&L < 0, i.e.
    the cluster-robust upper 95% bound of the net edge per share is below zero."""
    idx = build_index(snapshot)
    th = float(cfg["threshold_c"])
    _, episodes = C.scan_episodes(snapshot, idx, thresholds=[th])
    segs = set(cfg.get("segments") or [])
    episodes = [e for e in episodes if not segs or e["segment"] in segs]
    hold = float(cfg.get("horizon_s", 60))
    windows: Dict[str, List] = defaultdict(list)
    for e in episodes:
        windows[e["coin"]].append((int(e["start_ns"]) - 120 * 10**9, int(e["start_ns"]) + int((hold + 120) * 1e9)))
    lat = LatencyModel(float(cfg.get("latency_p50_ms", 300)), float(cfg.get("latency_p95_ms", 600)))
    fees = FeeModel(taker_bps=float(cfg.get("taker_fee_bps", 8.75)))
    sim = Sim(snapshot, windows.keys(), latency=lat, fees=fees, ledger=Ledger(), seed=seed, min_notional_usdc=float(cfg.get("min_notional_usdc", 10)), windows=windows, index=idx)
    strat = _GapTaker(episodes, cfg)
    res = sim.run(strat)
    per_cluster: Dict[str, List[float]] = defaultdict(list)
    attempted = filled = unresolved = 0
    for rec in strat.trades:
        attempted += 1
        got = [f for f in res.fills if f.cloid == f"e{rec['episode']}"]
        q = sum(f.sz6 for f in got)
        if not q:
            continue
        filled += 1
        entry = sum(f.px7 * f.sz6 for f in got) / q
        out = [f for f in res.fills if f.cloid == f"e{rec['episode']}x"]
        oq = sum(f.sz6 for f in out)
        fees_usdc = sum(f.fee_usdc for f in got + out)
        if oq:
            exitp = sum(f.px7 * f.sz6 for f in out) / oq
        else:
            inst = idx.at(rec["coin"], rec["decision_ns"])
            if inst is None or inst.settle_fraction is None:
                unresolved += 1  # neither exited nor settled inside the snapshot
                continue
            exitp = inst.settle_fraction * PX
        sign = 1 if rec["side"] == "buy" else -1
        edge_c = (sign * (exitp - entry) / 1e5) - fees_usdc / (q / SZ) * 100
        per_cluster[rec["cluster"]].append(edge_c)
    st = _cluster_stats(per_cluster)
    kill = st["upper95"] is not None and st["upper95"] < 0
    cov_days = C.Coverage(C.coverage_intervals(snapshot)).seconds / 86400
    screen = "KILL (net edge upper 95% < 0)" if kill else ("insufficient (fewer than 2 clusters)" if st["upper95"] is None else "pass to next stage (simulation can only kill)")
    if cov_days < C.INDICATIVE_BELOW_DAYS:
        screen = f"indicative only ({cov_days:.2f} of {C.INDICATIVE_BELOW_DAYS:g} recorded days): {screen}"
    return {
        "covered_days": round(cov_days, 3),
        "threshold_c": th, "horizon_s": hold, "episodes": len(episodes), "attempted": attempted, "filled": filled,
        "race_loss_rate": round(1 - filled / attempted, 4) if attempted else None,
        "unresolved_positions": unresolved,
        "net_edge_c_per_share": st, "requests": res.requests, "rejects": res.rejects,
        "sim_log_sha256": res.log_sha256, "sim_params": res.params,
        "screen": screen,
    }


EVALUATORS: Dict[str, Callable] = {"markout": markout, "reference_gap_taker": reference_gap_taker}
