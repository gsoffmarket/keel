"""S1a census (PRD M1): universe, liquidity, markout volatility, reference-gap episodes
and the minimum-detectable-edge table, computed from one immutable snapshot.

The report is deterministic: the same snapshot and the same Keel version produce the
same bytes, so `keel research census --verify REPORT` can prove a report reproduces.
Every number says what it is measured over; spans too short to support a figure are
reported as such, never extrapolated silently.
"""
from __future__ import annotations

import gzip
import json
import math
from array import array
from bisect import bisect_right
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from keel import __version__
from keel.compact import iter_table, table_paths
from keel.research import mde as M
from keel.research.snapshot import Snapshot
from keel.research.universe import SEGMENTS, InstanceIndex, build_index

PX = 10**7  # px7 per 1 USDC
CENT = 10**5  # px7 per cent
SEC = 10**9
HORIZONS = {"10s": 10, "1m": 60, "5m": 300, "1h": 3600}
GAP_THRESHOLDS_C = (0.3, 0.5, 1.0, 2.0)
BAND_C = 4.0
DEPTH_MIN_USDC = 50.0
MIN_TRADES_FOR_SIGMA = 30
MIN_COVERED_DAYS_FOR_RATES = 1.0  # below this, per-week and per-day rates are not reported
INDICATIVE_BELOW_DAYS = 7.0  # PRD S1a: the census runs over days 0-7


def _r(x: Optional[float], nd: int = 4) -> Optional[float]:
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    return round(float(x), nd)


def _median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _quantile(xs: List[float], q: float) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, int(q * len(s)))]


def _sd(xs: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def icc_anova(groups: Dict[str, List[float]]) -> Optional[float]:
    """One-way ANOVA estimator of the intra-cluster correlation, clipped to [0, 1]."""
    gs = [g for g in groups.values() if g]
    k = len(gs)
    n = sum(len(g) for g in gs)
    if k < 2 or n <= k:
        return None
    grand = sum(sum(g) for g in gs) / n
    ssb = sum(len(g) * (sum(g) / len(g) - grand) ** 2 for g in gs)
    ssw = sum(sum((x - sum(g) / len(g)) ** 2 for x in g) for g in gs)
    msb, msw = ssb / (k - 1), ssw / (n - k)
    n0 = (n - sum(len(g) ** 2 for g in gs) / n) / (k - 1)
    den = msb + (n0 - 1) * msw
    if den <= 0:
        return None
    return min(1.0, max(0.0, (msb - msw) / den))


# ---------------------------------------------------------------- coverage

def coverage_intervals(snapshot: Snapshot) -> List[Tuple[int, int]]:
    """Intervals in which the recorder was running (session start to end, or to the next
    start when a session ended without an end event), clipped to the snapshot."""
    starts: List[Tuple[int, str]] = []
    out: List[Tuple[int, int]] = []
    open_at: Optional[int] = None
    last_ns = snapshot.start_ns
    for e in sorted(snapshot.events(), key=lambda e: e["ns"]):
        ns = int(e["ns"])
        k = e.get("kind")
        if k == "session_start":
            if open_at is not None:
                out.append((open_at, last_ns))
            open_at = ns
        elif k == "session_end" and open_at is not None:
            out.append((open_at, ns))
            open_at = None
        last_ns = ns
    if open_at is not None:
        out.append((open_at, last_ns))
    lo, hi = snapshot.start_ns, snapshot.end_ns
    return [(max(a, lo), min(b, hi)) for a, b in out if min(b, hi) > max(a, lo)]


class Coverage:
    def __init__(self, intervals: List[Tuple[int, int]]):
        self.iv = sorted(intervals)
        self.starts = [a for a, _ in self.iv]

    def same(self, a: int, b: int) -> bool:
        i = bisect_right(self.starts, a) - 1
        return i >= 0 and self.iv[i][0] <= a and b <= self.iv[i][1]

    @property
    def seconds(self) -> float:
        return sum(b - a for a, b in self.iv) / SEC


# ---------------------------------------------------------------- books

class Mids:
    """Per coin: times and 2*mid (px7) at every change of the two-sided mid."""

    def __init__(self) -> None:
        self.t: Dict[str, array] = {}
        self.v: Dict[str, array] = {}

    def add(self, coin: str, ns: int, mid2: Optional[int]) -> None:
        tv = self.t.get(coin)
        if tv is None:
            tv = self.t[coin] = array("q")
            self.v[coin] = array("q")
        vv = self.v[coin]
        if tv and ns < tv[-1]:
            ns = tv[-1]  # keep the series ordered if a message arrives out of block order
        val = -1 if mid2 is None else mid2
        if vv and vv[-1] == val:
            return
        tv.append(ns)
        vv.append(val)

    def at(self, coin: str, ns: int) -> Optional[float]:
        """Mid (px7) in force at `ns`; None if one-sided or unknown."""
        tv = self.t.get(coin)
        if not tv:
            return None
        i = bisect_right(tv, ns) - 1
        if i < 0:
            return None
        v = self.v[coin][i]
        return None if v < 0 else v / 2


def _depth(levels_json: str, mid: float) -> Tuple[float, float]:
    try:
        lv = json.loads(levels_json)
    except (ValueError, TypeError):
        return 0.0, 0.0
    band = BAND_C / 100
    m = mid / PX
    bd = sum(float(l["px"]) * float(l["sz"]) for l in lv[0] if float(l["px"]) >= m - band)
    ad = sum(float(l["px"]) * float(l["sz"]) for l in lv[1] if float(l["px"]) <= m + band)
    return bd, ad


def scan_books(snapshot: Snapshot, idx: InstanceIndex, venues: Iterable[str]) -> Tuple[Mids, Dict[str, Dict[str, List[float]]]]:
    vs = set(venues)
    mids = Mids()
    stats: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: {"spread_c": [], "bid_depth": [], "ask_depth": []})
    for path in table_paths(snapshot.root, "books", snapshot.days):
        cur: Optional[Tuple[str, int]] = None
        pending: Optional[Dict[str, Any]] = None

        def flush(row: Optional[Dict[str, Any]]) -> None:
            if not row or row["bid_px7"] is None or row["ask_px7"] is None:
                return
            inst = idx.at(row["coin"], row["recv_ns"])
            if inst is None or inst.venue not in vs:
                return
            mid = (row["bid_px7"] + row["ask_px7"]) / 2
            bd, ad = _depth(row["levels_json"], mid)
            s = stats[inst.key]
            s["spread_c"].append((row["ask_px7"] - row["bid_px7"]) / CENT)
            s["bid_depth"].append(bd)
            s["ask_depth"].append(ad)

        for row in iter_table(path, ["recv_ns", "exch_ms", "coin", "bid_px7", "ask_px7", "levels_json"]):
            # exchange (block) time, the same clock as trades; local receipt only if missing
            coin = row["coin"]
            ns = int(row["exch_ms"]) * 10**6 if row.get("exch_ms") else int(row["recv_ns"])
            b, a = row["bid_px7"], row["ask_px7"]
            mids.add(coin, ns, (b + a) if b is not None and a is not None else None)
            k = (coin, ns // (60 * SEC))
            if k != cur:
                flush(pending)
                cur = k
            pending = row
        flush(pending)
    return mids, stats


# ---------------------------------------------------------------- trades and markouts

def scan_trades(snapshot: Snapshot, idx: InstanceIndex, mids: Mids, cov: Coverage, venues: Iterable[str]) -> Dict[str, Any]:
    vs = set(venues)
    per_inst: Dict[str, Dict[str, float]] = defaultdict(lambda: {"trades": 0, "volume_usdc": 0.0})
    mk: Dict[str, Dict[str, Dict[str, List[float]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # seg -> horizon -> cluster -> markouts
    seen = set()
    for path in table_paths(snapshot.root, "trades", snapshot.days):
        for t in iter_table(path, ["recv_ns", "exch_ms", "coin", "tid", "px7", "sz6", "aggressor"]):
            ns = int(t["exch_ms"]) * 10**6 if t.get("exch_ms") else int(t["recv_ns"])
            # a subscription replays recent trades, possibly from the previous day: count
            # each trade once and only inside the snapshot
            if (t["coin"], t["tid"]) in seen or not (snapshot.start_ns <= ns < snapshot.end_ns):
                continue
            seen.add((t["coin"], t["tid"]))
            inst = idx.at(t["coin"], ns)
            if inst is None or inst.venue not in vs:
                continue
            px, sz = int(t["px7"]), int(t["sz6"])
            pi = per_inst[inst.key]
            pi["trades"] += 1
            pi["volume_usdc"] += px * sz / (PX * 10**6)
            sgn = 1 if t.get("aggressor") == "B" else -1
            seg = inst.segment
            for h, secs in HORIZONS.items():
                t2 = ns + secs * SEC
                if inst.removed_ns and t2 > inst.removed_ns:
                    continue
                if not cov.same(ns, t2):
                    continue
                m = mids.at(t["coin"], t2)
                if m is None:
                    continue
                mk[seg][h][inst.cluster].append(sgn * (m - px) / CENT)
            if inst.event_ns and ns < inst.event_ns and cov.same(ns, inst.event_ns):
                m = mids.at(t["coin"], inst.event_ns)
                if m is not None:
                    mk[seg]["pre_start"][inst.cluster].append(sgn * (m - px) / CENT)
            if inst.settle_fraction is not None:
                mk[seg]["settlement"][inst.cluster].append(sgn * (inst.settle_fraction * PX - px) / CENT)
    markouts: Dict[str, Dict[str, Any]] = {}
    for seg, hs in mk.items():
        markouts[seg] = {}
        for h, groups in hs.items():
            xs = [x for g in groups.values() for x in g]
            rho = icc_anova(groups)
            m_per = len(xs) / len(groups) if groups else 0
            sd = _sd(xs)
            markouts[seg][h] = {
                "trades": len(xs),
                "clusters": len(groups),
                "sigma_c": _r(sd, 3) if len(xs) >= MIN_TRADES_FOR_SIGMA else None,
                "sigma_c_low_n": _r(sd, 3) if 0 < len(xs) < MIN_TRADES_FOR_SIGMA else None,
                "mean_c": _r(sum(xs) / len(xs), 3) if xs else None,
                "rho": _r(rho, 3),
                "trades_per_cluster": _r(m_per, 2),
                "design_effect": _r(M.design_effect(m_per, rho or 0.0), 2) if rho is not None else None,
            }
    return {"per_instance": per_inst, "markouts": markouts}


# ---------------------------------------------------------------- reference-gap episodes

def scan_episodes(snapshot: Snapshot, idx: InstanceIndex, thresholds: Iterable[float] = GAP_THRESHOLDS_C, covered_days: float = 0.0) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Episodes in which Outcome is executable through the matched reference mid by at
    least `θ` cents, from usable aligned rows only (stale or unmatched references can
    never open or extend an episode)."""
    ths = sorted(thresholds)
    episodes: List[Dict[str, Any]] = []
    rows_total = rows_usable = 0
    excluded: Counter = Counter()
    open_ep: Dict[Tuple[str, str, float], Dict[str, Any]] = {}
    last_ns: Dict[str, int] = {}

    def close(key, ns: int, reason: str, row: Optional[Dict[str, Any]] = None) -> None:
        ep = open_ep.pop(key)
        ep["end_ns"] = ns
        ep["duration_s"] = round((ns - ep["start_ns"]) / SEC, 3)
        if row is not None and reason == "closed":
            d_out = ((row["ask_px7"] - ep["_oa"]) if ep["side"] == "buy" else (ep["_ob"] - row["bid_px7"])) / CENT
            rmid = (row["ref_bid_px7"] + row["ref_ask_px7"]) / 2
            d_ref = ((ep["_rm"] - rmid) if ep["side"] == "buy" else (rmid - ep["_rm"])) / CENT
            reason = "outcome moved" if d_out > d_ref else ("reference moved" if d_ref > d_out else "both moved")
        ep["closed_by"] = reason
        for k in ("_oa", "_ob", "_rm"):
            ep.pop(k, None)
        episodes.append(ep)

    for path in table_paths(snapshot.root, "refs", snapshot.days):
        for row in iter_table(path):
            rows_total += 1
            coin, ns = row["coin"], int(row["recv_ns"])
            prev = last_ns.get(coin)
            last_ns[coin] = ns
            if not row.get("usable"):
                excluded[row.get("why") or "unusable"] += 1
                for key in [k for k in open_ep if k[0] == coin]:
                    close(key, ns, "reference unusable")
                continue
            rows_usable += 1
            rmid = (row["ref_bid_px7"] + row["ref_ask_px7"]) / 2
            edges = {"buy": (rmid - row["ask_px7"]) / CENT, "sell": (row["bid_px7"] - rmid) / CENT}
            inst = idx.at(coin, ns)
            for side, edge in edges.items():
                for th in ths:
                    key = (coin, side, th)
                    if key in open_ep:
                        if edge >= th:
                            ep = open_ep[key]
                            ep["max_edge_c"] = max(ep["max_edge_c"], round(edge, 4))
                        else:
                            close(key, ns, "closed", row)
                    elif edge >= th:
                        sz = row["ask_sz6"] if side == "buy" else row["bid_sz6"]
                        pxs = row["ask_px7"] if side == "buy" else row["bid_px7"]
                        open_ep[key] = {
                            "coin": coin, "side": side, "threshold_c": th, "start_ns": ns,
                            "segment": inst.segment if inst else None, "cluster": inst.cluster if inst else None,
                            "start_edge_c": round(edge, 4), "max_edge_c": round(edge, 4),
                            "size_usdc_at_price": round((sz or 0) * pxs / (PX * 10**6), 2),
                            "reference_decided": rmid >= 0.98 * PX or rmid <= 0.02 * PX,
                            "tier": row.get("tier"), "confidence": row.get("confidence"),
                            "_oa": row["ask_px7"], "_ob": row["bid_px7"], "_rm": rmid,
                        }
        for key in list(open_ep):
            close(key, last_ns.get(key[0], open_ep[key]["start_ns"]), "data end")
    episodes.sort(key=lambda e: (e["threshold_c"], e["start_ns"], e["coin"], e["side"]))
    summary: Dict[str, Any] = {"rows": rows_total, "rows_usable": rows_usable, "rows_excluded": dict(sorted(excluded.items())), "by_threshold": {}}
    for th in ths:
        eps = [e for e in episodes if e["threshold_c"] == th]
        done = [e for e in eps if e["closed_by"] not in ("data end",)]
        durs = [e["duration_s"] for e in done]
        summary["by_threshold"][f"{th:g}c"] = {
            "episodes": len(eps),
            "per_covered_day": _r(len(eps) / covered_days, 2) if covered_days >= MIN_COVERED_DAYS_FOR_RATES else None,
            "per_covered_hour": _r(len(eps) / (covered_days * 24), 3) if covered_days > 0 else None,
            "clusters": len({e["cluster"] for e in eps}),
            "by_segment": dict(sorted(Counter(e["segment"] or "unknown" for e in eps).items())),
            "duration_s_p50": _r(_median(durs), 3),
            "duration_s_p90": _r(_quantile(durs, 0.9), 3),
            "lasting_over": {f"{s}s": sum(1 for d in durs if d > s) for s in (0.5, 1, 2, 5, 30)},
            "size_usdc_p50": _r(_median([e["size_usdc_at_price"] for e in eps]), 2),
            "max_edge_c_p50": _r(_median([e["max_edge_c"] for e in eps]), 3),
            "closed_by": dict(sorted(Counter(e["closed_by"] for e in eps).items())),
            "reference_decided": sum(1 for e in eps if e["reference_decided"]),
        }
    return summary, episodes


# ---------------------------------------------------------------- report

def build(snapshot: Snapshot, venues: Iterable[str] = ("out",)) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    venues = sorted(set(venues))
    idx = build_index(snapshot)
    cov = Coverage(coverage_intervals(snapshot))
    mids, bstats = scan_books(snapshot, idx, venues)
    tr = scan_trades(snapshot, idx, mids, cov, venues)
    span_s = (snapshot.end_ns - snapshot.start_ns) / SEC
    cov_days = cov.seconds / 86400
    ep_summary, episodes = scan_episodes(snapshot, idx, covered_days=cov_days)
    rates_ok = cov_days >= MIN_COVERED_DAYS_FOR_RATES
    insts = idx.instances(venues)

    universe: Dict[str, Any] = {}
    for seg in SEGMENTS:
        xs = [i for i in insts if i.segment == seg]
        if not xs:
            continue
        settled = [i for i in xs if i.settled_ns or i.removed_ns]
        clusters_settled = {i.cluster for i in settled}
        clusters_dir = {i.cluster_dir for i in settled}
        lifetimes = [(i.removed_ns - i.first_seen_ns) / 3.6e12 for i in xs if i.removed_ns]
        vols = [tr["per_instance"].get(i.key, {}).get("volume_usdc", 0.0) for i in xs]
        med_spread, two_sided_50 = [], 0
        for i in xs:
            s = bstats.get(i.key)
            if not s or not s["spread_c"]:
                continue
            med_spread.append(_median(s["spread_c"]))
            if _median([min(b, a) for b, a in zip(s["bid_depth"], s["ask_depth"])]) >= DEPTH_MIN_USDC:
                two_sided_50 += 1
        with_books = sum(1 for i in xs if bstats.get(i.key, {}).get("spread_c"))
        per_week = len(clusters_settled) / cov_days * 7 if rates_ok else None
        live = idx.live_counts.get(f"{venues[0]}/{seg}", []) if len(venues) == 1 else []
        universe[seg] = {
            "instances_seen": len(xs),
            "live_mean": _r(sum(live) / len(live), 1) if live else None,
            "removed_in_span": len(settled),
            "settled_official": sum(1 for i in xs if i.settle_fraction is not None),
            "clusters_seen": len({i.cluster for i in xs}),
            "clusters_resolved": len(clusters_settled),
            "clusters_resolved_directional": len(clusters_dir),
            "clusters_resolved_per_week": _r(per_week, 1),
            "lifetime_h_p50": _r(_median(lifetimes), 2),
            "trades": sum(int(tr["per_instance"].get(i.key, {}).get("trades", 0)) for i in xs),
            "volume_usdc": _r(sum(vols), 2),
            "volume_usdc_per_instance_p50": _r(_median(vols), 2),
            "instances_with_books": with_books,
            "spread_c_p50": _r(_median([x for x in med_spread if x is not None]), 4),
            "share_two_sided_depth_ge_50usdc": _r(two_sided_50 / with_books, 3) if with_books else None,
        }

    # MDE table from what was measured
    mde_rows = []
    for seg, u in universe.items():
        pw = u["clusters_resolved_per_week"]
        mk = tr["markouts"].get(seg, {})
        for days in (30, 45, 60, 90):
            for part in (1.0, 0.5, 0.25):
                n = M.clusters(pw or 0, days, part)
                row = {"segment": seg, "days": days, "participation": part, "clusters": int(n)}
                row["hold_to_resolution_mde_c_sigma50"] = _r(M.mde(50.0, n), 2) if pw else None
                row["hold_to_resolution_mde_c_sigma50_k3"] = _r(M.mde(50.0, n, k=3), 2) if pw else None
                for h in ("1m", "5m", "1h"):
                    sig = (mk.get(h) or {}).get("sigma_c")
                    row[f"exit_{h}_mde_c"] = _r(M.mde(sig, n), 3) if sig and pw else None
                mde_rows.append(row)
    sharpe = [{"days": d, "mde_daily_sharpe": _r(M.mde_sharpe(d), 3), "mde_daily_sharpe_k3": _r(M.mde_sharpe(d, k=3), 3)} for d in (30, 60, 84, 90)]

    limits = []
    if not rates_ok:
        limits.append(f"Recorder coverage is {cov.seconds / 3600:.1f} h. Per-week and per-day rates, and the MDE table that depends on them, need at least {MIN_COVERED_DAYS_FOR_RATES:g} covered day and are not reported. Counts, liquidity, markout volatility and episodes below describe only the covered hours.")
    elif cov_days < INDICATIVE_BELOW_DAYS:
        limits.append(f"Recorder coverage is {cov_days:.2f} days; the PRD census needs 7 (S1a). Rates per week are scaled from this span and are indicative only.")
    if any((tr["markouts"].get(s, {}).get("1m") or {}).get("sigma_c") is None for s in universe):
        limits.append(f"Markout sigma is reported only with at least {MIN_TRADES_FOR_SIGMA} trades; segments below that show sigma_c_low_n.")
    if not ep_summary["rows"]:
        limits.append("No reference rows in this snapshot (no mappings, or the days were compacted before the refs table existed).")

    report = {
        "kind": "keel.census.s1a",
        "keel_version": __version__,
        "snapshot": {"id": snapshot.id, **snapshot.describe()},
        "venues": venues,
        "span": {"days": len(snapshot.days), "span_hours": _r(span_s / 3600, 2), "covered_hours": _r(cov.seconds / 3600, 2), "coverage_pct": _r(100 * cov.seconds / span_s, 2) if span_s else None, "sessions": len(cov.iv), "meta_polls": idx.polls},
        "universe": universe,
        "markouts_c": tr["markouts"],
        "episodes": ep_summary,
        "mde": {"method": "one-sided alpha 0.05, power 0.80, O'Brien-Fleming inflation 1.03; k3 = Bonferroni over 3 concurrent pilots; clusters = measured resolved clusters/week x participation x days/7", "table": mde_rows, "inventory_daily_sharpe": sharpe},
        "limits": limits,
        "status": "complete" if cov_days >= INDICATIVE_BELOW_DAYS else ("indicative" if rates_ok else "insufficient span"),
    }
    return report, episodes


def canonical(report: Dict[str, Any]) -> bytes:
    return (json.dumps(report, sort_keys=True, indent=1, ensure_ascii=False) + "\n").encode()


def write(report: Dict[str, Any], episodes: List[Dict[str, Any]], out_dir: Path) -> Tuple[Path, Path, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"census-{report['snapshot']['id'][:12]}"
    jp, mp, ep = out_dir / f"{stem}.json", out_dir / f"{stem}.md", out_dir / f"{stem}-episodes.jsonl.gz"
    jp.write_bytes(canonical(report))
    mp.write_text(render_markdown(report))
    with gzip.GzipFile(ep, "wb", mtime=0) as fh:  # mtime=0 keeps the file byte-identical
        for e in episodes:
            fh.write(json.dumps(e, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    return jp, mp, ep


def _f(x: Any) -> str:
    if x is None:
        return "—"
    if isinstance(x, float):
        return f"{x:,.4g}" if abs(x) < 1000 else f"{x:,.0f}"
    if isinstance(x, int):
        return f"{x:,}"
    return str(x)


def render_markdown(r: Dict[str, Any]) -> str:
    sp = r["span"]
    L = [f"# Keel census (S1a) — snapshot {r['snapshot']['id'][:12]}", ""]
    L.append(f"**Status: {r['status']}.**")
    L.append("")
    L.append(f"Days {r['snapshot']['days'][0]} to {r['snapshot']['days'][-1]} ({sp['days']} day(s)); recorder ran {sp['covered_hours']} of {sp['span_hours']} h ({sp['coverage_pct']}%) across {sp['sessions']} session(s); {sp['meta_polls']} market-list polls. Venues: {', '.join(r['venues'])}. Keel {r['keel_version']}.")
    if r["limits"]:
        L += ["", "**Limits of this report**", ""] + [f"- {x}" for x in r["limits"]]
    L += ["", "## Universe", "", "| Segment | Instances | Live (mean) | Resolved | Official results | Clusters resolved | per week | Lifetime p50 (h) | Trades | Volume (USDC) | Spread p50 (¢) | Two-sided ≥ $50 in ±4¢ |", "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for seg, u in r["universe"].items():
        L.append(f"| {seg} | {_f(u['instances_seen'])} | {_f(u['live_mean'])} | {_f(u['removed_in_span'])} | {_f(u['settled_official'])} | {_f(u['clusters_resolved'])} | {_f(u['clusters_resolved_per_week'])} | {_f(u['lifetime_h_p50'])} | {_f(u['trades'])} | {_f(u['volume_usdc'])} | {_f(u['spread_c_p50'])} | {_f(u['share_two_sided_depth_ge_50usdc'])} |")
    L += ["", "## Markouts (aggressor's view, cents per share)", "", "Mid at the horizon minus the trade price, signed by the aggressor. Only horizons inside one recorder session count.", "", "| Segment | Horizon | Trades | Clusters | σ (¢) | Mean (¢) | ρ | Trades/cluster | Design effect |", "|---|---|---|---|---|---|---|---|---|"]
    for seg, hs in r["markouts_c"].items():
        for h in ("10s", "1m", "5m", "1h", "pre_start", "settlement"):
            x = hs.get(h)
            if x:
                sig = _f(x["sigma_c"]) if x["sigma_c"] is not None else (f"({_f(x['sigma_c_low_n'])}, n<30)" if x["sigma_c_low_n"] is not None else "—")
                L.append(f"| {seg} | {h} | {_f(x['trades'])} | {_f(x['clusters'])} | {sig} | {_f(x['mean_c'])} | {_f(x['rho'])} | {_f(x['trades_per_cluster'])} | {_f(x['design_effect'])} |")
    e = r["episodes"]
    L += ["", "## Reference-gap episodes (O-S7)", "", f"{_f(e['rows'])} aligned rows, {_f(e['rows_usable'])} usable. Excluded: {e['rows_excluded'] or 'none'}. Gap = Outcome executable price through the Polymarket mid; stale references cannot open or extend an episode.", "", "| Threshold | Episodes | per covered day | per covered hour | Clusters | Duration p50 (s) | p90 (s) | >1 s | >5 s | Size p50 (USDC) | Closed by | Reference decided |", "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for th, x in e["by_threshold"].items():
        L.append(f"| {th} | {_f(x['episodes'])} | {_f(x['per_covered_day'])} | {_f(x['per_covered_hour'])} | {_f(x['clusters'])} | {_f(x['duration_s_p50'])} | {_f(x['duration_s_p90'])} | {_f(x['lasting_over']['1s'])} | {_f(x['lasting_over']['5s'])} | {_f(x['size_usdc_p50'])} | {x['closed_by'] or '—'} | {_f(x['reference_decided'])} |")
    L += ["", "## Minimum detectable edge", "", r["mde"]["method"] + ".", "", "| Segment | Days | Participation | Clusters | Hold σ=50¢ | Hold, K=3 | Exit 1m | Exit 5m | Exit 1h |", "|---|---|---|---|---|---|---|---|---|"]
    shown = [x for x in r["mde"]["table"] if x["participation"] in (1.0, 0.5) and x["hold_to_resolution_mde_c_sigma50"] is not None]
    if not shown:
        L.append("| — | — | — | — | not computed: see Limits | | | | |")
    for x in shown:
        if True:
            L.append(f"| {x['segment']} | {x['days']} | {x['participation']:.0%} | {_f(x['clusters'])} | {_f(x['hold_to_resolution_mde_c_sigma50'])} | {_f(x['hold_to_resolution_mde_c_sigma50_k3'])} | {_f(x['exit_1m_mde_c'])} | {_f(x['exit_5m_mde_c'])} | {_f(x['exit_1h_mde_c'])} |")
    L += ["", "Inventory-carrying strategies (daily Sharpe): " + "; ".join(f"{x['days']} d → {x['mde_daily_sharpe']} (K=3: {x['mde_daily_sharpe_k3']})" for x in r["mde"]["inventory_daily_sharpe"]) + "."]
    L += ["", f"Reproduce: `keel research census --days {','.join(r['snapshot']['days'])}` gives byte-identical output; `keel research census --verify <this report's .json>` checks it.", ""]
    return "\n".join(L)
