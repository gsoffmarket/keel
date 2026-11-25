"""S1b maker census (PRD M1; O9 §5): realised P&L of the addresses that already provide
liquidity on Outcome, by segment, from public trades (both counterparties are on every
Hyperliquid trade; the aggressor flag tells who was passive).

For every address with at least `min_maker_fills` passive fills in the snapshot:
    P&L = cash flows of all its fills in the snapshot
          + net position acquired in the snapshot x (settlement value if the instance
            settled in the snapshot, else the last mid: marked, labelled as such)
          - fees on position-reducing fills and on settlement (assumed rates, stated)
Rewards are not observable here and are excluded: the census reports P&L *ex-rewards*.

Inference is kill-only (O9 §5, PRD S1b exit): the aggregate passive-liquidity P&L of a
segment is summed per resolution cluster and its cluster-robust upper 95% bound is
reported. If that bound is below 0, market-making hypotheses in the segment are killed
before registration. A positive result promotes nothing: survivorship, hedges on other
venues and multiple addresses per firm are invisible to this method.
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, Iterable, Optional, Tuple

from keel import __version__
from keel.compact import iter_table, table_paths
from keel.research.census import INDICATIVE_BELOW_DAYS, PX, Coverage, Mids, _r, coverage_intervals, scan_books
from keel.research.snapshot import Snapshot
from keel.research.universe import SEGMENTS, build_index

SZ = 10**6
Z95 = 1.6448536269514722  # one-sided 95%
MIN_CLUSTERS_FOR_VERDICT = 20  # a normal-approximation bound on fewer clusters is not trusted


def _bucket(px7: int) -> str:
    p = px7 / PX
    for lo, hi in ((0.0, 0.1), (0.1, 0.3), (0.3, 0.7), (0.7, 0.9), (0.9, 1.0001)):
        if lo <= p < hi:
            return f"{lo:.1f}-{min(hi, 1.0):.1f}"
    return "other"


class Book:
    """One address's position and cash in one instance (side-0 shares)."""

    __slots__ = ("pos", "cash", "fees", "maker_fills", "taker_fills", "maker_volume", "spread_capture")

    def __init__(self) -> None:
        self.pos = 0.0
        self.cash = 0.0
        self.fees = 0.0
        self.maker_fills = 0
        self.taker_fills = 0
        self.maker_volume = 0.0
        self.spread_capture = 0.0

    def fill(self, qty: float, px: float, maker: bool, fee_rate: float, mid: Optional[float]) -> None:
        """qty > 0 buys, < 0 sells, px in USDC per share."""
        reducing = self.pos != 0 and (self.pos > 0) != (qty > 0)
        if reducing:
            self.fees += abs(min(abs(qty), abs(self.pos))) * px * fee_rate
        self.pos += qty
        self.cash -= qty * px
        if maker:
            self.maker_fills += 1
            self.maker_volume += abs(qty) * px
            if mid is not None:
                self.spread_capture += (mid - px) * qty  # buying below mid / selling above earns
        else:
            self.taker_fills += 1


def cluster_upper_bound(per_cluster: Dict[str, float]) -> Tuple[Optional[float], Optional[float]]:
    """Total and its one-sided upper 95% bound, treating clusters as independent."""
    vals = list(per_cluster.values())
    n = len(vals)
    if n == 0:
        return None, None
    total = sum(vals)
    if n < 2:
        return total, None
    m = total / n
    var = sum((v - m) ** 2 for v in vals) / (n - 1)
    se_total = math.sqrt(n * var)
    return total, total + Z95 * se_total


def build(
    snapshot: Snapshot,
    venues: Iterable[str] = ("out",),
    min_maker_fills: int = 50,
    maker_fee_bps: float = 0.0,
    taker_fee_bps: float = 8.75,
    settlement_fee_bps: float = 0.0,
    mids: Optional[Mids] = None,
) -> Dict[str, Any]:
    venues = sorted(set(venues))
    idx = build_index(snapshot)
    cov_days = Coverage(coverage_intervals(snapshot)).seconds / 86400
    full_span = cov_days >= INDICATIVE_BELOW_DAYS
    if mids is None:
        mids, _ = scan_books(snapshot, idx, venues)
    books: Dict[Tuple[str, str], Book] = defaultdict(Book)  # (address, instance) -> Book
    inst_of: Dict[str, Any] = {}
    last_px: Dict[str, float] = {}
    seen = set()
    trades = 0
    for path in table_paths(snapshot.root, "trades", snapshot.days):
        for t in iter_table(path, ["recv_ns", "exch_ms", "coin", "tid", "px7", "sz6", "aggressor", "buyer", "seller"]):
            ns = int(t["exch_ms"]) * 10**6 if t.get("exch_ms") else int(t["recv_ns"])
            if (t["coin"], t["tid"]) in seen or not (snapshot.start_ns <= ns < snapshot.end_ns):
                continue
            seen.add((t["coin"], t["tid"]))
            inst = idx.at(t["coin"], ns)
            if inst is None or inst.venue not in venues or not t.get("buyer") or not t.get("seller"):
                continue
            trades += 1
            inst_of[inst.key] = inst
            px = int(t["px7"]) / PX
            q = int(t["sz6"]) / SZ
            m = mids.at(t["coin"], ns - 1)  # the mid just before the trade
            mid = m / PX if m is not None else None
            buyer_maker = t.get("aggressor") == "A"  # seller aggressed: the bid was passive
            books[(t["buyer"], inst.key)].fill(q, px, buyer_maker, (maker_fee_bps if buyer_maker else taker_fee_bps) / 1e4, mid)
            books[(t["seller"], inst.key)].fill(-q, px, not buyer_maker, (taker_fee_bps if buyer_maker else maker_fee_bps) / 1e4, mid)
            last_px[inst.key] = px

    # value open positions
    by_addr: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"pnl": 0.0, "realised_settled": 0.0, "marked": 0.0, "fees": 0.0, "maker_fills": 0, "taker_fills": 0, "maker_volume": 0.0, "spread_capture": 0.0, "instances": set(), "segments": defaultdict(float)})
    per_inst_pnl: Dict[Tuple[str, str], float] = {}
    for (addr, ikey), b in books.items():
        inst = inst_of[ikey]
        if inst.settle_fraction is not None:
            value = b.pos * inst.settle_fraction
            settle_fee = abs(value) * settlement_fee_bps / 1e4
            kind = "settled"
        else:
            m = mids.at(inst.coin, snapshot.end_ns)
            mark = m / PX if m is not None else last_px.get(ikey, 0.0)
            value, settle_fee, kind = b.pos * mark, 0.0, "marked"
        pnl = b.cash + value - b.fees - settle_fee
        per_inst_pnl[(addr, ikey)] = pnl
        a = by_addr[addr]
        a["pnl"] += pnl
        a["realised_settled" if kind == "settled" else "marked"] += pnl
        a["fees"] += b.fees + settle_fee
        a["maker_fills"] += b.maker_fills
        a["taker_fills"] += b.taker_fills
        a["maker_volume"] += b.maker_volume
        a["spread_capture"] += b.spread_capture
        a["instances"].add(ikey)
        a["segments"][inst.segment] += pnl

    makers = {a for a, v in by_addr.items() if v["maker_fills"] >= min_maker_fills}
    # every trade has a buyer and a seller valued at the same mark, so across all
    # addresses P&L must equal minus the fees: a check on the accounting itself
    conservation = {"sum_pnl_usdc": round(sum(v["pnl"] for v in by_addr.values()), 6), "sum_fees_usdc": round(sum(v["fees"] for v in by_addr.values()), 6)}
    conservation["residual_usdc"] = round(conservation["sum_pnl_usdc"] + conservation["sum_fees_usdc"], 6)
    segs: Dict[str, Any] = {}
    for seg in SEGMENTS:
        per_cluster: Dict[str, float] = defaultdict(float)
        per_bucket: Dict[str, float] = defaultdict(float)
        n_addr = set()
        vol = 0.0
        settled_share = 0.0
        for (addr, ikey), pnl in per_inst_pnl.items():
            inst = inst_of[ikey]
            if addr not in makers or inst.segment != seg:
                continue
            n_addr.add(addr)
            per_cluster[inst.cluster] += pnl
            b = books[(addr, ikey)]
            vol += b.maker_volume
            if inst.settle_fraction is not None:
                settled_share += abs(pnl)
            lp = last_px.get(ikey)
            per_bucket[_bucket(int(lp * PX)) if lp is not None else "other"] += pnl
        if not n_addr:
            continue
        total, ub = cluster_upper_bound(per_cluster)
        tot_abs = sum(abs(v) for (a, k), v in per_inst_pnl.items() if a in makers and inst_of[k].segment == seg)
        if ub is None or len(per_cluster) < MIN_CLUSTERS_FOR_VERDICT:
            verdict = f"insufficient: {len(per_cluster)} clusters (needs {MIN_CLUSTERS_FOR_VERDICT})"
        else:
            verdict = "KILL market making here (ex-rewards upper 95% bound < 0)" if ub < 0 else "no kill (suggestive only; cannot promote)"
            if not full_span:
                verdict = f"indicative only (coverage {cov_days:.2f} of {INDICATIVE_BELOW_DAYS:g} days): " + verdict
        segs[seg] = {
            "makers": len(n_addr),
            "clusters": len(per_cluster),
            "maker_volume_usdc": _r(vol, 2),
            "pnl_ex_rewards_usdc": _r(total, 2),
            "pnl_per_100_usdc_maker_volume": _r(100 * total / vol, 4) if vol else None,
            "upper95_usdc": _r(ub, 2),
            "share_of_pnl_from_settled_instances": _r(settled_share / tot_abs, 3) if tot_abs else None,
            "by_price_bucket_usdc": {k: _r(v, 2) for k, v in sorted(per_bucket.items())},
            "verdict": verdict,
        }
    top = sorted(((a, v) for a, v in by_addr.items() if a in makers), key=lambda x: -x[1]["maker_volume"])[:20]
    return {
        "kind": "keel.census.s1b_makers",
        "keel_version": __version__,
        "snapshot": {"id": snapshot.id, **snapshot.describe()},
        "venues": venues,
        "assumptions": {
            "min_maker_fills": min_maker_fills,
            "fees": f"opening fills free; position-reducing fills pay maker {maker_fee_bps:g} bps / taker {taker_fee_bps:g} bps; settlement {settlement_fee_bps:g} bps (assumed; per-market rates are not in the public trade feed)",
            "rewards": "excluded (not observable from trades)",
            "window": "P&L of activity inside the snapshot only: positions held before it are not known, so their price change is not counted",
            "open_positions": "marked to the last two-sided mid at the end of the snapshot (labelled 'marked'); settled instances use the official settle fraction",
        },
        "covered_days": _r(cov_days, 3),
        "status": "complete" if full_span else "indicative",
        "trades": trades,
        "addresses": len(by_addr),
        "makers": len(makers),
        "segments": segs,
        "conservation": conservation,
        "top_makers": [
            {
                "address": a,
                "maker_fills": v["maker_fills"], "taker_fills": v["taker_fills"],
                "maker_volume_usdc": _r(v["maker_volume"], 2),
                "pnl_ex_rewards_usdc": _r(v["pnl"], 2), "of_which_settled": _r(v["realised_settled"], 2), "of_which_marked": _r(v["marked"], 2),
                "spread_capture_usdc": _r(v["spread_capture"], 2), "fees_usdc": _r(v["fees"], 2),
                "instances": len(v["instances"]),
                "pnl_by_segment": {k: _r(x, 2) for k, x in sorted(v["segments"].items())},
            }
            for a, v in top
        ],
    }


def render_markdown(r: Dict[str, Any]) -> str:
    L = [f"# Keel maker census (S1b) — snapshot {r['snapshot']['id'][:12]}", ""]
    L.append(f"**Status: {r['status']}** ({r['covered_days']} recorded days; the PRD's S1b runs over days 3-14).")
    L.append("")
    L.append(f"{r['trades']:,} trades, {r['addresses']:,} addresses, {r['makers']:,} with at least {r['assumptions']['min_maker_fills']} passive fills. Keel {r['keel_version']}.")
    L += ["", "**Method and limits**", ""] + [f"- {k.replace('_', ' ').capitalize()}: {v}" for k, v in r["assumptions"].items()]
    L.append("- Kill-only: a positive result is observational and cannot promote any hypothesis.")
    L += ["", "| Segment | Makers | Clusters | Maker volume (USDC) | P&L ex-rewards (USDC) | per $100 volume | Upper 95% | Verdict |", "|---|---|---|---|---|---|---|---|"]
    for seg, s in r["segments"].items():
        L.append(f"| {seg} | {s['makers']} | {s['clusters']} | {s['maker_volume_usdc']:,} | {s['pnl_ex_rewards_usdc']} | {s['pnl_per_100_usdc_maker_volume']} | {s['upper95_usdc']} | {s['verdict']} |")
    L += ["", "## Largest makers by passive volume", "", "| Address | Passive fills | Taker fills | Passive volume | P&L ex-rewards | settled / marked | Spread capture | Fees |", "|---|---|---|---|---|---|---|---|"]
    for m in r["top_makers"]:
        L.append(f"| `{m['address'][:10]}…` | {m['maker_fills']:,} | {m['taker_fills']:,} | {m['maker_volume_usdc']:,} | {m['pnl_ex_rewards_usdc']} | {m['of_which_settled']} / {m['of_which_marked']} | {m['spread_capture_usdc']} | {m['fees_usdc']} |")
    L.append("")
    return "\n".join(L)
