"""Data-quality and first-census report over the raw store.

One pass over the raw files and the event log. Everything here is computed from
recorded data only; nothing is fetched. Definitions are in docs/data-quality.md.
"""
from __future__ import annotations

import bisect
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from keel.hl.coins import parse_coin
from keel.hl.metadata import parse_outcome_meta, summarize
from keel.recorder.store import iter_events, iter_raw, raw_files
from keel.units import PX_SCALE, TICK_PX7, UnitError, px7, sz6

MIN_NS = 60 * 10**9
ONE_CENT_PX7 = PX_SCALE // 100


def _pct(xs: List[float], q: float) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    k = min(len(s) - 1, max(0, int(math.ceil(q * len(s))) - 1))
    return s[k]


def _iso(ns: Optional[int]) -> Optional[str]:
    if ns is None:
        return None
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


@dataclass
class Book:
    recv_ns: int
    exch_ms: int
    bids: List[Tuple[int, int]]  # (px7, sz6) best first
    asks: List[Tuple[int, int]]


def parse_book(data: Dict[str, Any], recv_ns: int) -> Book:
    levels = data.get("levels") or [[], []]
    bids = [(px7(l["px"]), sz6(l["sz"])) for l in levels[0]]
    asks = [(px7(l["px"]), sz6(l["sz"])) for l in levels[1]]
    return Book(recv_ns, int(data.get("time", 0)), bids, asks)


def book_metrics(b: Book) -> Dict[str, Any]:
    """Spread (cents) and depth within +-1 cent of mid (shares)."""
    if not b.bids or not b.asks:
        return {"two_sided": False}
    bb, ba = b.bids[0][0], b.asks[0][0]
    mid2 = bb + ba  # 2*mid, avoids rounding
    lo, hi = mid2 - 2 * ONE_CENT_PX7, mid2 + 2 * ONE_CENT_PX7
    depth = sum(s for p, s in b.bids if 2 * p >= lo) + sum(s for p, s in b.asks if 2 * p <= hi)
    return {
        "two_sided": True,
        "crossed": bb >= ba,
        "spread_c": (ba - bb) / PX_SCALE * 100,
        "mid": mid2 / 2 / PX_SCALE,
        "depth_1c_sh": depth / 1e6,
    }


@dataclass
class Window:
    start_ns: int
    end_ns: int

    @property
    def minutes(self) -> int:
        return max(0, (self.end_ns - self.start_ns) // MIN_NS)


@dataclass
class Acc:
    first_ns: Optional[int] = None
    last_ns: Optional[int] = None
    first_ws_ns: Optional[int] = None
    last_ws_ns: Optional[int] = None
    ws_minutes: set = field(default_factory=set)
    msgs: Counter = field(default_factory=Counter)
    lat_ms: List[float] = field(default_factory=list)
    book_gaps_s: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))
    last_book_recv: Dict[str, int] = field(default_factory=dict)
    last_book: Dict[str, Book] = field(default_factory=dict)
    ws_book_hist: Dict[str, List[Tuple[int, int, int]]] = field(default_factory=lambda: defaultdict(list))  # (recv_ns, bb, ba)
    crossed: int = 0
    one_sided: int = 0
    off_tick: int = 0
    books: int = 0
    book_recv_ns: List[int] = field(default_factory=list)
    bad_units: int = 0
    tids: set = field(default_factory=set)
    trades: int = 0
    trade_dupes: int = 0
    trades_backfill: int = 0
    trade_notional_by_coin: Counter = field(default_factory=Counter)
    trades_by_coin: Counter = field(default_factory=Counter)
    addr_count: Counter = field(default_factory=Counter)
    meta_polls: List[int] = field(default_factory=list)
    last_meta: Optional[Dict[str, Any]] = None
    audits: List[Tuple[str, int, Book]] = field(default_factory=list)
    mirror: List[Tuple[int, str, Any]] = field(default_factory=list)
    perp_ctx_polls: int = 0
    refs: Dict[str, Dict[str, Any]] = field(default_factory=lambda: defaultdict(lambda: {"ok": 0, "bytes": 0, "last_ns": None}))
    sessions_start: List[int] = field(default_factory=list)


def _in(w: Optional[Window], ns: int) -> bool:
    return w is None or (w.start_ns <= ns < w.end_ns)


def scan(root: Path, window: Optional[Window] = None) -> Tuple[Acc, List[Dict[str, Any]]]:
    acc = Acc()
    events = [e for e in iter_events(root) if _in(window, int(e.get("ns", 0)))]
    session_start = min((e["ns"] for e in events if e["kind"] == "session_start"), default=None)
    acc.sessions_start = sorted(e["ns"] for e in events if e["kind"] == "session_start")
    for rec in iter_raw(root):
        ns = int(rec["recv_ns"])
        if not _in(window, ns):
            continue
        acc.first_ns = ns if acc.first_ns is None else min(acc.first_ns, ns)
        acc.last_ns = ns if acc.last_ns is None else max(acc.last_ns, ns)
        src = rec["src"]
        msg = rec["msg"]
        if src == "hl_ws":
            acc.first_ws_ns = ns if acc.first_ws_ns is None else min(acc.first_ws_ns, ns)
            acc.last_ws_ns = ns if acc.last_ws_ns is None else max(acc.last_ws_ns, ns)
            acc.ws_minutes.add(ns // MIN_NS)
            ch = msg.get("channel") if isinstance(msg, dict) else None
            acc.msgs[ch or "?"] += 1
            if ch == "l2Book":
                data = msg["data"]
                coin = data.get("coin", "")
                try:
                    b = parse_book(data, ns)
                except (UnitError, KeyError, TypeError, ValueError):
                    acc.bad_units += 1
                    continue
                acc.books += 1
                acc.book_recv_ns.append(ns)
                if b.exch_ms:
                    acc.lat_ms.append(ns / 1e6 - b.exch_ms)
                prev = acc.last_book_recv.get(coin)
                if prev is not None:
                    acc.book_gaps_s[coin].append((ns - prev) / 1e9)
                acc.last_book_recv[coin] = ns
                acc.last_book[coin] = b
                if any(p % TICK_PX7 for p, _ in b.bids + b.asks):
                    acc.off_tick += 1
                if not b.bids or not b.asks:
                    acc.one_sided += 1
                else:
                    if b.bids[0][0] >= b.asks[0][0]:
                        acc.crossed += 1
                    acc.ws_book_hist[coin].append((ns, b.bids[0][0], b.asks[0][0]))
            elif ch == "trades":
                for t in msg.get("data") or []:
                    tid = (t.get("coin"), t.get("tid"))
                    if tid in acc.tids:
                        acc.trade_dupes += 1
                        continue
                    acc.tids.add(tid)
                    acc.trades += 1
                    if session_start and int(t.get("time", 0)) * 10**6 < session_start:
                        acc.trades_backfill += 1
                    coin = t.get("coin", "")
                    try:
                        notional = px7(t["px"]) * sz6(t["sz"]) / (PX_SCALE * 1e6)
                    except (UnitError, KeyError):
                        acc.bad_units += 1
                        continue
                    acc.trade_notional_by_coin[coin] += notional
                    acc.trades_by_coin[coin] += 1
                    for a in t.get("users") or []:
                        acc.addr_count[a] += 1
        elif src == "hl_rest":
            req = rec.get("req") or {}
            typ = req.get("type")
            if typ == "outcomeMeta" and isinstance(msg, dict):
                acc.meta_polls.append(ns)
                acc.last_meta = msg
            elif typ == "l2Book" and isinstance(msg, dict) and "levels" in msg:
                try:
                    acc.audits.append((req.get("coin", ""), ns, parse_book(msg, ns)))
                except (UnitError, KeyError, TypeError, ValueError):
                    acc.bad_units += 1
            elif typ == "recentTrades":
                acc.mirror.append((ns, req.get("coin", ""), msg))
            elif typ == "metaAndAssetCtxs":
                acc.perp_ctx_polls += 1
        elif src.startswith("ref_"):
            r = acc.refs[src[4:]]
            r["ok"] += 1
            r["last_ns"] = ns
            r["bytes"] += len(json.dumps(msg)) if not isinstance(msg, str) else len(msg)
    return acc, events


def connection_intervals(events: List[Dict[str, Any]], end_ns: int) -> List[Tuple[int, int]]:
    """[start, end) intervals during which the WebSocket was connected."""
    out: List[Tuple[int, int]] = []
    up: Optional[int] = None
    for e in sorted(events, key=lambda e: e["ns"]):
        k = e["kind"]
        if k == "ws_connected":
            if up is None:
                up = e["ns"]
        elif k in ("ws_disconnected", "session_end"):
            if up is not None:
                out.append((up, e["ns"]))
                up = None
        elif k == "session_start" and up is not None:
            # previous process died without a session_end: close at its last known activity
            out.append((up, e["ns"]))
            up = None
    if up is not None:
        out.append((up, end_ns))
    return out


def unmasked_minutes(intervals: List[Tuple[int, int]], msg_minutes: set, window: Window) -> Tuple[int, List[Tuple[int, int]]]:
    """A minute is unmasked if the socket was connected for all of it and at least one
    message arrived in it. Returns (count, masked spans as [start_ns, end_ns))."""
    good = 0
    masked: List[Tuple[int, int]] = []
    m0 = window.start_ns // MIN_NS
    m1 = window.end_ns // MIN_NS
    for m in range(m0, m1):
        a, b = m * MIN_NS, (m + 1) * MIN_NS
        covered = any(s <= a and e >= b for s, e in intervals)
        if covered and m in msg_minutes:
            good += 1
        else:
            if masked and masked[-1][1] == a:
                masked[-1] = (masked[-1][0], b)
            else:
                masked.append((a, b))
    return good, masked


def per_day_unmasked(intervals: List[Tuple[int, int]], msg_minutes: set, window: Window) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for m in range(window.start_ns // MIN_NS, window.end_ns // MIN_NS):
        a, b = m * MIN_NS, (m + 1) * MIN_NS
        day = datetime.fromtimestamp(a / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
        d = out.setdefault(day, {"minutes": 0, "unmasked": 0})
        d["minutes"] += 1
        if m in msg_minutes and any(s <= a and e >= b for s, e in intervals):
            d["unmasked"] += 1
    for d in out.values():
        d["pct"] = round(100 * d["unmasked"] / d["minutes"], 2) if d["minutes"] else None
    return out


def fast_book_coverage(acc: "Acc", fast_coins: set) -> Dict[str, Any]:
    """For outcomes on the fast stream: share of minutes, between an outcome's first
    and last book, that contain at least one stored book (changes or the heartbeat)."""
    total = covered = 0
    worst: List[Tuple[float, str]] = []
    for coin in fast_coins:
        ts = sorted(t for t, _, _ in acc.ws_book_hist.get(coin, []))
        if len(ts) < 2:
            continue
        mins = {t // MIN_NS for t in ts}
        span = ts[-1] // MIN_NS - ts[0] // MIN_NS + 1
        total += span
        covered += len(mins)
        worst.append((len(mins) / span, coin))
    worst.sort()
    return {"coins": len(worst), "coin_minutes": total, "covered_pct": round(100 * covered / total, 2) if total else None, "worst": [(c, round(100 * f, 1)) for f, c in worst[:5]]}


def tier_intervals(events: List[Dict[str, Any]], end_ns: int) -> Dict[str, List[Tuple[int, int]]]:
    """Fast-stream intervals per outcome from `tier_change` events (activity tiering)."""
    out: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
    open_at: Dict[str, int] = {}
    for e in sorted(events, key=lambda e: e["ns"]):
        k = e["kind"]
        if k == "tier_change":
            c = e["coin"]
            if e["tier"] == "fast":
                open_at.setdefault(c, e["ns"])
            elif c in open_at:
                out[c].append((open_at.pop(c), e["ns"]))
        elif k in ("session_end", "session_start"):
            for c, t in list(open_at.items()):
                out[c].append((t, e["ns"]))
            open_at.clear()
        elif k == "outcome_removed" and e.get("coin") in open_at:
            out[e["coin"]].append((open_at.pop(e["coin"]), e["ns"]))
    for c, t in open_at.items():
        out[c].append((t, end_ns))
    return out


def fast_coverage_from_tiers(acc: "Acc", intervals: Dict[str, List[Tuple[int, int]]]) -> Dict[str, Any]:
    """Whole minutes inside each outcome's fast intervals that contain a stored book."""
    total = covered = 0
    worst: List[Tuple[float, str]] = []
    for coin, ivs in intervals.items():
        mins = {t // MIN_NS for t, _, _ in acc.ws_book_hist.get(coin, [])}
        t_c = c_c = 0
        for a, b in ivs:
            for m in range(a // MIN_NS + 1, b // MIN_NS):
                t_c += 1
                c_c += m in mins
        if t_c:
            total += t_c
            covered += c_c
            worst.append((c_c / t_c, coin))
    worst.sort()
    return {"coins": len(worst), "coin_minutes": total, "covered_pct": round(100 * covered / total, 2) if total else None, "worst": [(c, round(100 * f, 1)) for f, c in worst[:5]], "basis": "tier_change intervals"}


def audit_agreement(acc: Acc) -> Dict[str, Any]:
    """Compare REST l2Book snapshots with the latest WebSocket top of book before them."""
    n = agree = no_ws = 0
    ages: List[float] = []
    for coin, ns, b in acc.audits:
        hist = acc.ws_book_hist.get(coin) or []
        k = bisect.bisect_right(hist, (ns, 1 << 62, 1 << 62))
        if k == 0 or not b.bids or not b.asks:
            no_ws += 1
            continue
        h = hist[k - 1]
        n += 1
        ages.append((ns - h[0]) / 1e9)
        if h[1] == b.bids[0][0] and h[2] == b.asks[0][0]:
            agree += 1
    return {"compared": n, "top_of_book_agree": agree, "agree_pct": round(100 * agree / n, 1) if n else None, "no_ws_reference": no_ws, "ws_age_s_p50": _pct(ages, 0.5)}


def mirror_check(acc: Acc) -> Dict[str, Any]:
    """Side-1 trades should each mirror a side-0 trade: same time, prices summing to $1."""
    by_ns = sorted(acc.mirror, key=lambda x: x[0])
    pairs = checked = matched = 0
    i = 0
    while i < len(by_ns) - 1:
        (n0, c0, m0), (n1, c1, m1) = by_ns[i], by_ns[i + 1]
        try:
            s0, s1 = parse_coin(c0), parse_coin(c1)
        except ValueError:
            i += 1
            continue
        if s0.outcome == s1.outcome and s0.side == 0 and s1.side == 1 and isinstance(m0, list) and isinstance(m1, list):
            pairs += 1
            zero = {(int(t["time"]), px7(t["px"])) for t in m0}
            # only check side-1 trades inside the time span covered by the side-0 response
            t_min = min((int(t["time"]) for t in m0), default=0)
            for t in m1:
                if int(t["time"]) < t_min:
                    continue
                checked += 1
                if (int(t["time"]), PX_SCALE - px7(t["px"])) in zero:
                    matched += 1
            i += 2
        else:
            i += 1
    return {"pairs": pairs, "side1_trades_checked": checked, "mirrored": matched, "mirrored_pct": round(100 * matched / checked, 1) if checked else None}


def build_report(root: Path, window: Optional[Window] = None) -> Dict[str, Any]:
    root = Path(root)
    acc, events = scan(root, window)
    if acc.first_ns is None:
        return {"empty": True, "root": str(root)}
    # The scored span is the recorder's own: from the first recorder session start (or first
    # stream message) to the last session end (or last stream message). Records written
    # later by other commands (e.g. `keel settlements`) do not stretch it.
    starts = [e["ns"] for e in events if e["kind"] == "session_start"]
    ends = [e["ns"] for e in events if e["kind"] == "session_end"]
    start = min(starts + ([acc.first_ws_ns] if acc.first_ws_ns else []) or [acc.first_ns])
    end = max(ends + ([acc.last_ws_ns] if acc.last_ws_ns else []) or [acc.last_ns])
    w = window or Window(start, end)
    # only score whole minutes inside the observed span
    w_eff = Window(max(w.start_ns, start) // MIN_NS * MIN_NS + MIN_NS, min(w.end_ns, end) // MIN_NS * MIN_NS)
    intervals = connection_intervals(events, end)
    good, masked = unmasked_minutes(intervals, acc.ws_minutes, w_eff)
    total_min = w_eff.minutes
    per_day = per_day_unmasked(intervals, acc.ws_minutes, w_eff)

    # universe and census from the last outcomeMeta + last WS book per coin
    meta = parse_outcome_meta(acc.last_meta) if acc.last_meta else None
    census: Dict[str, Any] = {}
    stale_share = None
    if meta:
        rows = defaultdict(list)
        live_coins = {o.coin0: o for o in meta.outcomes.values()}
        quiet = 0
        for coin, o in live_coins.items():
            b = acc.last_book.get(coin)
            gaps = acc.book_gaps_s.get(coin, [])
            if gaps and max(gaps) > 60:
                quiet += 1
            if b is None:
                continue
            m = book_metrics(b)
            m["venue"] = o.venue
            m["category"] = o.category
            rows[(o.venue, o.category)].append(m)
        for (venue, cat), ms in sorted(rows.items()):
            two = [m for m in ms if m["two_sided"]]
            notional = sum(acc.trade_notional_by_coin.get(c, 0.0) for c, o in live_coins.items() if o.venue == venue and o.category == cat)
            census[f"{venue}/{cat}"] = {
                "books": len(ms),
                "two_sided": len(two),
                "median_spread_c": round(statistics.median([m["spread_c"] for m in two]), 4) if two else None,
                "median_depth_1c_sh": round(statistics.median([m["depth_1c_sh"] for m in two]), 1) if two else None,
                "trade_notional_usdc": round(notional, 2),
                "trades": sum(acc.trades_by_coin.get(c, 0) for c, o in live_coins.items() if o.venue == venue and o.category == cat),
            }
        stale_share = round(100 * quiet / max(1, len(live_coins)), 1)

    gaps_all = [g for gs in acc.book_gaps_s.values() for g in gs]
    meta_gaps = [(b - a) / 1e9 for a, b in zip(acc.meta_polls, acc.meta_polls[1:])]
    removed = [e for e in events if e["kind"] == "outcome_removed"]
    # official results can arrive after the window (retries, `keel settlements`): look them up
    # across the whole event log, keyed by the instances removed inside the window
    all_events = list(iter_events(root)) if window else events
    settled = {e["instance_id"]: e for e in all_events if e["kind"] == "outcome_settled"}
    missing = {e["instance_id"] for e in all_events if e["kind"] == "outcome_settlement_missing"} - set(settled)
    official = [settled[e["instance_id"]] for e in removed if e["instance_id"] in settled]
    mid_agree = [s for s in official if s.get("last_mid") is not None and (s["last_mid"] >= 0.5) == (s["settle_fraction"] >= 0.5)]
    book_seconds = {ns // 10**9 for ns in acc.book_recv_ns}
    session_ends = [e for e in events if e["kind"] == "session_end"]
    last_status = session_ends[-1]["status"] if session_ends else {}
    settle_capture = [e for e in removed if e.get("last_book_recv_ns") and e["removed_seen_ns"] - e["last_book_recv_ns"] <= 10 * MIN_NS]
    ek = Counter(e["kind"] for e in events)
    disk = defaultdict(int)
    disk_window = defaultdict(int)
    days_in = {datetime.fromtimestamp(t / 1e9, tz=timezone.utc).strftime("%Y-%m-%d") for t in range(start - start % (86400 * 10**9), end + 1, 86400 * 10**9)} | {_iso(end)[:10]}
    for p in raw_files(root):
        disk[p.parts[-3]] += p.stat().st_size
        if p.parts[-2] in days_in:
            disk_window[p.parts[-3]] += p.stat().st_size
    unmasked_pct = round(100 * good / total_min, 2) if total_min else None

    report: Dict[str, Any] = {
        "generated": _iso(int(datetime.now(tz=timezone.utc).timestamp() * 1e9)),
        "root": str(root),
        "span": {"start": _iso(start), "end": _iso(end), "hours": round((end - start) / 3.6e12, 2), "scored_minutes": total_min, "sessions": len(acc.sessions_start)},
        "coverage": {
            "unmasked_minutes": good,
            "unmasked_pct": unmasked_pct,
            "per_day": per_day,
            "masked_spans": [{"start": _iso(a), "end": _iso(b), "minutes": (b - a) // MIN_NS} for a, b in masked][:50],
            "reconnects": sum(1 for e in events if e["kind"] == "ws_disconnected" and e.get("reason") != "stop"),
            "connect_failures": ek.get("ws_connect_failed", 0),
        },
        "messages": dict(acc.msgs),
        "books": {
            "updates": acc.books,
            "coins_with_books": len(acc.last_book),
            "crossed": acc.crossed,
            "one_sided": acc.one_sided,
            "off_tick": acc.off_tick,
            "update_gap_s_p50": _pct(gaps_all, 0.5),
            "update_gap_s_p99": _pct(gaps_all, 0.99),
            "update_gap_s_max": max(gaps_all) if gaps_all else None,
            "coins_with_a_gap_over_60s_pct": stale_share,
            "mode": last_status.get("book_mode", "standard"),
            "fast_book_coins": last_status.get("fast_book_coins", 0),
            "unchanged_fast_snapshots_not_stored": (last_status.get("ws_counts") or {}).get("l2Book_unchanged_not_stored", 0),
            "seconds_with_book_messages": len(book_seconds),
        },
        "latency_ms_recv_minus_exchange": {"p50": _pct(acc.lat_ms, 0.5), "p95": _pct(acc.lat_ms, 0.95), "p99": _pct(acc.lat_ms, 0.99), "note": "includes local clock offset; see keel doctor"},
        "trades": {
            "unique": acc.trades,
            "duplicates_dropped": acc.trade_dupes,
            "backfilled_from_before_start": acc.trades_backfill,
            "notional_usdc": round(sum(acc.trade_notional_by_coin.values()), 2),
            "distinct_addresses": len(acc.addr_count),
            "top_addresses_by_fills": acc.addr_count.most_common(10),
        },
        "meta": {"polls": len(acc.meta_polls), "poll_gap_s_max": max(meta_gaps) if meta_gaps else None, "universe": summarize(meta.outcomes.values()) if meta else None, "questions": len(meta.questions) if meta else None},
        "universe_changes": {"added": ek.get("outcome_added", 0), "removed": ek.get("outcome_removed", 0), "changed": ek.get("outcome_changed", 0), "named_settled": ek.get("question_named_settled", 0)},
        "settlements": {
            "removed_outcomes": len(removed),
            "with_book_within_10min_of_removal": len(settle_capture),
            "capture_pct": round(100 * len(settle_capture) / len(removed), 1) if removed else None,
            "official_results": len(official),
            "official_pct": round(100 * len(official) / len(removed), 1) if removed else None,
            "official_missing": len(missing),
            "official_pending": len(removed) - len(official) - len(missing),
            "final_mid_on_winning_side_pct": round(100 * len(mid_agree) / len(official), 1) if official else None,
            "results": dict(Counter(str(s["settle_fraction"]) for s in official)),
            "labels": dict(Counter(e.get("settle_source") for e in removed)),
            "examples": [{k: e.get(k) for k in ("coin", "venue", "category", "last_mid")} | {"removed_seen": _iso(e.get("removed_seen_ns")), "official": (settled.get(e["instance_id"]) or {}).get("settle_fraction")} for e in removed[:10]],
        },
        "rest_audit": audit_agreement(acc),
        "mirror_audit": mirror_check(acc),
        "perp_ctx_polls": acc.perp_ctx_polls,
        "references": {k: {"ok": v["ok"], "bytes": v["bytes"], "last": _iso(v["last_ns"])} for k, v in acc.refs.items()},
        "reference_errors": dict(Counter(e.get("name") for e in events if e["kind"] == "reference_error")),
        "references_configured": sorted({r["name"] for e in events if e["kind"] == "session_start" for r in (e.get("config") or {}).get("references", [])}),
        "errors": {k: v for k, v in ek.items() if "error" in k or "fail" in k or "refused" in k},
        "storage_bytes": dict(disk),
        "storage_bytes_span_days": dict(disk_window),
        "budget_events": {k: ek.get(k, 0) for k in ("budget_enforced", "budget_exceeded", "disk_low", "disk_critical")},
        "census": census,
        "parse_failures": acc.bad_units,
    }
    # live reference feed (Polymarket market channel) and the stale-reference guard
    try:
        from keel.refs.align import pm_audit_agreement, summarize as ref_summarize

        report["reference_alignment"] = ref_summarize(root, max(w.start_ns, start), min(w.end_ns, end) + 1)
        report["pm_audit"] = pm_audit_agreement(root)
    except Exception as exc:  # never let the reference section break the core report
        report["reference_alignment"] = {"error": repr(exc)[:300]}
        report["pm_audit"] = {}
    report["pm_feed"] = {
        "connects": ek.get("pm_connected", 0),
        "disconnects": sum(1 for e in events if e["kind"] == "pm_disconnected" and e.get("reason") != "stop"),
        "mappings_added": ek.get("mapping_added", 0),
        "mappings_removed": ek.get("mapping_removed", 0),
        "last_matching": next((e for e in reversed(events) if e["kind"] == "matching_summary"), None),
    }
    cfgs = [e.get("config") or {} for e in events if e["kind"] == "session_start"]
    fast_coins: set = set()
    if meta and cfgs and cfgs[-1].get("book_mode") == "fast":
        fv, fc = cfgs[-1].get("fast_venues"), cfgs[-1].get("fast_categories")
        fast_coins = {o.coin0 for o in meta.outcomes.values() if (fv is None or o.venue in fv) and (fc is None or o.category in fc)}
    if any(e["kind"] == "tier_change" for e in events):
        tiv = tier_intervals(events, end)
        report["fast_books"] = fast_coverage_from_tiers(acc, tiv)
        report["tiering"] = {"changes": ek.get("tier_change", 0), "outcomes_ever_fast": len(tiv), "fast_outcome_hours": round(sum(b - a for ivs in tiv.values() for a, b in ivs) / 3.6e12, 2)}
    else:
        report["fast_books"] = fast_book_coverage(acc, fast_coins) if fast_coins else None
    report["restarts"] = max(0, len(acc.sessions_start) - 1)
    report["checks"] = acceptance_checks(report)
    return report


def acceptance_checks(r: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pass/fail against the PRD's recorder criteria. Short spans are reported as
    'not yet measurable' rather than passed."""
    hours = r["span"]["hours"]
    pct = r["coverage"]["unmasked_pct"]
    out = []

    def add(name, ok, detail):
        out.append({"check": name, "status": ok, "detail": detail})

    add("Unmasked minutes >= 95%", "PASS" if pct is not None and pct >= 95 else "FAIL", f"{pct}% over {r['span']['scored_minutes']} scored minutes")
    add("7 consecutive days >= 95% (M0 acceptance)", "NOT YET" if hours < 168 else ("PASS" if pct and pct >= 95 else "FAIL"), f"{hours} h recorded")
    s = r["settlements"]
    add("Official settlement result for every removal", "NOT YET" if not s["removed_outcomes"] else ("PASS" if s["official_pct"] == 100 else ("PENDING" if s["official_pending"] and not s["official_missing"] else "FAIL")), f"{s['official_results']}/{s['removed_outcomes']} via settledOutcome ({s['official_missing']} missing, {s['official_pending']} pending)")
    add("100% settlement capture", "NOT YET" if not s["removed_outcomes"] else ("PASS" if s["capture_pct"] == 100 else "FAIL"), f"{s['with_book_within_10min_of_removal']}/{s['removed_outcomes']} removals had a book within 10 min")
    b = r["books"]
    add("No crossed books", "PASS" if b["crossed"] == 0 else "FAIL", f"{b['crossed']} crossed of {b['updates']} updates")
    add("All prices on the 0.00001 tick", "PASS" if b["off_tick"] == 0 else "FAIL", f"{b['off_tick']} off-tick books")
    a = r["rest_audit"]
    add("REST audit agrees with WebSocket top of book", "NOT YET" if not a["compared"] else ("PASS" if (a["agree_pct"] or 0) >= 90 else "REVIEW"), f"{a['top_of_book_agree']}/{a['compared']} agree (book may move between the two reads)")
    m = r["mirror_audit"]
    add("Side-1 trades mirror side-0", "NOT YET" if not m["side1_trades_checked"] else ("PASS" if (m["mirrored_pct"] or 0) >= 99 else "REVIEW"), f"{m['mirrored']}/{m['side1_trades_checked']}")
    conf, refs, rerr = r.get("references_configured", []), r.get("references", {}), r.get("reference_errors", {})
    dead = [n for n in conf if not (refs.get(n) or {}).get("ok")]
    add("Every reference feed delivered data", "NOT YET" if not conf else ("PASS" if not dead else "FAIL"), (f"no data from {dead}" if dead else f"{len(conf)} feeds OK") + (f"; errors {rerr}" if rerr else ""))
    fb = r.get("fast_books")
    if fb and fb.get("covered_pct") is not None:
        add("Fast-stream outcomes have a stored book in >= 99% of minutes", "PASS" if fb["covered_pct"] >= 99 else "FAIL", f"{fb['covered_pct']}% of {fb['coin_minutes']:,} outcome-minutes over {fb['coins']} outcomes; worst {fb['worst']}")
    ra = r.get("reference_alignment") or {}
    if ra.get("rows"):
        add("Fresh reference for >= 95% of matched Outcome book observations", "PASS" if ra["usable_pct"] >= 95 else "FAIL", f"{ra['usable_pct']}% of {ra['rows']:,} rows usable; reasons {ra['unusable_reasons']}")
        add("No suspect mappings (median gap between the two bid-ask ranges <= 5 cents)", "PASS" if not ra["suspect_mappings"] else "REVIEW", f"{len(ra['median_abs_mid_diff_c'])} mapped outcomes, median |mid difference| {ra.get('median_mid_diff_c_overall')} cents; suspect {ra['suspect_mappings'] or 'none'}")
    elif r.get("pm_feed", {}).get("mappings_added"):
        add("Fresh reference for >= 95% of matched Outcome book observations", "FAIL", "mappings exist but no aligned rows")
    pa = r.get("pm_audit") or {}
    if pa.get("compared"):
        add("Polymarket REST audit agrees with the rebuilt stream book", "PASS" if pa["agree_pct"] >= 90 else "REVIEW", f"{pa['agree']}/{pa['compared']}")
    add("No parse failures", "PASS" if r["parse_failures"] == 0 else "FAIL", f"{r['parse_failures']}")
    return out


def render_markdown(r: Dict[str, Any]) -> str:
    if r.get("empty"):
        return f"# Keel data-quality report\n\nNo recorded data under `{r['root']}`.\n"
    L: List[str] = []
    sp, cv, bk = r["span"], r["coverage"], r["books"]
    L.append("# Keel data-quality report")
    L.append("")
    L.append(f"Generated {r['generated']} from `{r['root']}`. Span {sp['start']} to {sp['end']} ({sp['hours']} h, {sp['sessions']} recorder session(s)).")
    L.append("")
    L.append("## Checks")
    L.append("")
    L.append("| Check | Status | Detail |")
    L.append("| --- | --- | --- |")
    for c in r["checks"]:
        L.append(f"| {c['check']} | {c['status']} | {c['detail']} |")
    L.append("")
    L.append("## Coverage")
    L.append("")
    L.append(f"- Unmasked minutes: {cv['unmasked_minutes']} of {sp['scored_minutes']} ({cv['unmasked_pct']}%). A minute is unmasked when the WebSocket was connected throughout and at least one message arrived.")
    L.append(f"- Reconnects: {cv['reconnects']}; failed connection attempts: {cv['connect_failures']}; recorder restarts: {r.get('restarts', 0)}.")
    if len(cv.get("per_day") or {}) > 1:
        L.append("- Per UTC day: " + "; ".join(f"{d} {v['unmasked']}/{v['minutes']} ({v['pct']}%)" for d, v in cv["per_day"].items()))
    fb = r.get("fast_books")
    if fb:
        L.append(f"- Fast-stream book coverage: {fb['covered_pct']}% of {fb['coin_minutes']:,} outcome-minutes across {fb['coins']} outcomes (worst {fb['worst']}).")
    if cv["masked_spans"]:
        L.append("- Masked spans: " + "; ".join(f"{m['start']} to {m['end']} ({m['minutes']} min)" for m in cv["masked_spans"][:10]))
    L.append(f"- Messages by channel: " + ", ".join(f"{k} {v:,}" for k, v in sorted(r["messages"].items())))
    L.append("")
    L.append("## Order books")
    L.append("")
    L.append(f"- {bk['updates']:,} book messages across {bk['coins_with_books']} outcomes. Crossed: {bk['crossed']}. One-sided: {bk['one_sided']:,}. Off-tick: {bk['off_tick']}.")
    L.append(f"- Book stream mode: {bk['mode']} ({bk['fast_book_coins']} outcomes on the fast stream; {bk['unchanged_fast_snapshots_not_stored']:,} unchanged fast snapshots not stored). Book messages arrived in {bk['seconds_with_book_messages']:,} distinct seconds.")
    L.append(f"- Time between book messages per outcome: p50 {bk['update_gap_s_p50']} s, p99 {bk['update_gap_s_p99']} s, max {bk['update_gap_s_max']} s. Outcomes with any gap over 60 s: {bk['coins_with_a_gap_over_60s_pct']}%.")
    lat = r["latency_ms_recv_minus_exchange"]
    L.append(f"- Receive time minus exchange time: p50 {_fmt(lat['p50'])} ms, p95 {_fmt(lat['p95'])} ms, p99 {_fmt(lat['p99'])} ms ({lat['note']}).")
    a = r["rest_audit"]
    L.append(f"- REST audit: {a['top_of_book_agree']} of {a['compared']} REST snapshots matched the latest WebSocket top of book (median WebSocket age {_fmt(a['ws_age_s_p50'])} s).")
    L.append("")
    L.append("## Trades")
    L.append("")
    t = r["trades"]
    L.append(f"- {t['unique']:,} unique trades (${t['notional_usdc']:,.2f} notional); {t['duplicates_dropped']:,} duplicates dropped by trade id; {t['backfilled_from_before_start']:,} arrived as subscription backfill from before the session started.")
    L.append(f"- {t['distinct_addresses']:,} distinct addresses. Most active (fills involved): " + ", ".join(f"{a[:8]}…{a[-4:]} ({n})" for a, n in t["top_addresses_by_fills"][:5]))
    m = r["mirror_audit"]
    L.append(f"- Mirror audit: {m['mirrored']} of {m['side1_trades_checked']} side-1 trades matched a side-0 trade at the same time with prices summing to $1.")
    L.append("")
    L.append("## Universe and settlements")
    L.append("")
    me = r["meta"]
    L.append(f"- {me['polls']} `outcomeMeta` polls (largest gap {_fmt(me['poll_gap_s_max'])} s). Live universe: {json.dumps(me['universe'])}; {me['questions']} questions.")
    uc = r["universe_changes"]
    L.append(f"- Changes seen: {uc['added']} added (includes the initial listing), {uc['removed']} removed, {uc['changed']} edited, {uc['named_settled']} named outcomes settled inside questions.")
    s = r["settlements"]
    L.append(f"- Settlement capture: {s['with_book_within_10min_of_removal']} of {s['removed_outcomes']} removals had a recorded book within 10 minutes. Official results (`settledOutcome`): {s['official_results']} ({s['results']}); missing {s['official_missing']}, pending {s['official_pending']}. Final mid on the winning side: {_fmt(s['final_mid_on_winning_side_pct'])}%.")
    for e in s["examples"][:5]:
        L.append(f"  - {e['coin']} ({e['venue']}/{e['category']}) removed {e['removed_seen']}, last mid {e['last_mid']}, official result {e['official']}")
    L.append("")
    L.append("## First census (last recorded book per outcome)")
    L.append("")
    L.append("Indicative only: one snapshot per outcome and trades inside this span.")
    L.append("")
    L.append("| Deployer / category | Books | Two-sided | Median spread (¢) | Median depth ±1¢ (shares) | Trades | Notional (USDC) |")
    L.append("| --- | --- | --- | --- | --- | --- | --- |")
    for k, v in r["census"].items():
        L.append(f"| {k} | {v['books']} | {v['two_sided']} | {_fmt(v['median_spread_c'])} | {_fmt(v['median_depth_1c_sh'])} | {v['trades']:,} | {v['trade_notional_usdc']:,.2f} |")
    L.append("")
    L.append("## References and other sources")
    L.append("")
    L.append(f"- Perp context polls (`metaAndAssetCtxs`): {r['perp_ctx_polls']}.")
    if r["references"]:
        for k, v in r["references"].items():
            L.append(f"- {k}: {v['ok']} successful polls, {v['bytes']:,} bytes, last {v['last']}.")
    else:
        L.append("- No reference-feed data in this span.")
    if r["errors"]:
        L.append(f"- Errors and refusals: {r['errors']}")
    L.append("")
    L.append("## Live reference feed and freshness")
    L.append("")
    pf = r.get("pm_feed") or {}
    lm = pf.get("last_matching") or {}
    L.append(f"- Polymarket market channel: {pf.get('connects', 0)} connects, {pf.get('disconnects', 0)} unplanned disconnects. Mappings added {pf.get('mappings_added', 0)}, removed {pf.get('mappings_removed', 0)}.")
    if lm:
        L.append(f"- Last matching run: {lm.get('matched')} of {lm.get('contracts')} Outcome sports contracts matched ({lm.get('levels')}); unmatched reasons {lm.get('reasons')}.")
    ra = r.get("reference_alignment") or {}
    if ra.get("rows"):
        L.append(f"- Aligned rows: {ra['rows']:,} Outcome book observations on {ra['mapped_coins_seen']} matched outcomes; usable {ra['usable']:,} ({ra['usable_pct']}%); coin-minutes with a usable reference {ra['coin_minutes_usable_pct']}%.")
        L.append(f"- Unusable reasons: {ra['unusable_reasons']}. Reference feed age p50 {ra['ref_feed_age_s_p50']} s, p99 {ra['ref_feed_age_s_p99']} s; time since the reference book last changed p50 {ra['ref_change_age_s_p50']} s.")
        L.append(f"- Median |Outcome mid - reference mid| across matched outcomes: {ra.get('median_mid_diff_c_overall')} cents. Outcomes whose bid-ask range sits apart from the reference's (median gap, cents): {ra.get('median_book_separation_c') or 'none'}. Suspect (> 5 cents apart): {ra['suspect_mappings'] or 'none'}.")
    else:
        L.append("- No aligned reference rows in this span.")
    pa = r.get("pm_audit") or {}
    if pa:
        L.append(f"- Polymarket REST audit: {pa.get('agree')} of {pa.get('compared')} snapshots matched the rebuilt stream book ({pa.get('skipped_unusable')} skipped while the stream was not usable).")
    L.append("")
    L.append("## Storage")
    L.append("")
    L.append("- Compressed bytes on disk by source (all data): " + ", ".join(f"{k} {v / 1e6:.2f} MB" for k, v in sorted(r["storage_bytes"].items())))
    L.append("- For the day(s) in this report: " + ", ".join(f"{k} {v / 1e6:.2f} MB" for k, v in sorted(r.get("storage_bytes_span_days", {}).items())) + f"; total {sum(r.get('storage_bytes_span_days', {}).values()) / 1e6:.1f} MB.")
    be = r.get("budget_events") or {}
    if any(be.values()):
        L.append(f"- Disk budget and space events: {be}.")
    tg = r.get("tiering")
    if tg:
        L.append(f"- Activity tiering: {tg['changes']} tier changes; {tg['outcomes_ever_fast']} outcomes were on the fast stream at some point, {tg['fast_outcome_hours']} outcome-hours in total.")
    L.append("")
    return "\n".join(L)


def _fmt(x: Any) -> str:
    if x is None:
        return "n/a"
    if isinstance(x, float):
        return f"{x:,.3f}" if abs(x) < 10 else f"{x:,.1f}"
    return str(x)


def write_report(root: Path, out_dir: Optional[Path] = None, window: Optional[Window] = None) -> Tuple[Path, Path, Dict[str, Any]]:
    r = build_report(root, window)
    out_dir = Path(out_dir or Path(root) / "reports")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    jp = out_dir / f"quality-{stamp}.json"
    mp = out_dir / f"quality-{stamp}.md"
    jp.write_text(json.dumps(r, indent=2, default=str))
    mp.write_text(render_markdown(r))
    return jp, mp, r
