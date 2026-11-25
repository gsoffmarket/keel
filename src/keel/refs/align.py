"""Join recorded Outcome books with the matched Polymarket reference, as of the same
moment, and mark every row usable or not.

This is the guard that keeps stale references out of research: anything that compares
Outcome with a reference must take its rows from `aligned_rows` and use only rows with
`usable=True`. `gap_candidates` refuses to look at anything else and reports how much
it excluded and why. A row is usable only if:
- the mapping existed at that moment and meets the minimum confidence level;
- the Polymarket feed was connected, had sent a full book snapshot on the current
  connection, and had been heard from (message or PONG) within `max_feed_age_s`;
- both books were two-sided.
"""
from __future__ import annotations

import heapq
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from keel.recorder.store import iter_events, iter_raw
from keel.refs.refbook import PMBooks
from keel.units import PX_SCALE, UnitError, px7, sz6


@dataclass
class Row:
    ns: int
    coin: str
    outcome_bid_px7: Optional[int]
    outcome_ask_px7: Optional[int]
    ref_bid_px7: Optional[int]
    ref_ask_px7: Optional[int]
    ref_feed_age_s: Optional[float]
    ref_change_age_s: Optional[float]
    ref_exch_ms: Optional[int]
    outcome_exch_ms: int
    confidence: float
    level: str
    usable: bool
    why: Optional[str]
    outcome_tier: str = "unknown"  # fast | standard at that moment (from tier_change events)
    outcome_bid_sz6: Optional[int] = None
    outcome_ask_sz6: Optional[int] = None


def _mapping_timeline(root: Path) -> Dict[str, List[Tuple[int, Optional[Dict[str, Any]]]]]:
    tl: Dict[str, List[Tuple[int, Optional[Dict[str, Any]]]]] = defaultdict(list)
    for e in iter_events(root):
        k = e["kind"]
        if k == "mapping_added":
            tl[e["coin"]].append((e["ns"], e))
        elif k == "mapping_changed":
            tl[e["after"]["coin"]].append((e["ns"], e["after"]))
        elif k == "mapping_removed":
            tl[e["coin"]].append((e["ns"], None))
    return tl


def _tier_timeline(root: Path) -> Dict[str, List[Tuple[int, str]]]:
    tl: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for e in iter_events(root):
        if e["kind"] == "tier_change":
            tl[e["coin"]].append((e["ns"], e["tier"]))
    return tl


def _active(tl: List[Tuple[int, Optional[Dict[str, Any]]]], ns: int) -> Optional[Dict[str, Any]]:
    cur = None
    for t, m in tl:
        if t > ns:
            break
        cur = m
    return cur


def _stream(root: Path, src: str, kind: str, days: Optional[List[str]] = None) -> Iterator[Tuple[int, str, Any]]:
    for day in (days or [None]):  # type: ignore[list-item]
        for r in iter_raw(root, src=src, day=day):
            yield int(r["recv_ns"]), kind, r["msg"]


def _pm_events(root: Path) -> Iterator[Tuple[int, str, Any]]:
    for e in iter_events(root):
        if e["kind"] in ("pm_connected", "pm_disconnected", "session_start", "session_end"):
            yield int(e["ns"]), e["kind"], None


def aligned_rows(root: Path, start_ns: Optional[int] = None, end_ns: Optional[int] = None, max_feed_age_s: float = 15.0, min_level: str = "medium", days: Optional[List[str]] = None) -> Iterator[Row]:
    """`days` limits which raw day folders are read (include the day before the window
    so a Polymarket book snapshot taken before midnight is known)."""
    root = Path(root)
    tl = _mapping_timeline(root)
    if not tl:
        return
    allowed = {"high"} if min_level == "high" else {"high", "medium"}
    tiers = _tier_timeline(root)
    books = PMBooks()
    events = sorted(_pm_events(root))
    merged = heapq.merge(events, _stream(root, "pm_ws", "pm", days), _stream(root, "hl_ws", "hl", days), key=lambda x: x[0])
    for ns, kind, payload in merged:
        if kind == "pm_connected":
            books.on_connect(ns)
            continue
        if kind in ("pm_disconnected", "session_start", "session_end"):
            books.on_disconnect(ns)
            continue
        if kind == "pm":
            books.apply_text(payload, ns)
            continue
        if not isinstance(payload, dict) or payload.get("channel") != "l2Book":
            continue
        if (start_ns and ns < start_ns) or (end_ns and ns >= end_ns):
            continue
        data = payload.get("data") or {}
        coin = data.get("coin", "")
        m = _active(tl.get(coin, []), ns)
        if m is None:
            continue
        lv = data.get("levels") or [[], []]
        try:
            ob = px7(lv[0][0]["px"]) if lv[0] else None
            oa = px7(lv[1][0]["px"]) if lv[1] else None
            obs = sz6(lv[0][0]["sz"]) if lv[0] else None
            oas = sz6(lv[1][0]["sz"]) if lv[1] else None
        except (UnitError, KeyError, IndexError):
            continue
        st = books.state(m["token_id"], ns, max_feed_age_s)
        why = st.get("why")
        if why is None and m.get("level") not in allowed:
            why = f"mapping level {m.get('level')} below {min_level}"
        if why is None and (ob is None or oa is None):
            why = "one-sided Outcome book"
        tier = "unknown"
        for t, v in tiers.get(coin, []):
            if t > ns:
                break
            tier = v
        yield Row(ns, coin, ob, oa, st.get("bid_px7"), st.get("ask_px7"), st.get("feed_age_s"), st.get("change_age_s"), st.get("exch_ms"), int(data.get("time") or 0), float(m.get("confidence") or 0), m.get("level") or "", why is None, why, tier, obs, oas)


def gap_candidates(rows: List[Row], min_gap_c: float) -> Tuple[List[Row], Dict[str, int]]:
    """Rows where Outcome is executable through the reference mid by at least
    `min_gap_c` cents. Only usable rows are considered; the rest are counted by reason
    so the exclusion is visible in every analysis."""
    out: List[Row] = []
    excluded: Counter = Counter()
    for r in rows:
        if not r.usable:
            excluded[r.why or "unusable"] += 1
            continue
        ref_mid2 = r.ref_bid_px7 + r.ref_ask_px7  # type: ignore[operator]
        thr = min_gap_c / 100 * PX_SCALE
        if 2 * r.outcome_ask_px7 + 2 * thr <= ref_mid2 or 2 * r.outcome_bid_px7 - 2 * thr >= ref_mid2:  # type: ignore[operator]
            out.append(r)
    return out, dict(excluded)


def summarize(root: Path, start_ns: Optional[int] = None, end_ns: Optional[int] = None, max_feed_age_s: float = 15.0) -> Dict[str, Any]:
    rows = list(aligned_rows(root, start_ns, end_ns, max_feed_age_s))
    if not rows:
        return {"rows": 0}
    usable = [r for r in rows if r.usable]
    why = Counter(r.why for r in rows if not r.usable)
    per_coin: Dict[str, List[float]] = defaultdict(list)
    sep_coin: Dict[str, List[float]] = defaultdict(list)
    for r in usable:
        om = (r.outcome_bid_px7 + r.outcome_ask_px7) / 2  # type: ignore[operator]
        rm = (r.ref_bid_px7 + r.ref_ask_px7) / 2  # type: ignore[operator]
        per_coin[r.coin].append(abs(om - rm) / PX_SCALE * 100)
        # separation of the two bid-ask intervals (0 when they overlap): a wide Outcome
        # book (e.g. 2.6c bid / 77c ask) has a meaningless mid but is not a wrong match
        sep = max(0, r.outcome_bid_px7 - r.ref_ask_px7, r.ref_bid_px7 - r.outcome_ask_px7)  # type: ignore[operator]
        sep_coin[r.coin].append(sep / PX_SCALE * 100)
    med = {c: sorted(v)[len(v) // 2] for c, v in per_coin.items()}
    med_sep = {c: sorted(v)[len(v) // 2] for c, v in sep_coin.items()}
    suspect = {c: round(v, 2) for c, v in med_sep.items() if v > 5.0}
    feed_ages = sorted(r.ref_feed_age_s for r in usable if r.ref_feed_age_s is not None)
    change_ages = sorted(r.ref_change_age_s for r in usable if r.ref_change_age_s is not None)
    minute_cover: Dict[Tuple[str, int], bool] = {}
    for r in rows:
        k = (r.coin, r.ns // (60 * 10**9))
        minute_cover[k] = minute_cover.get(k, False) or r.usable

    def pct(xs, q):
        return round(xs[min(len(xs) - 1, int(q * len(xs)))], 3) if xs else None

    return {
        "rows": len(rows),
        "rows_by_tier": dict(Counter(r.outcome_tier for r in rows)),
        "usable": len(usable),
        "usable_pct": round(100 * len(usable) / len(rows), 2),
        "unusable_reasons": dict(why),
        "mapped_coins_seen": len({r.coin for r in rows}),
        "coin_minutes": len(minute_cover),
        "coin_minutes_usable_pct": round(100 * sum(minute_cover.values()) / len(minute_cover), 2),
        "ref_feed_age_s_p50": pct(feed_ages, 0.5),
        "ref_feed_age_s_p99": pct(feed_ages, 0.99),
        "ref_change_age_s_p50": pct(change_ages, 0.5),
        "median_abs_mid_diff_c": {c: round(v, 3) for c, v in sorted(med.items())},
        "median_mid_diff_c_overall": round(sorted(med.values())[len(med) // 2], 3) if med else None,
        "median_book_separation_c": {c: round(v, 3) for c, v in sorted(med_sep.items()) if v > 0},
        "suspect_mappings": suspect,
    }


def pm_audit_agreement(root: Path, max_feed_age_s: float = 15.0) -> Dict[str, Any]:
    """Compare each REST book snapshot (`pm_rest`) with the book rebuilt from the
    market channel at that moment. Disagreement can be a genuine move between reads,
    so this is reviewed, not failed, below 90%."""
    root = Path(root)
    books = PMBooks()
    merged = heapq.merge(sorted(_pm_events(root)), _stream(root, "pm_ws", "pm"), _stream(root, "pm_rest", "rest"), key=lambda x: x[0])
    n = agree = skipped = 0
    for ns, kind, payload in merged:
        if kind == "pm_connected":
            books.on_connect(ns)
        elif kind in ("pm_disconnected", "session_start", "session_end"):
            books.on_disconnect(ns)
        elif kind == "pm":
            books.apply_text(payload, ns)
        elif kind == "rest" and isinstance(payload, dict):
            st = books.state(payload.get("asset_id", ""), ns, max_feed_age_s)
            try:
                rb = max((px7(l["price"]) for l in payload.get("bids") or []), default=None)
                ra = min((px7(l["price"]) for l in payload.get("asks") or []), default=None)
            except (UnitError, KeyError):
                skipped += 1
                continue
            if not st.get("usable"):
                skipped += 1
                continue
            n += 1
            agree += int((rb, ra) == (st.get("bid_px7"), st.get("ask_px7")))
    return {"compared": n, "agree": agree, "agree_pct": round(100 * agree / n, 1) if n else None, "skipped_unusable": skipped, "parse_errors": books.parse_errors}
