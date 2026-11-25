"""Outcome instances, segments and resolution clusters, reconstructed from the recorded
`outcomeMeta` polls (the `meta` table) and recorder events of a snapshot.

Unit of independence (O9 §3.1): one sports contest is one cluster whatever the number
of outcomes; all price markets on one underlying with the same expiry are one cluster
(and, for directional tests, all underlyings with one expiry count as about one).
"""
from __future__ import annotations

import json
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from keel.compact import iter_table, table_paths
from keel.hl.metadata import Outcome, Question, event_time_ns, parse_outcome_meta

THRESHOLD_KEYS = {"threshold", "target", "targetPrice", "marketCapThresholdB", "strike"}
SEGMENTS = ("sports_match", "sports_future", "price", "other")


def segment(o: Outcome, q: Optional[Question]) -> str:
    qf = q.fields if q else {}
    if o.category == "price":
        return "price"
    tmpl = (q.name if q else o.name).lower()
    if "tournament" in tmpl or ("season" in tmpl and "winner" in tmpl):
        return "sports_future"
    if o.category == "sports" or qf.get("sport") or qf.get("contestType"):
        ctype = (qf.get("contestType") or o.fields.get("contestType") or "").lower()
        if ctype and ctype != "match":
            return "sports_future"
        return "sports_match"
    if q and qf.get("sport"):
        return "sports_match"
    return "other"


def cluster_keys(o: Outcome, q: Optional[Question]) -> Tuple[str, str]:
    """(strict cluster, directional cluster). They differ only for price markets."""
    f = o.fields
    if q is not None:
        k = f"q{q.question}"
        return k, k
    if o.category == "price":
        und = f.get("underlying") or f.get("perp") or "?"
        exp = f.get("expiry") or f.get("time") or "?"
        return f"price|{und}|{exp}", f"price|{exp}"
    if f.get("participantA") and f.get("participantB"):
        a, b = sorted([f["participantA"], f["participantB"]])
        k = f"match|{f.get('competition', '')}|{a}|{b}|{f.get('scheduledStart', '')}"
        return k, k
    rest = {k: v for k, v in sorted(f.items()) if k not in THRESHOLD_KEYS}
    k = f"other|{o.name}|" + json.dumps(rest, sort_keys=True, separators=(",", ":"))
    return k, k


@dataclass
class Instance:
    key: str  # "<outcome>|<description>" (outcome ids are reused after settlement)
    outcome: int
    coin: str
    venue: str
    segment: str
    cluster: str
    cluster_dir: str
    name: str
    event_ns: Optional[int]
    first_seen_ns: int
    last_seen_ns: int
    removed_ns: Optional[int] = None
    settle_fraction: Optional[float] = None
    settled_ns: Optional[int] = None
    question_name: Optional[str] = None
    fields: Dict[str, str] = field(default_factory=dict)


class InstanceIndex:
    """All instances seen in a snapshot, with lookup by coin and time."""

    def __init__(self) -> None:
        self.by_key: Dict[str, Instance] = {}
        self._by_coin: Dict[str, List[Tuple[int, Instance]]] = {}
        self._times: Dict[str, List[int]] = {}
        self.polls = 0
        self.live_counts: Dict[str, List[int]] = {}

    def finalize(self) -> None:
        self._by_coin = {}
        for inst in self.by_key.values():
            self._by_coin.setdefault(inst.coin, []).append((inst.first_seen_ns, inst))
        for c, v in self._by_coin.items():
            v.sort(key=lambda x: (x[0], x[1].key))
            self._times[c] = [t for t, _ in v]

    def at(self, coin: str, ns: int) -> Optional[Instance]:
        """The instance trading as `coin` at time `ns` (the latest listed at or before it)."""
        v = self._by_coin.get(coin)
        if not v:
            return None
        i = bisect_right(self._times[coin], ns) - 1
        return v[i][1] if i >= 0 else v[0][1]

    def instances(self, venues: Optional[Iterable[str]] = None) -> List[Instance]:
        vs = set(venues) if venues else None
        return sorted((i for i in self.by_key.values() if vs is None or i.venue in vs), key=lambda i: (i.first_seen_ns, i.key))


def build_index(snapshot) -> InstanceIndex:
    idx = InstanceIndex()
    for path in table_paths(snapshot.root, "meta", snapshot.days):
        for row in iter_table(path, ["recv_ns", "json"]):
            try:
                meta = parse_outcome_meta(json.loads(row["json"]))
            except (ValueError, KeyError, TypeError):
                continue
            ns = int(row["recv_ns"])
            idx.polls += 1
            live: Dict[str, int] = {}
            for o in meta.outcomes.values():
                q = meta.questions.get(o.question) if o.question is not None else None
                key = f"{o.outcome}|{o.description}"
                inst = idx.by_key.get(key)
                if inst is None:
                    seg = segment(o, q)
                    strict, direc = cluster_keys(o, q)
                    inst = Instance(key, o.outcome, o.coin0, o.venue, seg, strict, direc, o.name, event_time_ns(o, q), ns, ns, question_name=q.name if q else None, fields=dict(o.fields))
                    idx.by_key[key] = inst
                inst.last_seen_ns = max(inst.last_seen_ns, ns)
                inst.first_seen_ns = min(inst.first_seen_ns, ns)
                live[f"{inst.venue}/{inst.segment}"] = live.get(f"{inst.venue}/{inst.segment}", 0) + 1
            for k, v in live.items():
                idx.live_counts.setdefault(k, []).append(v)
    idx.finalize()
    for e in snapshot.events():
        k = e.get("kind")
        if k not in ("outcome_removed", "outcome_settled"):
            continue
        inst = idx.at(e.get("coin", ""), int(e["ns"]) - 1)
        if inst is None:
            continue
        if k == "outcome_removed":
            inst.removed_ns = inst.removed_ns or int(e["ns"])
        else:
            inst.settle_fraction = float(e["settle_fraction"])
            inst.settled_ns = int(e["ns"])
    return idx
