"""Parse `outcomeMeta` into typed records and track the universe over time.

Outcome ids are reused after settlement, so every listing is keyed by an
*instance id* = "<outcome>@<first_seen_ns>" (PRD, Data infrastructure).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

from keel.hl.coins import side_coin


def parse_description(text: str) -> Dict[str, str]:
    """Template description "k:v|k:v" -> dict. Splits on the first ':' only, since
    values may contain ':' (e.g. "perp:xyz:SPCX"). A segment with no ':' is kept
    under "_text"."""
    out: Dict[str, str] = {}
    if not text:
        return out
    for seg in text.split("|"):
        if ":" in seg:
            k, v = seg.split(":", 1)
            out[k.strip()] = v.strip()
        elif seg.strip():
            out.setdefault("_text", seg.strip())
    return out


def classify(name: str, fields: Dict[str, str]) -> str:
    """Coarse category used for reporting: sports | price | other."""
    n = (name or "").lower()
    if "sports" in n or "sport" in fields:
        return "sports"
    if "price" in n or fields.get("class", "").lower().startswith("price") or "perp" in fields or "underlying" in fields:
        return "price"
    return "other"


@dataclass(frozen=True)
class Outcome:
    outcome: int
    name: str
    description: str
    fields: Dict[str, str]
    side_names: Tuple[str, str]
    quote_token: str
    venue: str  # deployer short name ("out" = Outcome); "native" when the listing has none
    fee_scale: Optional[str]
    category: str
    question: Optional[int] = None
    role: str = "standalone"  # named | fallback | standalone

    @property
    def coin0(self) -> str:
        return side_coin(self.outcome, 0).coin

    @property
    def is_outcome_deployed(self) -> bool:
        return self.venue == "out"

    def fingerprint(self) -> Tuple[Any, ...]:
        """Fields whose change matters for research (description edits, fee scale, sides)."""
        return (self.name, self.description, self.side_names, self.quote_token, self.venue, self.fee_scale, self.question, self.role)


@dataclass(frozen=True)
class Question:
    question: int
    name: str
    description: str
    fields: Dict[str, str]
    fallback_outcome: Optional[int]
    named_outcomes: Tuple[int, ...]
    settled_named_outcomes: Tuple[int, ...]


@dataclass
class Meta:
    outcomes: Dict[int, Outcome]
    questions: Dict[int, Question]
    deployers: List[Dict[str, Any]]
    fee_scale: Optional[str]


def parse_outcome_meta(doc: Dict[str, Any]) -> Meta:
    questions: Dict[int, Question] = {}
    role: Dict[int, Tuple[int, str]] = {}
    for q in doc.get("questions", []) or []:
        qid = int(q["question"])
        fb = q.get("fallbackOutcome")
        named = tuple(int(x) for x in q.get("namedOutcomes", []) or [])
        settled = tuple(int(x) for x in q.get("settledNamedOutcomes", []) or [])
        questions[qid] = Question(
            question=qid,
            name=q.get("name", ""),
            description=q.get("description", ""),
            fields=parse_description(q.get("description", "")),
            fallback_outcome=int(fb) if fb is not None else None,
            named_outcomes=named,
            settled_named_outcomes=settled,
        )
        for o in named:
            role[o] = (qid, "named")
        if fb is not None:
            role[int(fb)] = (qid, "fallback")

    outcomes: Dict[int, Outcome] = {}
    for o in doc.get("outcomes", []) or []:
        oid = int(o["outcome"])
        fields = parse_description(o.get("description", ""))
        specs = o.get("sideSpecs") or [{"name": "Yes"}, {"name": "No"}]
        names = tuple(str(s.get("name", "")) for s in specs[:2])
        if len(names) < 2:
            names = (names + ("", ""))[:2]
        q = role.get(oid)
        outcomes[oid] = Outcome(
            outcome=oid,
            name=o.get("name", ""),
            description=o.get("description", ""),
            fields=fields,
            side_names=(names[0], names[1]),
            quote_token=o.get("quoteToken", ""),
            venue=o.get("venue") or "native",
            fee_scale=o.get("deployerFeeScale"),
            category=classify(o.get("name", ""), fields),
            question=q[0] if q else None,
            role=q[1] if q else "standalone",
        )
    return Meta(outcomes=outcomes, questions=questions, deployers=list(doc.get("deployers", []) or []), fee_scale=doc.get("feeScale"))


@dataclass
class Instance:
    instance_id: str
    outcome: Outcome
    first_seen_ns: int
    last_seen_ns: int
    removed_ns: Optional[int] = None


@dataclass
class UniverseChange:
    added: List[Instance] = field(default_factory=list)
    removed: List[Instance] = field(default_factory=list)
    changed: List[Tuple[Instance, Outcome]] = field(default_factory=list)  # (instance, previous)
    newly_settled_named: List[Tuple[int, int]] = field(default_factory=list)  # (question, outcome)

    @property
    def empty(self) -> bool:
        return not (self.added or self.removed or self.changed or self.newly_settled_named)


class Universe:
    """Live set of outcome instances, updated from successive `outcomeMeta` snapshots."""

    def __init__(self) -> None:
        self.live: Dict[int, Instance] = {}
        self.questions: Dict[int, Question] = {}
        self.history: List[Instance] = []

    def apply(self, meta: Meta, ts_ns: int) -> UniverseChange:
        ch = UniverseChange()
        for oid, o in meta.outcomes.items():
            inst = self.live.get(oid)
            if inst is None:
                inst = Instance(f"{oid}@{ts_ns}", o, ts_ns, ts_ns)
                self.live[oid] = inst
                self.history.append(inst)
                ch.added.append(inst)
            else:
                if inst.outcome.fingerprint() != o.fingerprint():
                    ch.changed.append((inst, inst.outcome))
                    inst.outcome = o
                inst.last_seen_ns = ts_ns
        for oid in [k for k in self.live if k not in meta.outcomes]:
            inst = self.live.pop(oid)
            inst.removed_ns = ts_ns
            ch.removed.append(inst)
        for qid, q in meta.questions.items():
            prev = self.questions.get(qid)
            before = set(prev.settled_named_outcomes) if prev else set()
            for o in q.settled_named_outcomes:
                if o not in before:
                    ch.newly_settled_named.append((qid, o))
        self.questions = dict(meta.questions)
        return ch

    def coins(self) -> List[str]:
        return sorted(i.outcome.coin0 for i in self.live.values())

    def by_coin(self, coin: str) -> Optional[Instance]:
        for i in self.live.values():
            if i.outcome.coin0 == coin:
                return i
        return None


def summarize(outcomes: Iterable[Outcome]) -> Dict[str, Dict[str, int]]:
    by_venue: Dict[str, int] = {}
    by_cat: Dict[str, int] = {}
    for o in outcomes:
        by_venue[o.venue] = by_venue.get(o.venue, 0) + 1
        by_cat[o.category] = by_cat.get(o.category, 0) + 1
    return {"by_venue": dict(sorted(by_venue.items())), "by_category": dict(sorted(by_cat.items()))}


def event_time_ns(o: Outcome, q: Optional[Question] = None) -> Optional[int]:
    """Scheduled start (sports) or expiry/settlement time (price markets) in UTC ns, from
    the template fields ("20261125-2000"), or None."""
    from datetime import datetime, timezone

    for fields in (o.fields, q.fields if q is not None else {}):
        for k in ("scheduledStart", "expiry", "time"):
            v = fields.get(k)
            if v:
                try:
                    d = datetime.strptime(v, "%Y%m%d-%H%M").replace(tzinfo=timezone.utc)
                    return int(d.timestamp()) * 10**9
                except ValueError:
                    continue
    return None
