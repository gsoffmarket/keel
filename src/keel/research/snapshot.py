"""Immutable research snapshots.

A snapshot pins exactly which data an analysis used: the compacted tables of a set of
complete UTC days (by their SHA-256 in each day's manifest) plus the recorder's event
log for those days (hashed after canonicalisation). The snapshot id is the SHA-256 of
that description, so the same id always means the same input bytes. Census reports and
research runs carry the id; `verify` recomputes it and fails if any input changed.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from keel.compact import compacted_days, verify_day

DAY_NS = 86_400 * 10**9


def _canon(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()


def day_bounds(day: str) -> tuple:
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    a = int(d0.timestamp()) * 10**9
    return a, a + DAY_NS


@dataclass
class Snapshot:
    root: Path
    days: List[str]
    tables: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # day -> {table: sha256}
    events_sha256: str = ""
    events_count: int = 0
    id: str = ""

    @property
    def start_ns(self) -> int:
        return day_bounds(self.days[0])[0]

    @property
    def end_ns(self) -> int:
        return day_bounds(self.days[-1])[1]

    def describe(self) -> Dict[str, Any]:
        return {"days": self.days, "tables": self.tables, "events_sha256": self.events_sha256, "events_count": self.events_count}

    def events(self) -> Iterator[Dict[str, Any]]:
        """Recorder events inside the snapshot's days, in file order."""
        yield from _events(self.root, self.days)


def _events(root: Path, days: List[str]) -> Iterator[Dict[str, Any]]:
    base = Path(root) / "meta" / "events"
    lo, hi = day_bounds(days[0])[0], day_bounds(days[-1])[1]
    # an event can be written to the previous day's file only if its timestamp says so,
    # so reading the day files plus one either side and filtering by ns is exact
    first = (datetime.strptime(days[0], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    last = (datetime.strptime(days[-1], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    for p in sorted(base.glob("*.jsonl")):
        if not (first <= p.stem <= last):
            continue
        with open(p, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    e = json.loads(ln)
                except ValueError:
                    continue
                if lo <= int(e.get("ns", 0)) < hi:
                    yield e


def build(root: Path, days: Optional[List[str]] = None, verify_tables: bool = True) -> Snapshot:
    root = Path(root)
    avail = compacted_days(root)
    days = sorted(days or avail)
    if not days:
        raise FileNotFoundError(f"no compacted days under {root}/tables: run keel compact first")
    missing = [d for d in days if d not in avail]
    if missing:
        raise FileNotFoundError(f"days without compacted tables: {missing}")
    snap = Snapshot(root=root, days=days)
    for d in days:
        if verify_tables:
            verify_day(root, d)
        m = json.loads((root / "tables" / f"manifest-{d}.json").read_text())
        snap.tables[d] = {name: info["sha256"] for name, info in sorted(m["tables"].items())}
    h = hashlib.sha256()
    n = 0
    for e in _events(root, days):
        h.update(_canon(e))
        h.update(b"\n")
        n += 1
    snap.events_sha256, snap.events_count = h.hexdigest(), n
    snap.id = hashlib.sha256(_canon(snap.describe())).hexdigest()
    return snap


def verify(root: Path, expected: Dict[str, Any]) -> Snapshot:
    """Rebuild the snapshot described by `expected` (a report's `snapshot` block) and
    fail if any input differs."""
    snap = build(root, expected["days"])
    if snap.id != expected["id"]:
        diff = [d for d in expected["days"] if expected["tables"].get(d) != snap.tables.get(d)]
        raise ValueError(f"snapshot {expected['id'][:12]} no longer reproduces: tables changed on {diff or 'none'}, events {'changed' if snap.events_sha256 != expected['events_sha256'] else 'same'}")
    return snap
