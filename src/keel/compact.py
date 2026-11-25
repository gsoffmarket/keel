"""Daily compaction of raw records into research tables.

Output: <root>/tables/<table>/date=<YYYY-MM-DD>/part-0.<parquet|jsonl.gz> plus a
manifest with row counts, source files and SHA-256 of every output. Parquet is
written when `pyarrow` is installed (`pip install keel[parquet]`); otherwise the
same rows are written as gzip JSON lines so nothing depends on an optional package.

Tables
- books:   one row per WebSocket l2Book message (top of book + all levels as JSON)
- trades:  one row per unique trade id (subscription backfill duplicates removed)
- mids:    one row per outcome mid per allMids message
- meta:    one row per outcomeMeta poll (raw JSON)
- audits:  REST l2Book audit snapshots
- refs:    Outcome top of book joined with the matched Polymarket book as of the same
           moment, with the usability verdict (refs/align.py); only when mappings exist
Raw files are never modified or deleted by compaction.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from keel.recorder.store import iter_raw, raw_files
from keel.units import UnitError, px7, sz6

try:  # optional
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    HAVE_ARROW = True
except Exception:  # pragma: no cover - depends on environment
    HAVE_ARROW = False


def _book_row(recv_ns: int, d: Dict[str, Any]) -> Dict[str, Any]:
    lv = d.get("levels") or [[], []]
    bids, asks = lv[0], lv[1]
    return {
        "recv_ns": recv_ns,
        "exch_ms": int(d.get("time", 0)),
        "coin": d.get("coin"),
        "bid_px7": px7(bids[0]["px"]) if bids else None,
        "bid_sz6": sz6(bids[0]["sz"]) if bids else None,
        "ask_px7": px7(asks[0]["px"]) if asks else None,
        "ask_sz6": sz6(asks[0]["sz"]) if asks else None,
        "n_bid_levels": len(bids),
        "n_ask_levels": len(asks),
        "levels_json": json.dumps(lv, separators=(",", ":")),
    }


def build_tables(root: Path, day: str) -> Dict[str, List[Dict[str, Any]]]:
    t: Dict[str, List[Dict[str, Any]]] = {"books": [], "trades": [], "mids": [], "meta": [], "audits": [], "refs": []}
    seen_tids = set()
    for rec in iter_raw(root, day=day):
        ns = int(rec["recv_ns"])
        msg = rec["msg"]
        src = rec["src"]
        if src == "hl_ws" and isinstance(msg, dict):
            ch = msg.get("channel")
            if ch == "l2Book":
                try:
                    t["books"].append(_book_row(ns, msg["data"]))
                except (UnitError, KeyError, IndexError, TypeError):
                    continue
            elif ch == "trades":
                for tr in msg.get("data") or []:
                    tid = tr.get("tid")
                    if (tr.get("coin"), tid) in seen_tids:
                        continue
                    seen_tids.add((tr.get("coin"), tid))
                    users = tr.get("users") or [None, None]
                    t["trades"].append({
                        "recv_ns": ns, "exch_ms": int(tr.get("time", 0)), "coin": tr.get("coin"), "tid": tid,
                        "aggressor": tr.get("side"), "px7": px7(tr["px"]), "sz6": sz6(tr["sz"]), "hash": tr.get("hash"),
                        "buyer": users[0], "seller": users[1] if len(users) > 1 else None,
                    })
            elif ch == "allMids":
                for coin, v in ((msg.get("data") or {}).get("mids") or {}).items():
                    if coin.startswith("#"):
                        try:
                            t["mids"].append({"recv_ns": ns, "coin": coin, "mid_px7": px7(v)})
                        except UnitError:
                            # mids can carry half-ticks (e.g. 0.131885); keep exact at 1e-8 resolution
                            t["mids"].append({"recv_ns": ns, "coin": coin, "mid_px7": None, "mid_text": v})
        elif src == "hl_rest":
            req = rec.get("req") or {}
            if req.get("type") == "outcomeMeta":
                t["meta"].append({"recv_ns": ns, "json": json.dumps(msg, separators=(",", ":"))})
            elif req.get("type") == "l2Book" and isinstance(msg, dict) and "levels" in msg:
                try:
                    t["audits"].append(_book_row(ns, msg))
                except (UnitError, KeyError, IndexError, TypeError):
                    continue
    t["refs"] = _ref_rows(root, day)
    for name in ("books", "trades", "mids", "meta", "audits", "refs"):
        t[name].sort(key=lambda r: (r.get("coin") or "", r["recv_ns"]))
    return t


def _ref_rows(root: Path, day: str) -> List[Dict[str, Any]]:
    from datetime import timedelta

    from keel.recorder.store import iter_events
    from keel.refs.align import aligned_rows

    if not any(e.get("kind") == "mapping_added" for e in iter_events(root)):
        return []
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    lo = int(d0.timestamp()) * 10**9
    hi = lo + 86_400 * 10**9
    prev = (d0 - timedelta(days=1)).strftime("%Y-%m-%d")
    out = []
    for r in aligned_rows(root, lo, hi, days=[prev, day]):
        out.append({
            "recv_ns": r.ns, "coin": r.coin, "exch_ms": r.outcome_exch_ms,
            "bid_px7": r.outcome_bid_px7, "ask_px7": r.outcome_ask_px7, "bid_sz6": r.outcome_bid_sz6, "ask_sz6": r.outcome_ask_sz6,
            "ref_bid_px7": r.ref_bid_px7, "ref_ask_px7": r.ref_ask_px7, "ref_exch_ms": r.ref_exch_ms,
            "ref_feed_age_s": r.ref_feed_age_s, "ref_change_age_s": r.ref_change_age_s,
            "confidence": r.confidence, "level": r.level, "usable": r.usable, "why": r.why, "tier": r.outcome_tier,
        })
    return out


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write(rows: List[Dict[str, Any]], path_noext: Path, use_parquet: bool) -> Path:
    path_noext.parent.mkdir(parents=True, exist_ok=True)
    if use_parquet:
        p = path_noext.with_suffix(".parquet")
        cols: Dict[str, List[Any]] = {}
        keys: List[str] = []
        for r in rows:
            for k in r:
                if k not in cols:
                    cols[k] = []
                    keys.append(k)
        for r in rows:
            for k in keys:
                cols[k].append(r.get(k))
        pq.write_table(pa.table(cols) if rows else pa.table({}), p, compression="zstd")
        return p
    p = path_noext.with_suffix(".jsonl.gz")
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    return p


def compact_day(root: Path, day: str, force_jsonl: bool = False) -> Dict[str, Any]:
    root = Path(root)
    sources = [str(p.relative_to(root)) for p in raw_files(root, day=day)]
    if not sources:
        raise FileNotFoundError(f"no raw files for {day} under {root}/raw")
    tables = build_tables(root, day)
    use_parquet = HAVE_ARROW and not force_jsonl
    outputs = {}
    for name, rows in tables.items():
        p = _write(rows, root / "tables" / name / f"date={day}" / "part-0", use_parquet)
        outputs[name] = {"file": str(p.relative_to(root)), "rows": len(rows), "sha256": _sha256(p)}
    manifest = {
        "day": day,
        "created": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": "parquet" if use_parquet else "jsonl.gz",
        "sources": sources,
        "tables": outputs,
    }
    mp = root / "tables" / f"manifest-{day}.json"
    mp.write_text(json.dumps(manifest, indent=2))
    verify_day(root, day)
    return manifest


def read_table(path: Path) -> List[Dict[str, Any]]:
    if path.suffix == ".parquet":
        return pq.read_table(path).to_pylist()
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(l) for l in fh if l.strip()]


def iter_table(path: Path, columns: Optional[List[str]] = None, batch_rows: int = 65536) -> Iterable[Dict[str, Any]]:
    """Stream rows of one table file, optionally only some columns, without loading the
    file into memory (book tables carry full depth and can be large)."""
    path = Path(path)
    if path.suffix == ".parquet":
        pf = pq.ParquetFile(path)
        cols = [c for c in (columns or pf.schema_arrow.names) if c in pf.schema_arrow.names]
        for batch in pf.iter_batches(batch_size=batch_rows, columns=cols):
            yield from batch.to_pylist()
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = json.loads(line)
            yield {c: r.get(c) for c in columns} if columns else r


def table_paths(root: Path, table: str, days: Iterable[str]) -> List[Path]:
    """Files of `table` for the given days, from each day's manifest (verified tables only)."""
    root = Path(root)
    out = []
    for d in sorted(days):
        mp = root / "tables" / f"manifest-{d}.json"
        if not mp.exists():
            raise FileNotFoundError(f"no compacted tables for {d}: run keel compact --day {d}")
        info = json.loads(mp.read_text())["tables"].get(table)
        if info:
            out.append(root / info["file"])
    return out


def compacted_days(root: Path) -> List[str]:
    return sorted(p.name[len("manifest-"):-len(".json")] for p in (Path(root) / "tables").glob("manifest-*.json"))


def verify_day(root: Path, day: str) -> None:
    """Re-reads every output and checks row counts and hashes against the manifest."""
    root = Path(root)
    m = json.loads((root / "tables" / f"manifest-{day}.json").read_text())
    for name, info in m["tables"].items():
        p = root / info["file"]
        if _sha256(p) != info["sha256"]:
            raise ValueError(f"{name}: hash mismatch")
        n = len(read_table(p))
        if n != info["rows"]:
            raise ValueError(f"{name}: {n} rows read, manifest says {info['rows']}")


def days_with_raw(root: Path) -> List[str]:
    return sorted({p.parts[-2] for p in raw_files(root)})
