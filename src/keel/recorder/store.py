"""Append-only raw store.

Layout under the data root:
  raw/<src>/<YYYY-MM-DD>/<HH>Z-<session>.jsonl.gz   one line per received message
  meta/events/<YYYY-MM-DD>.jsonl                    recorder events (connects, gaps, universe changes)

Each raw line is
  {"seq":<n>,"recv_ns":<int>,"src":"<src>","req":<request or null>,"msg":<venue payload verbatim>}
The venue payload is inlined byte-for-byte (never re-serialised), so research always
sees exactly what arrived. Files are gzip members flushed with Z_SYNC_FLUSH every few
seconds; a crash loses at most the unflushed tail, and `iter_raw` reads truncated
files up to the last complete line. Each process start writes new files (the session
id is in the name), so a damaged file is never appended to.
"""
from __future__ import annotations

import gzip
import json
import os
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple


def utc_parts(ns: int) -> Tuple[str, str]:
    d = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
    return d.strftime("%Y-%m-%d"), d.strftime("%H")


def _is_json(text: str) -> bool:
    t = text.lstrip()
    if not t or t[0] not in "{[\"-0123456789tfn":
        return False
    try:
        json.loads(text)
        return True
    except ValueError:
        return False


class _Sink:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.raw = open(path, "ab")
        self.gz = gzip.GzipFile(fileobj=self.raw, mode="ab", compresslevel=6)
        self.lines = 0
        self.bytes_in = 0

    def write(self, data: bytes) -> None:
        self.gz.write(data)
        self.lines += 1
        self.bytes_in += len(data)

    def flush(self) -> None:
        self.gz.flush(zlib.Z_SYNC_FLUSH)
        self.raw.flush()
        os.fsync(self.raw.fileno())

    def close(self) -> None:
        self.gz.close()
        self.raw.close()


class RawStore:
    def __init__(self, root: Path, session: Optional[str] = None, flush_every: float = 2.0) -> None:
        self.root = Path(root)
        self.session = session or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}"
        self.flush_every = flush_every
        self.seq = 0
        self._sinks: Dict[Tuple[str, str, str], _Sink] = {}
        self._last_flush = time.monotonic()
        self._events_fh: Dict[str, Any] = {}
        self.counts: Dict[str, int] = {}

    def _sink(self, src: str, ns: int) -> _Sink:
        day, hour = utc_parts(ns)
        key = (src, day, hour)
        s = self._sinks.get(key)
        if s is None:
            # close sinks for the same source from older hours
            for k in [k for k in self._sinks if k[0] == src and k != key]:
                self._sinks.pop(k).close()
            s = _Sink(self.root / "raw" / src / day / f"{hour}Z-{self.session}.jsonl.gz")
            self._sinks[key] = s
        return s

    def write(self, src: str, msg_text: str, recv_ns: int, req: Optional[dict] = None) -> int:
        self.seq += 1
        if _is_json(msg_text):
            # Valid JSON can only hold a raw newline as whitespace (never inside a string),
            # so removing them keeps the payload's meaning and the one-record-per-line format.
            # Regression: Polymarket bodies end with "\n", which split records (2026-11-24).
            payload = msg_text.strip()
            if "\n" in payload or "\r" in payload:
                payload = payload.replace("\r", "").replace("\n", "")
        else:
            payload = json.dumps(msg_text)
        req_text = json.dumps(req, separators=(",", ":")) if req is not None else "null"
        line = f'{{"seq":{self.seq},"recv_ns":{recv_ns},"src":{json.dumps(src)},"req":{req_text},"msg":{payload}}}\n'
        self._sink(src, recv_ns).write(line.encode())
        self.counts[src] = self.counts.get(src, 0) + 1
        if time.monotonic() - self._last_flush >= self.flush_every:
            self.flush()
        return self.seq

    def event(self, kind: str, data: Optional[dict] = None, ns: Optional[int] = None) -> None:
        ns = ns or time.time_ns()
        day, _ = utc_parts(ns)
        fh = self._events_fh.get(day)
        if fh is None:
            for d in list(self._events_fh):
                self._events_fh.pop(d).close()
            p = self.root / "meta" / "events" / f"{day}.jsonl"
            p.parent.mkdir(parents=True, exist_ok=True)
            fh = open(p, "a", encoding="utf-8")
            self._events_fh[day] = fh
        fh.write(json.dumps({"ns": ns, "session": self.session, "kind": kind, **(data or {})}, separators=(",", ":"), default=str) + "\n")
        fh.flush()

    def flush(self) -> None:
        for s in self._sinks.values():
            s.flush()
        self._last_flush = time.monotonic()

    def close(self) -> None:
        for s in self._sinks.values():
            s.close()
        self._sinks.clear()
        for fh in self._events_fh.values():
            fh.close()
        self._events_fh.clear()


def _read_gz_tolerant(path: Path) -> Iterator[bytes]:
    """Yield complete lines from a (possibly truncated, possibly multi-member) gzip file."""
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    buf = b""
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            data = chunk
            while data:
                try:
                    out = d.decompress(data)
                except zlib.error:
                    out = b""
                    data = b""
                    break
                buf += out
                data = d.unused_data
                if data:  # next gzip member
                    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
            *lines, buf = buf.split(b"\n")
            for ln in lines:
                if ln:
                    yield ln
    # incomplete trailing line (crash mid-write) is dropped


def raw_files(root: Path, src: Optional[str] = None, day: Optional[str] = None) -> List[Path]:
    base = Path(root) / "raw"
    if not base.exists():
        return []
    pat = f"{src or '*'}/{day or '*'}/*.jsonl.gz"
    return sorted(base.glob(pat))


def iter_raw(root: Path, src: Optional[str] = None, day: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """Every raw record, file by file (each file is in arrival order)."""
    for p in raw_files(root, src, day):
        pending = b""
        for ln in _read_gz_tolerant(p):
            buf = pending + ln if pending else ln
            try:
                yield json.loads(buf)
                pending = b""
            except ValueError:
                # Files written before the newline fix can hold a record split across
                # lines; rejoin up to a bounded size, otherwise drop the fragment.
                pending = buf if buf.startswith(b'{"seq":') and len(buf) < (256 << 20) else b""


def iter_events(root: Path) -> Iterator[Dict[str, Any]]:
    base = Path(root) / "meta" / "events"
    if not base.exists():
        return
    for p in sorted(base.glob("*.jsonl")):
        with open(p, encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if ln:
                    try:
                        yield json.loads(ln)
                    except ValueError:
                        continue
