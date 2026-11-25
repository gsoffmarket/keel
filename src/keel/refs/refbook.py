"""Polymarket order-book state rebuilt from market-channel messages, with the
information needed to judge freshness.

A reference book is *valid* only after a full `book` snapshot received on the current
connection; a reconnect invalidates every book until its new snapshot arrives. A
quiet book (no changes for minutes) is still current as long as the connection is
proven live (any message or PONG recently): "age of the last change" and "age of the
feed" are different things and both are reported.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

from keel.units import UnitError, px7


@dataclass
class AssetBook:
    bids: Dict[int, str] = field(default_factory=dict)  # px7 -> size text
    asks: Dict[int, str] = field(default_factory=dict)
    snapshot_epoch: int = -1
    last_change_ns: int = 0
    last_exch_ms: int = 0
    last_trade_px7: Optional[int] = None
    best_field_mismatch: int = 0

    def best(self) -> Tuple[Optional[int], Optional[int]]:
        return (max(self.bids) if self.bids else None, min(self.asks) if self.asks else None)


class PMBooks:
    def __init__(self) -> None:
        self.books: Dict[str, AssetBook] = {}
        self.epoch = 0
        self.connected = False
        self.last_any_ns = 0
        self.parse_errors = 0

    # connection lifecycle (from recorder events)
    def on_connect(self, ns: int) -> None:
        self.epoch += 1
        self.connected = True
        self.last_any_ns = ns

    def on_disconnect(self, ns: int) -> None:
        self.connected = False

    def apply_text(self, text_or_obj: Any, recv_ns: int) -> None:
        self.last_any_ns = recv_ns
        obj = text_or_obj
        if isinstance(obj, str):
            if obj == "PONG":
                return
            try:
                obj = json.loads(obj)
            except ValueError:
                self.parse_errors += 1
                return
        if isinstance(obj, list):
            for ev in obj:
                self._apply(ev, recv_ns)
        elif isinstance(obj, dict):
            self._apply(obj, recv_ns)

    def _apply(self, ev: Dict[str, Any], recv_ns: int) -> None:
        try:
            et = ev.get("event_type") or ("book" if "bids" in ev and "asks" in ev else ("price_change" if "price_changes" in ev else None))
            ts = int(ev.get("timestamp") or 0)
            if et == "book":
                b = self.books.setdefault(ev["asset_id"], AssetBook())
                b.bids = {px7(l["price"]): l["size"] for l in ev.get("bids") or [] if float(l["size"]) > 0}
                b.asks = {px7(l["price"]): l["size"] for l in ev.get("asks") or [] if float(l["size"]) > 0}
                b.snapshot_epoch = self.epoch
                b.last_change_ns = recv_ns
                b.last_exch_ms = ts
            elif et == "price_change":
                for ch in ev.get("price_changes") or []:
                    b = self.books.get(ch.get("asset_id"))
                    if b is None:
                        continue  # no snapshot yet: cannot apply a delta
                    side = b.bids if ch.get("side") == "BUY" else b.asks
                    p = px7(ch["price"])
                    if float(ch.get("size") or 0) <= 0:
                        side.pop(p, None)
                    else:
                        side[p] = ch["size"]
                    b.last_change_ns = recv_ns
                    b.last_exch_ms = ts or b.last_exch_ms
                    if ch.get("best_bid") is not None and ch.get("best_ask") is not None:
                        bb, ba = b.best()
                        try:
                            if (bb, ba) != (px7(ch["best_bid"]), px7(ch["best_ask"])):
                                b.best_field_mismatch += 1
                        except UnitError:
                            pass
            elif et == "last_trade_price":
                b = self.books.get(ev.get("asset_id"))
                if b is not None:
                    b.last_trade_px7 = px7(ev["price"])
        except (KeyError, UnitError, TypeError, ValueError):
            self.parse_errors += 1

    def state(self, asset: str, now_ns: int, max_feed_age_s: float = 15.0) -> Dict[str, Any]:
        """Best bid/ask plus everything needed to decide whether it may be used."""
        b = self.books.get(asset)
        feed_age_s = (now_ns - self.last_any_ns) / 1e9 if self.last_any_ns else None
        if b is None:
            return {"usable": False, "why": "no snapshot", "feed_age_s": feed_age_s}
        bb, ba = b.best()
        valid = b.snapshot_epoch == self.epoch and self.connected
        why = None
        if not self.connected:
            why = "feed disconnected"
        elif b.snapshot_epoch != self.epoch:
            why = "no snapshot since reconnect"
        elif feed_age_s is None or feed_age_s > max_feed_age_s:
            why = f"feed silent {feed_age_s:.1f} s" if feed_age_s is not None else "feed never live"
        elif bb is None or ba is None:
            why = "one-sided reference book"
        return {
            "usable": why is None,
            "why": why,
            "bid_px7": bb,
            "ask_px7": ba,
            "feed_age_s": feed_age_s,
            "change_age_s": (now_ns - b.last_change_ns) / 1e9 if b.last_change_ns else None,
            "exch_ms": b.last_exch_ms,
            "valid_snapshot": valid,
        }
