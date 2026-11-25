"""Polymarket CLOB market channel (public, no authentication).

wss://ws-subscriptions-clob.polymarket.com/ws/market
- subscribe: {"assets_ids": [...], "type": "market"}; later {"assets_ids": [...], "operation": "subscribe"|"unsubscribe"}
- heartbeat: send text "PING" every 10 s; the server answers "PONG"
- on subscribe the server sends a `book` snapshot per asset (as a JSON array), then
  `price_change` (level updates with best bid/ask) and `last_trade_price`; every event
  carries the exchange `timestamp` in ms.
`custom_feature_enabled` is NOT set: it adds a firehose of unrelated new-market events
(observed live 2026-11-24).
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Callable, Dict, List, Optional, Set

from keel.hl.ws import ConnectionClosed, WebSocket

PM_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PMStream:
    def __init__(self, on_message: Callable[[str, int], None], on_event: Callable[[str, dict], None], url: str = PM_MARKET_WS,
                 ping_every: float = 10.0, idle_timeout: float = 30.0, batch: int = 100, connect=WebSocket.connect) -> None:
        self.on_message = on_message
        self.on_event = on_event
        self.url = url
        self.ping_every = ping_every
        self.idle_timeout = idle_timeout
        self.batch = batch
        self._connect = connect
        self.assets: Set[str] = set()
        self.ws: Optional[WebSocket] = None
        self.connected = asyncio.Event()
        self._stop = asyncio.Event()
        self.reconnects = 0
        self.last_msg_mono = 0.0
        self.last_pong_ns = 0
        self.connected_ns = 0
        self.pongs = 0
        self.bytes_in = 0
        self.ping_sent_mono = 0.0
        self.rtt_ms: List[float] = []

    async def set_assets(self, assets: Set[str]) -> None:
        add, rem = sorted(assets - self.assets), sorted(self.assets - assets)
        self.assets = set(assets)
        if self.ws and not self.ws.closed and self.connected.is_set():
            for i in range(0, len(add), self.batch):
                await self._send({"assets_ids": add[i:i + self.batch], "operation": "subscribe"})
            for i in range(0, len(rem), self.batch):
                await self._send({"assets_ids": rem[i:i + self.batch], "operation": "unsubscribe"})
        if add or rem:
            self.on_event("pm_assets", {"added": len(add), "removed": len(rem), "total": len(self.assets)})

    async def _send(self, obj: dict) -> None:
        try:
            await self.ws.send_text(json.dumps(obj))  # type: ignore[union-attr]
        except ConnectionClosed:
            pass

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            if not self.assets:
                await self._sleep(2.0)
                continue
            try:
                self.ws = await self._connect(self.url)
            except Exception as exc:
                self.on_event("pm_connect_failed", {"error": repr(exc)[:200], "backoff_s": backoff})
                await self._sleep(backoff * (1 + random.random() * 0.2))
                backoff = min(backoff * 2, 60.0)
                continue
            backoff = 1.0
            self.connected_ns = time.time_ns()
            self.last_msg_mono = time.monotonic()
            assets = sorted(self.assets)
            await self._send({"assets_ids": assets[: self.batch], "type": "market"})
            for i in range(self.batch, len(assets), self.batch):
                await self._send({"assets_ids": assets[i:i + self.batch], "operation": "subscribe"})
            self.connected.set()
            self.on_event("pm_connected", {"assets": len(assets), "reconnects": self.reconnects})
            tasks = [asyncio.ensure_future(self._reader()), asyncio.ensure_future(self._pinger()), asyncio.ensure_future(self._watchdog()), asyncio.ensure_future(self._stop.wait())]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            reason = "stop"
            for t in done:
                if t.exception() is not None:
                    reason = repr(t.exception())[:200]
                elif t is tasks[2]:
                    reason = "idle_timeout"
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self.connected.clear()
            await self.ws.close()
            self.on_event("pm_disconnected", {"reason": reason})
            if self._stop.is_set():
                break
            self.reconnects += 1
            await self._sleep(backoff)

    async def _sleep(self, s: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=s)
        except asyncio.TimeoutError:
            pass

    async def _reader(self) -> None:
        assert self.ws is not None
        while True:
            text = await self.ws.recv()
            now = time.time_ns()
            self.last_msg_mono = time.monotonic()
            self.bytes_in += len(text)
            if text == "PONG":
                self.pongs += 1
                self.last_pong_ns = now
                if self.ping_sent_mono:
                    self.rtt_ms.append((time.monotonic() - self.ping_sent_mono) * 1000)
                    del self.rtt_ms[:-500]
            # PONGs are recorded too: offline freshness checks need proof the feed was live
            self.on_message(text, now)

    async def _pinger(self) -> None:
        assert self.ws is not None
        while True:
            await asyncio.sleep(self.ping_every)
            self.ping_sent_mono = time.monotonic()
            await self.ws.send_text("PING")

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(min(5.0, self.idle_timeout / 3))
            if time.monotonic() - self.last_msg_mono > self.idle_timeout:
                return
