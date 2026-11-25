"""Local stand-ins for Hyperliquid's info API and WebSocket, fed with real recorded payloads."""
from __future__ import annotations

import asyncio
import copy
import json
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from keel.hl.ws import OP_CLOSE, OP_PING, OP_PONG, OP_TEXT, accept_key, encode_frame, read_frame

FIX = Path(__file__).parent / "fixtures"


def fixture(name: str) -> Any:
    return json.loads((FIX / name).read_text())


def book_for(coin: str, t_ms: int = 1795471189176) -> Dict[str, Any]:
    d = copy.deepcopy(fixture("ws_l2book_436390.json")["data"])
    d["coin"] = coin
    d["time"] = t_ms
    return d


def trades_for(coin: str, side: int = 0) -> List[Dict[str, Any]]:
    out = []
    for t in fixture("ws_trades_436390.json")["data"]:
        t = dict(t, coin=coin)
        if side == 1:
            from decimal import Decimal

            t["px"] = str(Decimal(1) - Decimal(t["px"]))
            t["side"] = "A" if t["side"] == "B" else "B"
            t["tid"] = t["tid"] + 1
        out.append(t)
    return out


class FakeInfo:
    """Threaded HTTP server answering POST /info like Hyperliquid (read-only types only)."""

    def __init__(self) -> None:
        self.meta = fixture("outcome_meta_2026-11-23.json")
        self.settled: Dict[int, str] = {}
        self.requests: List[Dict[str, Any]] = []
        self.fail_types: Set[str] = set()
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):  # silence
                pass

            def do_POST(self):
                n = int(self.headers.get("content-length", 0))
                body = json.loads(self.rfile.read(n))
                outer.requests.append(body)
                typ = body.get("type")
                if typ in outer.fail_types:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write(b"boom")
                    return
                if typ == "outcomeMeta":
                    resp: Any = outer.meta
                elif typ == "l2Book":
                    resp = book_for(body["coin"])
                elif typ == "recentTrades":
                    coin = body["coin"]
                    base = "#" + str(int(coin[1:]) // 10 * 10)
                    resp = trades_for(coin, side=int(coin[-1]))
                    for t in resp:
                        t["coin"] = coin
                elif typ == "settledOutcome":
                    oid = body["outcome"]
                    resp = None if oid not in outer.settled else {"spec": {"outcome": oid, "description": "fixture"}, "settleFraction": outer.settled[oid], "details": "template"}
                elif typ == "metaAndAssetCtxs":
                    resp = [{"universe": [{"name": "BTC"}]}, [{"markPx": "91234.0"}]]
                else:
                    self.send_response(422)
                    self.end_headers()
                    return
                data = json.dumps(resp).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}/info"

    def remove_outcome(self, oid: int, settle: Optional[str] = "1.0") -> None:
        if settle is not None:
            self.settled[oid] = settle
        self.meta = copy.deepcopy(self.meta)
        self.meta["outcomes"] = [o for o in self.meta["outcomes"] if o["outcome"] != oid]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *a):
        self.httpd.shutdown()
        self.httpd.server_close()


class FakeWS:
    """asyncio WebSocket server that behaves like Hyperliquid's for the subset Keel uses.

    - answers subscribe with subscriptionResponse, then a book snapshot / trade backfill
    - pushes a book update for every subscribed coin every `tick` seconds, and allMids
    - answers {"method":"ping"} with {"channel":"pong"}
    - `drop_after` closes each connection abruptly after N seconds (reconnect tests)
    """

    def __init__(self, tick: float = 0.05, drop_after: Optional[float] = None, mid: str = "0.318345") -> None:
        self.tick = tick
        self.drop_after = drop_after
        self.mid = mid
        self.server: Optional[asyncio.AbstractServer] = None
        self.connections = 0
        self.subscribe_msgs: List[Dict[str, Any]] = []
        self.t_ms = 1795471189176

    @property
    def url(self) -> str:
        port = self.server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        return f"ws://127.0.0.1:{port}/ws"

    async def start(self) -> "FakeWS":
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _send(self, w: asyncio.StreamWriter, obj: Any) -> None:
        w.write(encode_frame(OP_TEXT, json.dumps(obj, separators=(",", ":")).encode(), mask=False))
        await w.drain()

    async def _handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        self.connections += 1
        head = (await r.readuntil(b"\r\n\r\n")).decode()
        key = [l.split(":", 1)[1].strip() for l in head.split("\r\n") if l.lower().startswith("sec-websocket-key")][0]
        w.write(f"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept_key(key)}\r\n\r\n".encode())
        await w.drain()
        subs: List[Dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        t_end = loop.time() + self.drop_after if self.drop_after else None

        async def pusher():
            while True:
                await asyncio.sleep(self.tick)
                self.t_ms += 50
                for s in list(subs):
                    if s["type"] == "l2Book":
                        await self._send(w, {"channel": "l2Book", "data": book_for(s["coin"], self.t_ms)})
                    elif s["type"] == "allMids":
                        await self._send(w, {"channel": "allMids", "data": {"mids": {"#436390": self.mid, "BTC": "91234.5"}}})

        task = asyncio.ensure_future(pusher())
        try:
            while True:
                timeout = None if t_end is None else max(0.0, t_end - loop.time())
                try:
                    fin, op, payload = await asyncio.wait_for(read_frame(r, 1 << 20), timeout)
                except asyncio.TimeoutError:
                    w.transport.abort()  # abrupt drop, like a network failure
                    return
                if op == OP_CLOSE:
                    w.write(encode_frame(OP_CLOSE, payload[:2], mask=False))
                    await w.drain()
                    return
                if op == OP_PING:
                    w.write(encode_frame(OP_PONG, payload, mask=False))
                    continue
                if op != OP_TEXT:
                    continue
                msg = json.loads(payload)
                if msg.get("method") == "ping":
                    await self._send(w, {"channel": "pong"})
                elif msg.get("method") == "subscribe":
                    sub = msg["subscription"]
                    self.subscribe_msgs.append(sub)
                    subs.append(sub)
                    await self._send(w, {"channel": "subscriptionResponse", "data": msg})
                    if sub["type"] == "trades":
                        await self._send(w, {"channel": "trades", "data": trades_for(sub["coin"])})
                elif msg.get("method") == "unsubscribe":
                    sub = msg["subscription"]
                    if sub in subs:
                        subs.remove(sub)
        except (asyncio.IncompleteReadError, ConnectionError):
            return
        finally:
            task.cancel()
            try:
                w.close()
            except Exception:
                pass


class FakePM:
    """Polymarket market channel stand-in: book snapshot on subscribe, a price_change
    every `tick`, PONG for PING. `mute_after` makes it go silent (no data, no PONG)
    while keeping the TCP connection open, to exercise the stale-reference guard."""

    def __init__(self, bid: str = "0.31", ask: str = "0.33", tick: float = 0.2, mute_after: Optional[float] = None) -> None:
        self.bid, self.ask, self.tick, self.mute_after = bid, ask, tick, mute_after
        self.server: Optional[asyncio.AbstractServer] = None
        self.assets: List[str] = []
        self.connections = 0

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.server.sockets[0].getsockname()[1]}/ws/market"  # type: ignore[union-attr]

    async def start(self) -> "FakePM":
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self

    async def stop(self) -> None:
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, r, w) -> None:
        self.connections += 1
        head = (await r.readuntil(b"\r\n\r\n")).decode()
        key = [l.split(":", 1)[1].strip() for l in head.split("\r\n") if l.lower().startswith("sec-websocket-key")][0]
        w.write(f"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept_key(key)}\r\n\r\n".encode())
        await w.drain()
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        mine: List[str] = []

        def muted() -> bool:
            return self.mute_after is not None and loop.time() - t0 > self.mute_after

        async def send(obj) -> None:
            if muted():
                return
            w.write(encode_frame(OP_TEXT, (obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))).encode(), mask=False))
            await w.drain()

        async def ticker():
            while True:
                await asyncio.sleep(self.tick)
                for a in list(mine):
                    await send({"event_type": "price_change", "market": "0xm", "timestamp": str(int(loop.time() * 1000)), "price_changes": [{"asset_id": a, "price": self.bid, "size": "100", "side": "BUY", "best_bid": self.bid, "best_ask": self.ask}]})

        task = asyncio.ensure_future(ticker())
        try:
            while True:
                fin, op, payload = await read_frame(r, 1 << 20)
                if op == OP_CLOSE:
                    return
                if op != OP_TEXT:
                    continue
                text = payload.decode()
                if text == "PING":
                    await send("PONG")
                    continue
                msg = json.loads(text)
                ids = msg.get("assets_ids") or []
                if msg.get("operation") == "unsubscribe":
                    for a in ids:
                        if a in mine:
                            mine.remove(a)
                    continue
                mine.extend(ids)
                self.assets.extend(ids)
                await send([{"event_type": "book", "asset_id": a, "market": "0xm", "timestamp": "1", "bids": [{"price": self.bid, "size": "100"}], "asks": [{"price": self.ask, "size": "100"}]} for a in ids])
        except (asyncio.IncompleteReadError, ConnectionError):
            return
        finally:
            task.cancel()
            try:
                w.close()
            except Exception:
                pass


def gamma_event_for_fixture_meta() -> Dict[str, Any]:
    """A Polymarket event matching question 690 in the outcomeMeta fixture."""
    desc = "This market refers only to the outcome within the first 90 minutes of regular play plus stoppage time."

    def mk(q, toks):
        return {"question": q, "conditionId": "0x" + toks[0], "outcomes": '["Yes", "No"]', "clobTokenIds": json.dumps(toks), "sportsMarketType": "moneyline", "gameStartTime": "2026-11-25 20:00:00+00", "description": desc}

    return {"title": "Club Atlético de Madrid vs. Viking FK", "slug": "ucl-atm-vf-2026-11-25", "active": True, "closed": False,
            "markets": [mk("Will Club Atlético de Madrid win on 2026-11-25?", ["TATM", "NATM"]), mk("Will Club Atlético de Madrid vs. Viking FK end in a draw?", ["TDRAW", "NDRAW"]), mk("Will Viking FK win on 2026-11-25?", ["TVIK", "NVIK"])]}
