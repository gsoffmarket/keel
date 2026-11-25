"""Minimal RFC 6455 WebSocket client on asyncio (standard library only), plus a
Hyperliquid stream manager with subscriptions, app-level ping, idle watchdog and
reconnect with backoff.

Only what Hyperliquid needs is implemented: text frames, fragmentation, ping/pong,
close. No extensions are negotiated, so frames are never compressed.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import random
import ssl
import struct
import time
from typing import Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

MAINNET_WS = "wss://api.hyperliquid.xyz/ws"
_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA


class ConnectionClosed(Exception):
    def __init__(self, code: Optional[int] = None, reason: str = "") -> None:
        super().__init__(f"websocket closed code={code} reason={reason!r}")
        self.code = code
        self.reason = reason


class HandshakeError(Exception):
    pass


def accept_key(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()


def encode_frame(opcode: int, payload: bytes, mask: bool = True, fin: bool = True) -> bytes:
    head = bytearray([(0x80 if fin else 0) | opcode])
    n = len(payload)
    mbit = 0x80 if mask else 0
    if n < 126:
        head.append(mbit | n)
    elif n < 1 << 16:
        head.append(mbit | 126)
        head += struct.pack("!H", n)
    else:
        head.append(mbit | 127)
        head += struct.pack("!Q", n)
    if mask:
        key = os.urandom(4)
        head += key
        payload = bytes(b ^ key[i & 3] for i, b in enumerate(payload)) if n < 4096 else _mask_fast(payload, key)
    return bytes(head) + payload


def _mask_fast(payload: bytes, key: bytes) -> bytes:
    n = len(payload)
    k = int.from_bytes((key * ((n // 4) + 1))[:n], "big")
    return (int.from_bytes(payload, "big") ^ k).to_bytes(n, "big")


async def read_frame(reader: asyncio.StreamReader, max_size: int) -> Tuple[bool, int, bytes]:
    b0, b1 = await reader.readexactly(2)
    fin = bool(b0 & 0x80)
    opcode = b0 & 0x0F
    masked = bool(b1 & 0x80)
    n = b1 & 0x7F
    if n == 126:
        (n,) = struct.unpack("!H", await reader.readexactly(2))
    elif n == 127:
        (n,) = struct.unpack("!Q", await reader.readexactly(8))
    if n > max_size:
        raise ConnectionClosed(1009, f"frame of {n} bytes exceeds {max_size}")
    key = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(n) if n else b""
    if masked:
        payload = _mask_fast(payload, key)
    return fin, opcode, payload


class WebSocket:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, max_size: int = 32 << 20) -> None:
        self.reader = reader
        self.writer = writer
        self.max_size = max_size
        self.closed = False
        self._send_lock = asyncio.Lock()
        self.bytes_in = 0  # payload bytes received (for bandwidth accounting)

    @classmethod
    async def connect(cls, url: str, timeout: float = 15.0, headers: Optional[Dict[str, str]] = None) -> "WebSocket":
        u = urlparse(url)
        if u.scheme not in ("ws", "wss"):
            raise HandshakeError(f"unsupported scheme {u.scheme!r}")
        host = u.hostname or ""
        port = u.port or (443 if u.scheme == "wss" else 80)
        ctx = ssl.create_default_context() if u.scheme == "wss" else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ctx, server_hostname=host if ctx else None, limit=1 << 22), timeout
        )
        key = base64.b64encode(os.urandom(16)).decode()
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        host_hdr = host if (u.port is None) else f"{host}:{port}"
        lines = [
            f"GET {path} HTTP/1.1",
            f"Host: {host_hdr}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
            "User-Agent: keel-recorder/0.1",
        ]
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await writer.drain()
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout)
        head = raw.decode("latin-1").split("\r\n")
        if not head[0].startswith("HTTP/1.1 101"):
            writer.close()
            raise HandshakeError(f"upgrade refused: {head[0]!r}")
        hdrs = {}
        for line in head[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                hdrs[k.strip().lower()] = v.strip()
        if hdrs.get("sec-websocket-accept") != accept_key(key):
            writer.close()
            raise HandshakeError("bad Sec-WebSocket-Accept")
        return cls(reader, writer)

    async def _send(self, opcode: int, payload: bytes) -> None:
        if self.closed:
            raise ConnectionClosed(None, "send on closed socket")
        async with self._send_lock:
            self.writer.write(encode_frame(opcode, payload))
            await self.writer.drain()

    async def send_text(self, text: str) -> None:
        await self._send(OP_TEXT, text.encode())

    async def recv(self) -> str:
        """Next complete text message. Answers pings; raises ConnectionClosed on close."""
        parts: List[bytes] = []
        msg_op: Optional[int] = None
        while True:
            try:
                fin, op, payload = await read_frame(self.reader, self.max_size)
            except (asyncio.IncompleteReadError, ConnectionError) as exc:
                self.closed = True
                raise ConnectionClosed(1006, f"transport lost: {exc}") from exc
            self.bytes_in += len(payload) + 2
            if op == OP_PING:
                await self._send(OP_PONG, payload)
                continue
            if op == OP_PONG:
                continue
            if op == OP_CLOSE:
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None
                reason = payload[2:].decode(errors="replace")
                if not self.closed:
                    try:
                        await self._send(OP_CLOSE, payload[:2])
                    except Exception:
                        pass
                self.closed = True
                raise ConnectionClosed(code, reason)
            if op in (OP_TEXT, OP_BIN):
                msg_op = op
                parts = [payload]
            elif op == OP_CONT:
                if msg_op is None:
                    raise ConnectionClosed(1002, "continuation without start")
                parts.append(payload)
            else:
                raise ConnectionClosed(1002, f"unknown opcode {op}")
            if sum(len(p) for p in parts) > self.max_size:
                raise ConnectionClosed(1009, "message too big")
            if fin:
                data = b"".join(parts)
                return data.decode() if msg_op == OP_TEXT else data.decode(errors="replace")

    async def close(self, code: int = 1000) -> None:
        if not self.closed:
            try:
                await self._send(OP_CLOSE, struct.pack("!H", code))
            except Exception:
                pass
        self.closed = True
        try:
            self.writer.close()
        except Exception:
            pass


MessageHandler = Callable[[str, int], None]
EventHandler = Callable[[str, dict], None]


class HLStream:
    """Keeps a set of Hyperliquid subscriptions alive on one connection.

    - resubscribes everything after every reconnect
    - sends `{"method":"ping"}` every `ping_every` s (server closes idle sockets after 60 s)
    - reconnects if no message at all arrives for `idle_timeout` s
    - exponential backoff with jitter, capped
    - refuses to exceed `max_subscriptions` (Hyperliquid caps 1,000 per IP)
    Every received message is handed to `on_message(raw_text, recv_ns)` untouched.
    """

    def __init__(
        self,
        on_message: MessageHandler,
        on_event: EventHandler,
        url: str = MAINNET_WS,
        max_subscriptions: int = 900,
        ping_every: float = 30.0,
        idle_timeout: float = 45.0,
        subscribe_rate: float = 20.0,
        connect: Callable[[str], Awaitable[WebSocket]] = WebSocket.connect,
    ) -> None:
        self.on_message = on_message
        self.on_event = on_event
        self.url = url
        self.max_subscriptions = max_subscriptions
        self.ping_every = ping_every
        self.idle_timeout = idle_timeout
        self.subscribe_interval = 1.0 / subscribe_rate
        self._connect = connect
        self.subs: Dict[str, dict] = {}
        self.ws: Optional[WebSocket] = None
        self.connected = asyncio.Event()
        self._stop = asyncio.Event()
        self.reconnects = 0
        self.last_msg_mono = 0.0
        self.refused: List[str] = []
        self.ping_sent_mono = 0.0

    @staticmethod
    def key(sub: dict) -> str:
        return json.dumps(sub, sort_keys=True, separators=(",", ":"))

    async def subscribe(self, sub: dict) -> bool:
        k = self.key(sub)
        if k in self.subs:
            return True
        if len(self.subs) >= self.max_subscriptions:
            self.refused.append(k)
            self.on_event("subscription_refused", {"subscription": sub, "reason": f"cap {self.max_subscriptions}"})
            return False
        self.subs[k] = sub
        if self.ws and not self.ws.closed:
            await self._send_sub("subscribe", sub)
        return True

    async def unsubscribe(self, sub: dict) -> None:
        k = self.key(sub)
        if self.subs.pop(k, None) is not None and self.ws and not self.ws.closed:
            await self._send_sub("unsubscribe", sub)

    async def _send_sub(self, method: str, sub: dict) -> None:
        try:
            await self.ws.send_text(json.dumps({"method": method, "subscription": sub}))  # type: ignore[union-attr]
        except ConnectionClosed:
            pass
        await asyncio.sleep(self.subscribe_interval)

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            t_attempt = time.time_ns()
            try:
                self.ws = await self._connect(self.url)
            except Exception as exc:  # network down, DNS, TLS, refused
                self.on_event("connect_failed", {"error": repr(exc), "backoff_s": backoff})
                await self._sleep(backoff * (1 + random.random() * 0.2))
                backoff = min(backoff * 2, 60.0)
                continue
            self.on_event("connected", {"attempt_ns": t_attempt, "reconnects": self.reconnects, "subscriptions": len(self.subs)})
            self.connected.set()
            backoff = 1.0
            self.last_msg_mono = time.monotonic()
            tasks = [
                asyncio.ensure_future(self._reader()),
                asyncio.ensure_future(self._pinger()),
                asyncio.ensure_future(self._watchdog()),
                asyncio.ensure_future(self._resubscribe()),
                asyncio.ensure_future(self._stop.wait()),
            ]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            reason = "stop"
            for t in done:
                if t.exception() is not None:
                    reason = repr(t.exception())
                elif t is tasks[2]:
                    reason = "idle_timeout"
                elif t is tasks[0]:
                    reason = "reader_ended"
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            self.connected.clear()
            await self.ws.close()
            self.on_event("disconnected", {"reason": reason})
            if self._stop.is_set():
                break
            self.reconnects += 1
            await self._sleep(backoff)

    async def _sleep(self, s: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=s)
        except asyncio.TimeoutError:
            pass

    async def _resubscribe(self) -> None:
        for sub in list(self.subs.values()):
            await self._send_sub("subscribe", sub)
        await asyncio.Event().wait()  # keep running until cancelled

    async def _reader(self) -> None:
        assert self.ws is not None
        while True:
            text = await self.ws.recv()
            self.last_msg_mono = time.monotonic()
            self.on_message(text, time.time_ns())

    async def _pinger(self) -> None:
        assert self.ws is not None
        while True:
            await asyncio.sleep(self.ping_every)
            self.ping_sent_mono = time.monotonic()
            await self.ws.send_text('{"method":"ping"}')

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(min(5.0, self.idle_timeout / 3))
            if time.monotonic() - self.last_msg_mono > self.idle_timeout:
                return
