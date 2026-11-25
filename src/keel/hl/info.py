"""Read-only client for Hyperliquid's public `POST /info` endpoint.

Standard library only. Requests are paced by a weight budget: Hyperliquid allows
1,200 weight per minute per IP for REST; `l2Book`/`allMids` weigh 2 and most other
info requests 20 (Hyperliquid docs, rate limits). Keel uses only a share of that
budget (default 50%) so the recorder never crowds out anything else on the IP.
"""
from __future__ import annotations

import asyncio
import json
import ssl
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

MAINNET_INFO = "https://api.hyperliquid.xyz/info"
IP_WEIGHT_PER_MIN = 1200

_LIGHT = {"l2Book", "allMids", "clearinghouseState", "orderStatus", "spotClearinghouseState", "exchangeStatus"}


def request_weight(body: Dict[str, Any]) -> int:
    return 2 if body.get("type") in _LIGHT else 20


class WeightBudget:
    """Token bucket over weight units, refilled continuously."""

    def __init__(self, per_minute: float, clock=time.monotonic) -> None:
        self.capacity = float(per_minute)
        self.tokens = float(per_minute)
        self.rate = per_minute / 60.0
        self.clock = clock
        self.last = clock()

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
        self.last = now

    def wait_time(self, weight: int) -> float:
        self._refill()
        if self.tokens >= weight:
            return 0.0
        return (weight - self.tokens) / self.rate

    def take(self, weight: int) -> None:
        self._refill()
        self.tokens -= weight


class InfoError(RuntimeError):
    pass


class InfoClient:
    def __init__(self, url: str = MAINNET_INFO, budget_share: float = 0.5, timeout: float = 10.0, user_agent: str = "keel-recorder/0.1") -> None:
        if not url.rstrip("/").endswith("/info"):
            # read-only by construction: this client can only ever reach an /info endpoint
            raise ValueError(f"InfoClient only talks to an /info endpoint, got {url!r}")
        if not 0 < budget_share <= 1:
            raise ValueError("budget_share must be in (0, 1]")
        self.url = url
        self.timeout = timeout
        self.budget = WeightBudget(IP_WEIGHT_PER_MIN * budget_share)
        self.user_agent = user_agent
        self._lock = asyncio.Lock()
        self._ssl = ssl.create_default_context()
        self.requests = 0
        self.errors = 0
        self.latency_ms: List[float] = []

    def _post_sync(self, body: Dict[str, Any]) -> Tuple[str, int]:
        data = json.dumps(body).encode()
        req = urllib.request.Request(self.url, data=data, method="POST", headers={"content-type": "application/json", "user-agent": self.user_agent})
        ctx = self._ssl if self.url.startswith("https") else None
        t0 = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=ctx) as resp:
                text = resp.read().decode()
        except urllib.error.HTTPError as exc:
            raise InfoError(f"HTTP {exc.code} for {body.get('type')}: {exc.read()[:200]!r}") from exc
        except (urllib.error.URLError, OSError) as exc:
            raise InfoError(f"network error for {body.get('type')}: {exc}") from exc
        return text, int((time.monotonic() - t0) * 1000)

    async def post_raw(self, body: Dict[str, Any]) -> Tuple[str, int]:
        """Returns (response text, latency ms). Paces itself against the weight budget."""
        w = request_weight(body)
        async with self._lock:
            delay = self.budget.wait_time(w)
            if delay > 0:
                await asyncio.sleep(delay)
            self.budget.take(w)
        self.requests += 1
        try:
            text, ms = await asyncio.get_running_loop().run_in_executor(None, self._post_sync, body)
            self.latency_ms.append(ms)
            del self.latency_ms[:-500]
            return text, ms
        except InfoError:
            self.errors += 1
            raise

    async def post(self, body: Dict[str, Any]) -> Any:
        text, _ = await self.post_raw(body)
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise InfoError(f"bad JSON for {body.get('type')}: {text[:200]!r}") from exc

    async def outcome_meta(self) -> Dict[str, Any]:
        return await self.post({"type": "outcomeMeta"})

    async def l2_book(self, coin: str) -> Dict[str, Any]:
        return await self.post({"type": "l2Book", "coin": coin})

    async def recent_trades(self, coin: str) -> Any:
        return await self.post({"type": "recentTrades", "coin": coin})
