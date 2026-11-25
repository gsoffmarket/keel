"""HyperCore-accurate replay simulator (PRD: Strategy research pipeline, FR-R3).

Replays recorded Outcome books and trades and lets a research strategy submit simulated
intents against them. It never contacts an exchange; it only reads a snapshot.

Mechanics (PRD "HyperCore-accurate simulator"):
- Block time is the clock: every recorded book and trade carries the exchange's block
  time (`exch_ms`); intents arrive after a latency drawn from a measured distribution
  and are processed against the book in force at arrival ("arrival-time book").
- Within one block, cancels and post-only (ALO) intents are processed before IOC and GTC
  intents and before the block's trades (HyperCore's ordering).
- One merged book per outcome: side-1 intents are converted to side 0 (buying side 1 at
  p is selling side 0 at 1 - p); positions are kept in side-0 shares, negative meaning
  side-1 tokens held.
- Prices must sit on the 0.00001 tick (100 px7 units); orders below the minimum
  notional are rejected (the true minimum is an open conformance question: $10 default).
- Fees are charged only on fills that reduce |position| and on settlement (opening is
  free); rates are parameters, not facts.
- Resting orders join the back of the displayed queue at their price; they fill only
  from recorded trades at that price after the queue ahead has traded, or from a trade
  through their price. A book that merely crosses them without a print does not fill
  them (pessimistic for makers: this simulator can only kill a strategy).
- Liquidity an IOC takes is removed from that book snapshot until the next snapshot of
  the coin arrives (impact does not persist beyond the next recorded book).
- A request ledger applies HyperCore's address budget (an initial allowance plus one
  request per USDC traded); every submit and cancel costs one request.
- Settlement uses the official `settledOutcome` fraction recorded for the instance.
- Rewards are not modelled and are reported as such.

Determinism: identical snapshot, strategy, parameters and seed give a byte-identical
event log; `SimResult.log_sha256` is the replay fingerprint.
"""
from __future__ import annotations

import hashlib
import heapq
from bisect import bisect_right
import json
import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

from keel.compact import iter_table, table_paths
from keel.research.universe import InstanceIndex, build_index

PX = 10**7
SZ = 10**6
TICK_PX7 = 100  # 0.00001 USDC
MS = 10**6  # ns per ms
PRIO = {"timer": -1, "cancel": 0, "alo": 1, "ioc": 2, "gtc": 2}


# ---------------------------------------------------------------- models

@dataclass
class LatencyModel:
    """One-way latency from decision to the exchange, lognormal from p50/p95, or drawn
    from measured samples when given."""

    p50_ms: float = 300.0
    p95_ms: float = 600.0
    samples_ms: Optional[List[float]] = None

    def draw(self, rng: random.Random) -> float:
        if self.samples_ms:
            return float(self.samples_ms[rng.randrange(len(self.samples_ms))])
        mu = math.log(self.p50_ms)
        sigma = max(1e-9, (math.log(self.p95_ms) - mu) / 1.6448536269514722)
        return math.exp(rng.gauss(mu, sigma))

    def describe(self) -> Dict[str, Any]:
        return {"samples": len(self.samples_ms)} if self.samples_ms else {"lognormal_p50_ms": self.p50_ms, "lognormal_p95_ms": self.p95_ms}


@dataclass
class FeeModel:
    maker_bps: float = 0.0
    taker_bps: float = 8.75
    settlement_bps: float = 0.0


@dataclass
class Ledger:
    """HyperCore request budget per address: `initial` plus one per USDC traded."""

    initial: int = 10_000
    used: int = 0
    traded_usdc: float = 0.0
    enabled: bool = True

    @property
    def remaining(self) -> int:
        return int(self.initial + self.traded_usdc) - self.used

    def spend(self) -> bool:
        if not self.enabled:
            self.used += 1
            return True
        if self.remaining <= 0:
            return False
        self.used += 1
        return True


@dataclass
class SimIntent:
    coin: str  # side-0 coin, e.g. "#4363900"
    is_buy: bool  # side-0 direction
    px7: int
    sz6: int
    tif: str = "ioc"  # ioc | gtc | alo
    cloid: str = ""

    @staticmethod
    def side1(coin0: str, buy_side1: bool, px7_side1: int, sz6: int, tif: str = "ioc", cloid: str = "") -> "SimIntent":
        """Express a side-1 intent in the merged side-0 book."""
        return SimIntent(coin0, not buy_side1, PX - px7_side1, sz6, tif, cloid)


@dataclass
class Fill:
    ns: int
    coin: str
    cloid: str
    is_buy: bool
    px7: int
    sz6: int
    liquidity: str  # taker | maker
    fee_usdc: float


@dataclass
class _Resting:
    intent: SimIntent
    remaining: int
    queue_ahead: int
    placed_ns: int


@dataclass
class SimResult:
    fills: List[Fill]
    log: List[str]
    positions: Dict[str, int]
    cash_usdc: float
    fees_usdc: float
    settled_usdc: float
    marked_usdc: float
    pnl_usdc: float
    requests: int
    rejects: Dict[str, int]
    log_sha256: str
    params: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------- the book

class BookState:
    def __init__(self) -> None:
        self.bids: List[Tuple[int, int]] = []  # (px7, sz6), best first
        self.asks: List[Tuple[int, int]] = []
        self.ns = 0
        self.taken: Dict[Tuple[str, int], int] = {}

    def load(self, levels_json: str, ns: int) -> None:
        lv = json.loads(levels_json)
        self.bids = [(int(round(float(l["px"]) * PX)), int(round(float(l["sz"]) * SZ))) for l in lv[0]]
        self.asks = [(int(round(float(l["px"]) * PX)), int(round(float(l["sz"]) * SZ))) for l in lv[1]]
        self.ns = ns
        self.taken = {}

    def best(self) -> Tuple[Optional[int], Optional[int]]:
        return (self.bids[0][0] if self.bids else None, self.asks[0][0] if self.asks else None)

    def displayed(self, is_bid: bool, px7: int) -> int:
        for p, s in (self.bids if is_bid else self.asks):
            if p == px7:
                return max(0, s - self.taken.get(("b" if is_bid else "a", p), 0))
        return 0


# ---------------------------------------------------------------- the simulator

class Sim:
    def __init__(
        self,
        snapshot,
        coins: Iterable[str],
        latency: Optional[LatencyModel] = None,
        fees: Optional[FeeModel] = None,
        ledger: Optional[Ledger] = None,
        seed: int = 0,
        min_notional_usdc: float = 10.0,
        windows: Optional[Dict[str, List[Tuple[int, int]]]] = None,
        index: Optional[InstanceIndex] = None,
    ) -> None:
        self.snapshot = snapshot
        self.coins = sorted(set(coins))
        self.latency = latency or LatencyModel()
        self.fees = fees or FeeModel()
        self.ledger = ledger or Ledger()
        self.rng = random.Random(seed)
        self.seed = seed
        self.min_notional = min_notional_usdc
        self.windows = windows
        self.idx = index or build_index(snapshot)
        self.books: Dict[str, BookState] = {c: BookState() for c in self.coins}
        self.now = 0
        self.pos: Dict[str, int] = {c: 0 for c in self.coins}
        self.cash = 0.0
        self.fee_total = 0.0
        self.settled = 0.0
        self.fills: List[Fill] = []
        self.log: List[str] = []
        self.rejects: Dict[str, int] = {}
        self.resting: Dict[str, _Resting] = {}
        self._pending: List[Tuple[int, int, int, str, Any]] = []  # (arrival_ns, prio, seq, kind, payload)
        self._seq = 0
        self._last_mid: Dict[str, float] = {}

    # ----- strategy API
    def schedule(self, ns: int, fn: Callable[["Sim"], None]) -> None:
        """Call `fn(sim)` at local decision time `ns` (e.g. when a signal was received)."""
        self._seq += 1
        heapq.heappush(self._pending, (int(ns), PRIO["timer"], self._seq, "timer", fn))

    def submit(self, intent: SimIntent) -> None:
        self._seq += 1
        if not intent.cloid:
            intent.cloid = f"c{self._seq}"
        arrive = self.now + int(self.latency.draw(self.rng) * MS)
        heapq.heappush(self._pending, (arrive, PRIO[intent.tif], self._seq, "submit", intent))
        self._log("submit", coin=intent.coin, cloid=intent.cloid, buy=intent.is_buy, px7=intent.px7, sz6=intent.sz6, tif=intent.tif, arrive_ns=arrive)

    def cancel(self, cloid: str) -> None:
        self._seq += 1
        arrive = self.now + int(self.latency.draw(self.rng) * MS)
        heapq.heappush(self._pending, (arrive, PRIO["cancel"], self._seq, "cancel", cloid))
        self._log("cancel_sent", cloid=cloid, arrive_ns=arrive)

    def book(self, coin: str) -> BookState:
        return self.books[coin]

    def position(self, coin: str) -> int:
        return self.pos.get(coin, 0)

    # ----- internals
    def _log(self, kind: str, **kw: Any) -> None:
        self.log.append(json.dumps({"ns": self.now, "kind": kind, **kw}, sort_keys=True, separators=(",", ":")))

    def _reject(self, why: str, intent: SimIntent) -> None:
        self.rejects[why] = self.rejects.get(why, 0) + 1
        self._log("reject", cloid=intent.cloid, why=why)

    def _fill(self, coin: str, cloid: str, is_buy: bool, px7: int, sz6: int, liquidity: str) -> None:
        q = sz6 if is_buy else -sz6
        p = self.pos[coin]
        reducing = p != 0 and (p > 0) != (q > 0)
        reduced = min(abs(q), abs(p)) if reducing else 0
        rate = (self.fees.taker_bps if liquidity == "taker" else self.fees.maker_bps) / 1e4
        fee = reduced / SZ * px7 / PX * rate
        notional = sz6 / SZ * px7 / PX
        self.cash += -notional if is_buy else notional
        self.fee_total += fee
        self.pos[coin] = p + q
        self.ledger.traded_usdc += notional
        f = Fill(self.now, coin, cloid, is_buy, px7, sz6, liquidity, round(fee, 10))
        self.fills.append(f)
        self._log("fill", coin=coin, cloid=cloid, buy=is_buy, px7=px7, sz6=sz6, liquidity=liquidity, fee=round(fee, 10), pos=self.pos[coin])

    def _cross(self, intent: SimIntent, remaining: int) -> int:
        """Take from the arrival-time book; returns the unfilled size."""
        bk = self.books[intent.coin]
        levels = bk.asks if intent.is_buy else bk.bids
        tag = "a" if intent.is_buy else "b"
        for px, sz in levels:
            if remaining <= 0:
                break
            if (intent.is_buy and px > intent.px7) or (not intent.is_buy and px < intent.px7):
                break
            avail = sz - bk.taken.get((tag, px), 0)
            if avail <= 0:
                continue
            take = min(avail, remaining)
            bk.taken[(tag, px)] = bk.taken.get((tag, px), 0) + take
            self._fill(intent.coin, intent.cloid, intent.is_buy, px, take, "taker")
            remaining -= take
        return remaining

    def _arrive(self, kind: str, payload: Any) -> None:
        if kind == "timer":
            payload(self)
            return
        if not self.ledger.spend():
            if kind == "submit":
                self._reject("request ledger exhausted", payload)
            else:
                self._log("reject", cloid=payload, why="request ledger exhausted")
                self.rejects["request ledger exhausted"] = self.rejects.get("request ledger exhausted", 0) + 1
            return
        if kind == "cancel":
            r = self.resting.pop(payload, None)
            self._log("cancelled" if r else "cancel_missed", cloid=payload)
            return
        it: SimIntent = payload
        if it.coin not in self.books:
            return self._reject("coin not in simulation", it)
        if it.px7 % TICK_PX7 or not (0 < it.px7 < PX):
            return self._reject("off tick or out of range", it)
        if it.sz6 / SZ * it.px7 / PX < self.min_notional:
            return self._reject("below minimum notional", it)
        inst = self.idx.at(it.coin, self.now)
        if inst is not None and inst.removed_ns and self.now >= inst.removed_ns:
            return self._reject("market closed", it)
        bk = self.books[it.coin]
        bb, ba = bk.best()
        crosses = (it.is_buy and ba is not None and it.px7 >= ba) or (not it.is_buy and bb is not None and it.px7 <= bb)
        if it.tif == "alo":
            if crosses:
                return self._reject("post-only would cross", it)
            return self._rest(it, it.sz6)
        left = self._cross(it, it.sz6) if crosses else it.sz6
        if it.tif == "ioc":
            if left:
                self._log("ioc_unfilled", cloid=it.cloid, sz6=left)
            return
        if left:
            self._rest(it, left)

    def _rest(self, it: SimIntent, size: int) -> None:
        ahead = self.books[it.coin].displayed(it.is_buy, it.px7)
        self.resting[it.cloid] = _Resting(it, size, ahead, self.now)
        self._log("resting", cloid=it.cloid, px7=it.px7, sz6=size, queue_ahead=ahead)

    def _on_trade(self, coin: str, px7: int, sz6: int, aggressor: str) -> None:
        # aggressor "A" sells into bids; "B" buys from asks
        for cloid in sorted(self.resting):
            r = self.resting.get(cloid)
            if r is None or r.intent.coin != coin:
                continue
            hit = (aggressor == "A" and r.intent.is_buy) or (aggressor == "B" and not r.intent.is_buy)
            if not hit:
                continue
            through = (r.intent.is_buy and px7 < r.intent.px7) or (not r.intent.is_buy and px7 > r.intent.px7)
            at = px7 == r.intent.px7
            if not (through or at):
                continue
            if through:
                q = r.remaining
            else:
                eat = min(r.queue_ahead, sz6)
                r.queue_ahead -= eat
                q = min(r.remaining, sz6 - eat)
            if q > 0:
                self._fill(coin, cloid, r.intent.is_buy, r.intent.px7, q, "maker")
                r.remaining -= q
                if r.remaining <= 0:
                    self.resting.pop(cloid, None)

    def _settle(self, coin: str, fraction: float) -> None:
        p = self.pos.get(coin, 0)
        for cloid in [c for c, r in self.resting.items() if r.intent.coin == coin]:
            self.resting.pop(cloid)
            self._log("expired", cloid=cloid, why="settlement")
        if p == 0:
            return
        value = p / SZ * fraction
        fee = abs(value) * self.fees.settlement_bps / 1e4
        self.cash += value - fee
        self.fee_total += fee
        self.settled += value - fee
        self.pos[coin] = 0
        self._log("settled", coin=coin, fraction=fraction, value=round(value, 10), fee=round(fee, 10))

    def _events(self) -> Iterator[Tuple[int, int, str, Any]]:
        coins = set(self.coins)

        merged: Dict[str, Tuple[List[int], List[int]]] = {}
        for c, ivs in (self.windows or {}).items():
            starts: List[int] = []
            ends: List[int] = []
            for a, b in sorted(ivs):
                if starts and a <= ends[-1]:
                    ends[-1] = max(ends[-1], b)
                else:
                    starts.append(a)
                    ends.append(b)
            merged[c] = (starts, ends)

        def inside(coin: str, ns: int) -> bool:
            if self.windows is None:
                return True
            m = merged.get(coin)
            if not m:
                return False
            i = bisect_right(m[0], ns) - 1
            return i >= 0 and ns <= m[1][i]

        def books() -> Iterator[Tuple[int, int, str, Any]]:
            for path in table_paths(self.snapshot.root, "books", self.snapshot.days):
                day = [r for r in iter_table(path, ["recv_ns", "exch_ms", "coin", "levels_json"]) if r["coin"] in coins and inside(r["coin"], int(r["exch_ms"]) * MS)]
                day.sort(key=lambda r: (int(r["exch_ms"]), r["coin"], int(r["recv_ns"])))
                for r in day:
                    yield int(r["exch_ms"]) * MS, 2, "book", r

        def trades() -> Iterator[Tuple[int, int, str, Any]]:
            seen = set()
            for path in table_paths(self.snapshot.root, "trades", self.snapshot.days):
                day = [r for r in iter_table(path, ["exch_ms", "coin", "tid", "px7", "sz6", "aggressor"]) if r["coin"] in coins and inside(r["coin"], int(r["exch_ms"]) * MS)]
                day.sort(key=lambda r: (int(r["exch_ms"]), r["coin"], int(r["tid"] or 0)))
                for r in day:
                    if (r["coin"], r["tid"]) in seen:
                        continue
                    seen.add((r["coin"], r["tid"]))
                    yield int(r["exch_ms"]) * MS, 1, "trade", r

        def settlements() -> Iterator[Tuple[int, int, str, Any]]:
            out = []
            for inst in self.idx.instances():
                if inst.coin in coins and inst.settle_fraction is not None and inst.removed_ns:
                    out.append((inst.removed_ns, 3, "settle", (inst.coin, inst.settle_fraction)))
            yield from sorted(out)

        # within a block: trades (1) before the next book snapshot (2); settlement last
        yield from heapq.merge(trades(), books(), settlements(), key=lambda x: (x[0], x[1]))

    def run(self, strategy: "Strategy") -> SimResult:
        strategy.start(self)
        for ns, prio, kind, payload in self._events():
            # intents that arrived before this block are processed first. In the block an
            # intent arrives in, cancels and ALO go before the block's own trades and
            # IOC/GTC after them (we assume others' takers in that block were ahead of us)
            while self._pending and (self._pending[0][0] < ns or (self._pending[0][0] == ns and self._pending[0][1] < PRIO["ioc"])):
                arrive, _, _, k, p = heapq.heappop(self._pending)
                self.now = arrive
                self._arrive(k, p)
            self.now = ns
            if kind == "trade":
                self._on_trade(payload["coin"], int(payload["px7"]), int(payload["sz6"]), payload["aggressor"])
                strategy.on_trade(self, payload["coin"], payload)
            elif kind == "book":
                bk = self.books[payload["coin"]]
                bk.load(payload["levels_json"], ns)
                bb, ba = bk.best()
                if bb is not None and ba is not None:
                    self._last_mid[payload["coin"]] = (bb + ba) / 2 / PX
                strategy.on_book(self, payload["coin"], bk)
            elif kind == "settle":
                self._settle(*payload)
        while self._pending:
            arrive, _, _, k, p = heapq.heappop(self._pending)
            self.now = arrive
            self._arrive(k, p)
        strategy.finish(self)
        marked = sum(p / SZ * self._last_mid.get(c, 0.0) for c, p in self.pos.items() if p)
        # settlement fees are already in cash; trading fees are not
        trading_fees = sum(f.fee_usdc for f in self.fills)
        pnl = self.cash + marked - trading_fees
        h = hashlib.sha256("\n".join(self.log).encode()).hexdigest()
        return SimResult(
            fills=self.fills, log=self.log, positions={c: p for c, p in self.pos.items() if p},
            cash_usdc=round(self.cash, 8), fees_usdc=round(self.fee_total, 8), settled_usdc=round(self.settled, 8),
            marked_usdc=round(marked, 8), pnl_usdc=round(pnl, 8), requests=self.ledger.used, rejects=dict(sorted(self.rejects.items())),
            log_sha256=h,
            params={"seed": self.seed, "latency": self.latency.describe(), "fees": vars(self.fees), "min_notional_usdc": self.min_notional, "ledger_initial": self.ledger.initial, "rewards": "not modelled"},
        )


class Strategy:
    """Research strategy interface for the simulator. Strategies see only what the
    recorder saw up to `sim.now` and act only through `sim.submit` / `sim.cancel`."""

    def start(self, sim: Sim) -> None:
        pass

    def on_book(self, sim: Sim, coin: str, book: BookState) -> None:
        pass

    def on_trade(self, sim: Sim, coin: str, trade: Dict[str, Any]) -> None:
        pass

    def finish(self, sim: Sim) -> None:
        pass
