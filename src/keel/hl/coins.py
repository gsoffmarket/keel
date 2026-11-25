"""HIP-4 outcome coin encoding.

For outcome `o` and side `s` (0 or 1): enc = 10*o + s.
- trading coin name:  "#<enc>"          e.g. outcome 43639 side 0 -> "#436390"
- asset id:           100_000_000 + enc
- balance token name: "+<enc>"

YES and NO share one mirrored book: side 1 prices are 1 - side 0 prices. Keel
subscribes to side 0 only; side 1 is derived. (Experimentally supported: recentTrades
on side 1 mirror side 0 trades at the same time with complementary price.)
"""
from __future__ import annotations

from dataclasses import dataclass

ASSET_OFFSET = 100_000_000


@dataclass(frozen=True)
class SideCoin:
    outcome: int
    side: int

    @property
    def enc(self) -> int:
        return 10 * self.outcome + self.side

    @property
    def coin(self) -> str:
        return f"#{self.enc}"

    @property
    def asset_id(self) -> int:
        return ASSET_OFFSET + self.enc

    @property
    def balance_token(self) -> str:
        return f"+{self.enc}"


def side_coin(outcome: int, side: int = 0) -> SideCoin:
    if side not in (0, 1):
        raise ValueError(f"side must be 0 or 1, got {side}")
    if outcome < 0:
        raise ValueError(f"outcome must be non-negative, got {outcome}")
    return SideCoin(outcome, side)


def parse_coin(coin: str) -> SideCoin:
    """"#436390" -> SideCoin(43639, 0). Raises ValueError for non-outcome coins."""
    if not coin or coin[0] not in "#+":
        raise ValueError(f"not an outcome coin: {coin!r}")
    body = coin[1:]
    if not body.isdigit():
        raise ValueError(f"not an outcome coin: {coin!r}")
    enc = int(body)
    side = enc % 10
    if side not in (0, 1):
        raise ValueError(f"bad side digit in {coin!r}")
    return SideCoin(enc // 10, side)


def is_outcome_coin(coin: str) -> bool:
    try:
        parse_coin(coin)
        return True
    except ValueError:
        return False
