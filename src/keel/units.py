"""Fixed-point units and duration parsing.

Money, prices and sizes are never floats inside Keel (PRD, Interfaces: Conventions):
- prices in 1e-7 of $1 (`_px7`); the HIP-4 tick of 0.00001 is 100 px7
- sizes in 1e-6 shares (`_sz6`)
- money in micro-USDC (`_uusdc`)
- times in UTC nanoseconds (`_ns`)
"""
from __future__ import annotations

import re
import time
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

PX_SCALE = 10_000_000
SZ_SCALE = 1_000_000
USDC_SCALE = 1_000_000
TICK_PX7 = 100  # 0.00001


class UnitError(ValueError):
    pass


def _to_scaled(text: str | int | Decimal, scale: int, what: str) -> int:
    try:
        d = Decimal(str(text))
    except InvalidOperation as exc:
        raise UnitError(f"not a number for {what}: {text!r}") from exc
    if not d.is_finite():
        raise UnitError(f"non-finite {what}: {text!r}")
    scaled = d * scale
    q = scaled.to_integral_value(rounding=ROUND_HALF_EVEN)
    if q != scaled:
        raise UnitError(f"{what} {text!r} has more precision than 1/{scale}")
    return int(q)


def px7(text: str | int | Decimal) -> int:
    """Parse a venue price string ("0.31834") into 1e-7 units, exactly."""
    return _to_scaled(text, PX_SCALE, "price")


def sz6(text: str | int | Decimal) -> int:
    return _to_scaled(text, SZ_SCALE, "size")


def uusdc(text: str | int | Decimal) -> int:
    return _to_scaled(text, USDC_SCALE, "amount")


def px7_to_cents(p: int) -> float:
    """For display and reports only."""
    return p / PX_SCALE * 100.0


def now_ns() -> int:
    return time.time_ns()


_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)\s*$")
_FACTOR = {"ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def parse_duration(text: str | int | float) -> float:
    """"60s" -> 60.0 seconds. Bare numbers are rejected so units are always explicit."""
    if isinstance(text, (int, float)) and not isinstance(text, bool):
        raise UnitError(f"duration needs a unit, got bare number {text!r} (write e.g. \"{text}s\")")
    m = _DURATION.match(str(text))
    if not m:
        raise UnitError(f"bad duration {text!r}; use e.g. 500ms, 60s, 5m, 1h, 7d")
    return float(m.group(1)) * _FACTOR[m.group(2)]
