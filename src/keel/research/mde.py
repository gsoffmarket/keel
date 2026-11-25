"""Minimum detectable edge (PRD: registration rules; O9 §3.2; R2 §2).

One-sided test at level alpha (alpha/K with K concurrent pilots, Bonferroni), power
1 - beta, O'Brien-Fleming group-sequential sample inflation of 3%:

    MDE = (z[1 - alpha/K] + z[power]) * sigma / sqrt(n / 1.03)

n is the number of *independent* observations: resolution clusters for a
hold-to-resolution statistic, or fills divided by the design effect 1 + (m - 1) * rho
for a markout statistic with m fills per cluster and intra-cluster correlation rho. For
an inventory-carrying strategy the statistic is daily P&L per $, n is days and sigma is
1 (the MDE is a daily Sharpe ratio). These reproduce O9's published tables exactly.
"""
from __future__ import annotations

import math
from statistics import NormalDist
from typing import Any, Dict, Iterable, List

OBF_INFLATION = 1.03
_N = NormalDist()


def z(p: float) -> float:
    return _N.inv_cdf(p)


def z_sum(alpha: float = 0.05, power: float = 0.80, k: int = 1) -> float:
    return z(1 - alpha / k) + z(power)


def mde(sigma: float, n: float, alpha: float = 0.05, power: float = 0.80, k: int = 1, inflation: float = OBF_INFLATION) -> float:
    if n <= 0:
        return math.inf
    return z_sum(alpha, power, k) * sigma / math.sqrt(n / inflation)


def n_required(sigma: float, delta: float, alpha: float = 0.05, power: float = 0.80, k: int = 1, inflation: float = OBF_INFLATION) -> int:
    if delta <= 0:
        return math.inf  # type: ignore[return-value]
    return math.ceil(inflation * (z_sum(alpha, power, k) * sigma / delta) ** 2)


def design_effect(fills_per_cluster: float, rho: float) -> float:
    return 1.0 + max(0.0, fills_per_cluster - 1.0) * max(0.0, rho)


def clusters(per_week: float, days: float, participation: float = 1.0) -> float:
    return per_week * participation * days / 7.0


def mde_sharpe(days: float, alpha: float = 0.05, power: float = 0.80, k: int = 1) -> float:
    return mde(1.0, days, alpha, power, k)


def days_to_detect(sigma: float, delta: float, per_week: float, participation: float = 1.0, k: int = 1) -> float:
    """Calendar days of trading needed before an edge of `delta` is detectable."""
    rate = per_week * participation / 7.0
    if rate <= 0:
        return math.inf
    return n_required(sigma, delta, k=k) / rate


def hold_table(per_week: Iterable[float], days: Iterable[int], sigma: float = 50.0, k: int = 1) -> List[Dict[str, Any]]:
    """O9 §3.2 layout: MDE in cents per contract by clusters/week and test length."""
    out = []
    for w in per_week:
        row: Dict[str, Any] = {"clusters_per_week": w}
        for d in days:
            n = clusters(w, d)
            row[f"{d}d"] = {"n": int(n), "mde_c": round(mde(sigma, n, k=k), 2)}
        out.append(row)
    return out


def registration_check(plausible_edge: float, sigma: float, per_week: float, days: float, participation: float = 1.0, k: int = 1, design: float = 1.0, max_days: float = 180.0) -> Dict[str, Any]:
    """The rule `keel research register` applies: reject as untestable if the plausible
    edge is below the MDE for the budget, or if reaching it needs more than `max_days`."""
    n = clusters(per_week, days, participation) / max(1.0, design)
    m = mde(sigma, n, k=k)
    need = days_to_detect(sigma, plausible_edge, per_week, participation, k) * max(1.0, design)
    ok = plausible_edge >= m and need <= max_days
    reason = None
    if plausible_edge < m:
        reason = f"plausible edge {plausible_edge:g} is below the MDE {m:.3g} for {days:g} days at {per_week:g} clusters/week x {participation:g} participation"
    elif need > max_days:
        reason = f"needs {need:.0f} days to reach the edge, more than {max_days:g}"
    return {"testable": ok, "mde": m, "n_effective": n, "days_needed": need, "reason": reason}
