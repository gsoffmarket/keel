# Research engine (M1)

The research engine turns recorded data into evidence about whether a strategy can be tested at all, and kills strategies that cannot work, before any capital is involved. Everything here reads recorded data only. Nothing can place, sign or cancel an order; the simulator replays recorded books.

Four rules run through all of it:

- **Snapshots, not folders.** Every analysis runs on a snapshot: a set of complete, compacted UTC days whose table hashes and event log are pinned by a SHA-256 id. Reports carry the id and reproduce byte for byte.
- **Kill-only evidence.** The censuses and the simulator can reject a hypothesis; they can never promote one. The registry refuses a GO verdict on simulation, census or historical-trade evidence.
- **Every run is a trial.** Runs are counted from the moment they start, including failed ones. Hypotheses and runs are never deleted.
- **No rates from thin data.** Per-week rates and MDEs are not reported from less than one recorded day, and are labelled indicative until the PRD's seven days exist.

## Commands

| Command | What it does |
| --- | --- |
| `keel compact --all` | Builds the daily tables a snapshot needs, including `refs` (Outcome top of book joined with the matched Polymarket book as of the same moment, with the usability verdict). Run it again on days compacted before the `refs` table existed |
| `keel research census [--days D1,D2] [--venues out]` | S1a census: universe, liquidity, markout volatility, reference-gap episodes and the MDE table. Writes `reports/census/census-<snapshot>.md/.json` and the episode list |
| `keel research census --verify REPORT.json` | Rebuilds the report from its snapshot and compares the bytes. Exit 0 if identical |
| `keel research makers [--min-fills 50] [--taker-fee-bps 8.75]` | S1b maker census: P&L ex-rewards of the addresses that provide liquidity, by segment, with the kill rule |
| `keel research mde [--sigma 50] [--per-week 25,50,100,200] [--test-days 30,60,90] [--k 1]` | The minimum-detectable-edge table for any assumptions |
| `keel research register SPEC.json` | Registers a hypothesis. Refused if a field is missing or the plausible edge is below the MDE (refusals stay in the graveyard) |
| `keel research run NAME --evaluator E [--variant 0] [--seed 0] [--days ...]` | Runs an evaluator on a snapshot; records snapshot id, code version, seed and environment hash |
| `keel research verdict RUN_ID GO\|NO-GO\|REDESIGN\|INSUFFICIENT [--note]` | Records a verdict. GO on kill-only evidence is refused; NO-GO moves the hypothesis to REJECTED |
| `keel research validate [--days ...]` | Checks the engine against ground truth (below). Exit 0 only if every testable check passes |
| `keel research list` / `show NAME` | Hypotheses, stages, trial counts and verdicts |

The registry is a SQLite file at `<data>/research/registry.sqlite`. Database triggers make registered specs immutable and forbid deleting hypotheses or runs.

## S1a census

| Section | Measured as |
| --- | --- |
| Segments | `sports_match` (a contest and every outcome in it, including the fallback), `sports_future` (tournament and season winners), `price`, `other` |
| Clusters | One contest is one cluster; price markets on one underlying with one expiry are one cluster (strict); all underlyings with one expiry are one cluster for directional tests (O9 §3.1) |
| Instances | One listing of an outcome id with its description (ids are reused after settlement), from the recorded `outcomeMeta` polls |
| Clusters resolved per week | Distinct clusters with a removal or official settlement, divided by the hours the recorder actually ran |
| Liquidity | Per outcome, the last book of each minute: spread, and depth within ±4¢ of the mid on each side. "Two-sided ≥ $50" counts outcomes whose median min(bid depth, ask depth) is at least $50 |
| Markouts | For every trade, the mid at 10 s, 1 m, 5 m, 1 h, at the scheduled start and at settlement, minus the trade price, signed by the aggressor, in cents. Only horizons inside one recorder session count. σ is reported with at least 30 trades; ρ is the ANOVA intra-cluster correlation and the design effect is 1 + (m − 1)ρ |
| Reference-gap episodes | From usable `refs` rows only: an episode opens when Outcome is executable through the Polymarket mid by at least the threshold (0.3, 0.5, 1 and 2¢) and closes when the gap falls below it (recorded as who moved: Outcome or the reference), when the reference becomes unusable, or at the end of data. Duration, size at the price and the share lasting more than 0.5, 1, 2, 5 and 30 s are reported, because a gap shorter than Keel's latency cannot be taken |
| MDE table | Hold-to-resolution at σ = 50¢ (and with K = 3 pilots) and exit-before-resolution at the measured 1 m, 5 m and 1 h σ, for 30, 45, 60 and 90 days and 100%, 50% and 25% participation |

MDE = (z₀.₉₅ + z₀.₈₀) σ / √(n / 1.03), one-sided α = 0.05 (α/K for K concurrent pilots), power 0.80, 3% O'Brien–Fleming inflation. It reproduces O9's tables (12.2¢ at 25 clusters/week over 30 days, 3.5¢ at 100/week over 90 days; daily Sharpe 0.275 at 84 days).

## S1b maker census

For every address with at least 50 passive fills: cash flows of all its fills in the snapshot, plus its net position marked to the official settlement (if the outcome settled in the snapshot) or to the last mid (labelled "marked"), minus fees on position-reducing fills (assumed rates, printed in the report). Rewards are not observable and are excluded. Activity before the snapshot is unknown, so only P&L from activity inside it is counted.

Per segment, P&L is summed per cluster and a one-sided upper 95% bound is computed across clusters. **Kill rule (PRD S1b):** if the bound is below 0, no market-making hypothesis is registered in that segment. With fewer than 20 clusters the segment is "insufficient"; under seven recorded days the verdict is labelled indicative. A positive result promotes nothing: survivorship, hedges elsewhere and multiple addresses per firm are invisible.

## Simulator

`keel.research.sim.Sim` replays recorded Outcome books and trades for chosen outcomes and lets a `Strategy` act through `submit` and `cancel`. It implements the PRD's HyperCore rules:

- **Clock:** block time (`exch_ms`). Intents arrive after a latency drawn from measured samples or a lognormal fitted to p50/p95, and execute against the book in force at arrival.
- **Same-block order:** cancels and post-only intents go before the block's trades; IOC and GTC go after them.
- **Merged book:** side-1 intents become side-0 intents at 1 − p.
- **Validation:** the 0.00001 tick and a minimum notional ($10 by default, an open conformance question) are enforced.
- **Fees:** charged only on fills that reduce the position, and on settlement.
- **Resting orders:** they join the back of the displayed queue and fill only from recorded prints at their price after the queue ahead has traded, or from a print through their price.
- **Request ledger:** HyperCore's address budget is applied.
- **Settlement:** at the official fraction.
- **Rewards:** not modelled, and reported as such.

Liquidity an IOC takes stays removed until the next recorded book of that outcome. A book that merely crosses a resting order without a print does not fill it. Both choices are pessimistic, which suits a tool that may only kill.

The same snapshot, strategy, parameters and seed give a byte-identical event log. `SimResult.log_sha256` is the replay fingerprint, and the tests check it.

## Evaluators

| Evaluator | Evidence | What it measures |
| --- | --- | --- |
| `reference_gap_taker` | simulation | S1c screen for O-S7-L: on each reference-gap episode an IOC is sent through Outcome's book at no worse than the reference mid less the threshold, and closed with an IOC after `horizon_s` (or settled). Reports net edge per share after fees with a cluster-robust 95% interval, race-loss rate, positions stranded below the minimum notional, request count and the sim log hash. **Kill if the upper bound is below 0.** Configuration: `threshold_c`, `horizon_s`, `size_usdc`, `latency_p50_ms`, `latency_p95_ms`, `taker_fee_bps`, `exit_slippage_c`, `segments` |
| `markout` | census | Aggressor markout for one segment and horizon |

## Validation against ground truth

The unit tests show the code is consistent. `keel research validate` checks that its outputs are right, using facts the engine cannot manufacture:

| Check | Ground truth | Pass criterion |
| --- | --- | --- |
| 1. Simulator vs the exchange | Every real aggressive order in the snapshot (prints sharing a transaction hash) is replayed as a zero-latency IOC against the book recorded just before its block | At least 90% of orders that met a book under 1 s old fill the printed size at the printed average price, within one tick. Agreement is reported by book age |
| 2. Accounting conservation | Every trade has a buyer and a seller valued at the same mark | Sum of all addresses' P&L plus fees is zero |
| 3. Planted edge | 20 synthetic markets whose Outcome book follows the reference 10 s late after 2¢ jumps, and 20 that never follow | Edge world: mean net edge within 0.1¢ of jump − spread − fee, lower 95% bound above 0. No-edge world: upper 95% bound below 0 |
| 4. Mutations | Removing fees, adding 20 s latency, an impossible threshold | Edge rises without fees, the replay changes with latency, no episodes at 50¢ |
| 5. Independent recomputation | One-minute markouts rebuilt from the raw WebSocket messages with separate code | Trade count within 1% and mean within 5% of the census in every segment |

First run on real data (2026-11-23/24, 1.5 recorded hours): all five passed. Trades that happened while the recorder was off (8,907, replayed by the exchange when it reconnected) are excluded. Of the 10,297 real orders placed while it was recording, the simulator reproduced 89.6% exactly: 95.8% when the book was under 1 s old, 93.1% at 1–5 s, 85.5% at 5–15 s and 13.8% for the 94 orders that met an older book. Simulated fills are strongest evidence where the recorder had a fresh book, which is the fast stream. The planted 1.855¢ edge was recovered as 1.849¢ and the no-edge world measured −0.144¢ (spread plus fee). Accounting conserved to 0.000000 USDC across 525 addresses.

## A registration spec

```json
{
  "name": "O-S7-L",
  "mechanism": "Outcome sports books lag Polymarket after news; takers fast enough are paid by stale resting quotes",
  "universe_rule": "Outcome sports_match instances with a high or medium Polymarket mapping at decision time",
  "cluster_key": "question or match",
  "configs": [{"threshold_c": 0.5, "horizon_s": 60, "size_usdc": 25}],
  "primary_statistic": "net edge per share at the arrival-time book after fees",
  "alpha": 0.05, "power": 0.8,
  "guardrails": {"max_race_loss": 0.5},
  "live_loss_budget_usdc": 150, "calendar_cap_days": 90,
  "plausible_edge": 0.8, "sigma": 1.0,
  "clusters_per_week": 100, "participation": 0.5, "budget_days": 45
}
```

`sigma` and `clusters_per_week` must come from a census of at least seven recorded days; the MDE is computed from them at registration.
