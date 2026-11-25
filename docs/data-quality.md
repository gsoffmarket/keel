# Data-quality report: definitions

`keel quality` reads only recorded data. Each metric below says exactly how it is computed, so a reader can reproduce it.

## Checks

| Check | Passes when | Why it matters |
| --- | --- | --- |
| Unmasked minutes ≥ 95% | See *Coverage* | PRD acceptance floor for the recorder |
| 7 consecutive days ≥ 95% | Span ≥ 168 h and the rate holds | M0 acceptance; shown as NOT YET before 7 days |
| Official settlement result for every removal | Every outcome that left `outcomeMeta` has an `outcome_settled` event from the documented `settledOutcome` request (PENDING while retries are still scheduled; `keel settlements` backfills) | The label research uses; the final-mid inference is only a cross-check |
| 100% settlement capture | Every outcome that left `outcomeMeta` had a WebSocket book within 10 minutes before removal was seen | Settlement-lag research (O-S8) needs the final minutes of every market |
| No crossed books | Best bid < best ask in every two-sided book message | A crossed book means a parsing or ordering bug |
| All prices on the tick | Every level price is a multiple of 0.00001 | Fixed-point integrity |
| REST audit agrees | ≥ 90% of REST `l2Book` snapshots equal the latest WebSocket top of book received before them | Confirms the stream is complete; disagreement can be genuine movement between the two reads, so this is REVIEW, not FAIL |
| Side-1 trades mirror side-0 | ≥ 99% of side-1 trades (within the side-0 response's time span) have a side-0 trade at the same time with prices summing to $1 | Justifies recording side 0 only |
| No parse failures | Zero records that fail fixed-point parsing | Data integrity |

| Every reference feed delivered data | Each configured broad feed returned at least once | Catches silent feed outages |
| Fast-stream outcomes have a stored book in ≥ 99% of minutes | Whole minutes inside each outcome's fast intervals (from `tier_change` events) that contain at least one stored book (changes or the 10 s heartbeat) | Completeness of the high-frequency data, not just process uptime |
| Fresh reference for ≥ 95% of matched observations | See references.md | Whether the data supports short-gap research |
| No suspect mappings | Median |Outcome mid − reference mid| ≤ 5¢ per mapping | Catches wrong matches |
| Polymarket REST audit agrees | ≥ 90% | Confirms the rebuilt reference book |

NOT YET means there is not enough data to judge, never PASS. The report also lists unmasked minutes per UTC day and recorder restarts, so a 7-day run is judged on data availability day by day.

## Coverage

- **Scored minutes**: whole UTC minutes from the first full minute after the first record to the last full minute before the last.
- **Unmasked minute**: the WebSocket was connected for the entire minute (from `ws_connected` / `ws_disconnected` / `session_*` events) and at least one WebSocket message arrived in it.
- **Masked spans**: runs of minutes that fail either test, merged.
- A process that dies without `session_end` is treated as disconnected from its last event until the next `session_start`.

## Order books

- **Book messages**: WebSocket `l2Book` pushes. Hyperliquid pushes a book periodically even when unchanged, so gaps between pushes per outcome measure stream health.
- **Update gap**: time between consecutive book messages for the same outcome; p50, p99, max across all outcomes.
- **Receive minus exchange time**: local receive time minus the book's `time` field. It includes local clock offset, so `keel doctor` reports the offset separately.

## Trades

- **Unique trades**: de-duplicated by (coin, trade id). Hyperliquid re-sends recent trades on every subscription, so reconnects create duplicates; they are counted and dropped.
- **Backfilled**: unique trades whose exchange time is before the first session start.
- **Distinct addresses / most active**: from each trade's `users` (buyer, seller). This is involvement, not P&L; the maker census (S1b) computes P&L.

## Settlements

When an outcome leaves `outcomeMeta`, the recorder logs `outcome_removed` with the last mid from `allMids`. The settlement label is:
- `final_mid_inferred` with 1.0 or 0.0 when the last mid is ≥ 0.99 or ≤ 0.01;
- `unknown_mid_not_extreme` or `unknown_no_mid` otherwise.

The recorder then asks the documented `settledOutcome` info request for the official result (`settleFraction`), retrying on the `settlement_checks` schedule, and logs `outcome_settled`. The report shows how often the final observed mid was on the winning side, as a cross-check. Named outcomes settling inside questions are logged separately (`question_named_settled`).

## First census

Per deployer and category, from the last recorded book of each live outcome:
- **Median spread (¢)**: best ask minus best bid.
- **Median depth ±1¢ (shares)**: shares on both sides within one cent of the mid.
- **Trades / notional**: unique trades in the span and their price × size.

This is one snapshot per outcome and is indicative only. The census in Milestone 2 uses time-weighted measures over at least 7 days.

## Resolution provenance

With activity tiering a market switches between the fast (~0.5 s) and standard (~12 s) streams. Every switch is a `tier_change` event, every aligned research row carries `outcome_tier`, and the report shows rows by tier, so a standard-resolution period is never analysed as if it were fast.
