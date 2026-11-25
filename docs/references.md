# Reference prices: matching, live feeds and the stale-reference guard

Outcome books are recorded at block resolution (~0.5 s). A reference price polled every
10 minutes can be minutes old, and comparing the two would turn staleness into fake
"price gaps". So the polled Polymarket and Kalshi feeds are kept only as broad context
(which markets exist), and every comparison uses a matched, live reference instead.

## Sources

| Venue | Interface | Auth | Used |
| --- | --- | --- | --- |
| Polymarket | CLOB market channel `wss://ws-subscriptions-clob.polymarket.com/ws/market` | none | yes, live, for matched markets |
| Polymarket | CLOB REST `GET /book?token_id=` | none | yes, audit every 60 s (rotating) |
| Polymarket | Gamma `public-search`, `events` | none | yes, matching only |
| Kalshi | WebSocket `orderbook_delta` | API key required, even for public data | **no**: needs a Kalshi API key, which is a private credential and needs the owner's approval |
| Kalshi | REST events | none | broad context poll only (10 min) |

Polymarket's market channel sends a full `book` snapshot per asset on subscribe, then
`price_change` level updates (with best bid/ask) and `last_trade_price`; every event
carries the exchange timestamp (ms). The client sends `PING` every 10 s and the server
answers `PONG`. The `custom_feature_enabled` flag is not used: in a live test it added a
stream of unrelated new-market announcements.

## Matching

`keel.refs.contracts` turns each Outcome sports outcome into what it pays on; then
`keel.refs.polymarket` searches Polymarket's Gamma API and scores candidates.

| Outcome template | Polymarket equivalent | Token |
| --- | --- | --- |
| `sportsContestResult` team outcome | "Will {team} win on {date}?" in the match event | YES |
| `sportsContestResult` draw | "Will {A} vs. {B} end in a draw?" | YES |
| `sportsContestWinner` (two-way, e.g. NBA) | moneyline market of the game event | the side-0 team's token |
| `sportsTournamentWinner` participant | "Will {team} win the {season} {competition}…?" in the champion event | YES |
| fallback outcomes and other templates | none | not matched, reason logged |

Confidence = 0.45 × name + 0.30 × time + 0.25 × rules, where:
- **name**: share of each team's identifying words found in the event title (diacritics,
  "FC"/"SK"-style words and known aliases handled); both short codes in the event slug
  (e.g. `nba-mia-phi-…`) count as full agreement;
- **time**: 1.0 if the start times agree within 15 minutes, 0.6 within 3 hours; otherwise
  rejected; for tournaments, whether the season appears;
- **rules**: 1.0 when the reference text states the same counted play (90 minutes plus
  stoppage for soccer; overtime included for NBA), 0.5 if not found or if tie handling may
  differ (Polymarket resolves an NFL tie 50-50; Outcome's fields do not state its tie rule,
  so NFL games map at `medium`), and rejected if the reference appears to count extra time.

Search: soccer by full club names; US two-way games by nickname ("76ers Heat"), because
full names surface season-long futures first; tournaments by Polymarket's competition
names ("EPL", "Pro Football"). These came from the first live run (2026-11-24), where
all two-way games and half the tournament outcomes went unmatched under the original
queries.

Derivative events ("– Exact Score", "– Player Props", "League Phase Winner", …) and closed
events are ignored. `high` ≥ 0.90, `medium` ≥ 0.75; below that, no mapping. Each mapping
is logged (`mapping_added/changed/removed`) with its components and written to
`data/meta/mappings.json`. Mappings are never `signed`: under the PRD, only George's
sign-off lets a mapping inform trading.

## Freshness: when a reference may be used

A reference price is **usable** at time t only if all hold:
1. a mapping existed at t at or above the minimum level;
2. the Polymarket connection was up and a full `book` snapshot for that asset had arrived
   **on the current connection** (a reconnect invalidates every book until its new
   snapshot);
3. the feed had been heard from (any message or `PONG`) within `max_reference_feed_age`
   (default 15 s);
4. both the Outcome and the reference books were two-sided.

Two ages are recorded and reported: **feed age** (time since the feed last proved it was
live) and **change age** (time since that book last changed). A quiet book is not stale
while the feed is live; a silent feed makes every book stale, however recently it changed.

Every row carries both exchange timestamps (Hyperliquid `time`, Polymarket `timestamp`),
local receipt time, both ages, and the mapping's confidence and level.

## The guard

`keel.refs.align.aligned_rows` joins each recorded Outcome book with the reference as of
that moment and marks it usable or not, with the reason. `gap_candidates` looks only at
usable rows and returns the excluded counts by reason, so every analysis states what it
left out. A test runs the recorder with a reference that goes silent mid-run and checks
that no candidate comes from the silent period, even though the frozen reference would
show a large "gap".

## Quality checks

- Fresh reference for ≥ 95% of matched Outcome book observations
- No suspect mappings: the median gap between the two bid-ask ranges ≤ 5¢ per mapping (0
  when they overlap). Mid differences are reported too, but a wide Outcome book (live
  example: 2.6¢ bid / 77.2¢ ask against Polymarket 3.7¢ / 3.8¢) has a meaningless mid
  and is not a wrong match; ranges that sit apart are. Reviewed, not failed.
- Polymarket REST audit agrees with the book rebuilt from the stream (≥ 90%)

The 7-day recording counts as research-grade for short-gap studies only if these pass
across the period, not just the infrastructure checks.
