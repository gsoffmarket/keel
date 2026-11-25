# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Recorder (M0).** Read-only recorder for Outcome (HIP-4) markets: `outcomeMeta`, side-0 books, trades with counterparties, mids, perp marks, REST book and mirror audits, official settlements via `settledOutcome`.
- Fast (per-block) books for Outcome sports and price markets with activity tiering, a per-market tier log and resolution provenance on every research row.
- Live Polymarket reference books for matched markets, with match confidence, exchange and receipt timestamps, feed and change age, and a freshness rule that keeps stale references out of research.
- Data-quality report, daily compaction to research tables with manifests and SHA-256 verification, retention that deletes only verified days, a disk budget, per-source storage accounting, stall and disk exit codes, external health pings, and the daily job.
- `keel health`: read-only Health page bound to loopback.
- **Research engine (M1).** Immutable snapshots; S1a census (universe, clusters, liquidity, markouts, reference-gap episodes, MDE table); S1b maker census with the kill rule; HyperCore replay simulator; research registry with registration rules, immutable specs, trial counting and kill-only evidence; the `reference_gap_taker` and `markout` evaluators.
- `keel research validate`: ground-truth checks (simulator against real fills, accounting conservation, planted-edge recovery, mutations, independent recomputation).
- CI on Python 3.9–3.13 with and without Parquet; deploy units for systemd and launchd.
