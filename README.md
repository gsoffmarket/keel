# Keel

[![CI](https://github.com/gsoffmarket/keel/actions/workflows/ci.yml/badge.svg)](https://github.com/gsoffmarket/keel/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)
![Dependencies: none](https://img.shields.io/badge/runtime%20dependencies-none-brightgreen.svg)

Keel is an open-source, self-hosted, research-first quantitative system for [Outcome](https://outcome.xyz), the HIP-4 prediction-market platform on Hyperliquid. The full product covers market intelligence, fair value, validation, execution, risk and capital allocation. It is built in milestones, each accepted on measured evidence rather than on passing tests alone.

**Read-only by construction.** This release holds no keys, signs nothing and never calls Hyperliquid's exchange endpoint. Live trading stays behind a ten-condition gate.

## Status

| Milestone | Scope | State |
| --- | --- | --- |
| M0 Recorder | Outcome books, trades, settlements and live Polymarket references; quality reports, compaction, retention, disk budget, health page | Built; awaiting the 7-day always-on acceptance run |
| M1 Census + simulator | Snapshots, S1a/S1b censuses, minimum-detectable-edge table, HyperCore replay simulator, research registry, ground-truth validation | Built; censuses need 7 recorded days for final numbers |
| M2 Fair value, scanner, paper engine | | Next |

## What it records

| Stream | Source | Cadence |
| --- | --- | --- |
| Universe, templates, questions | `outcomeMeta` (REST) | 60 s |
| Order books, side 0 of every live Outcome market (side 1 is the mirror) | WebSocket `l2Book`; per-block (`fast`) for active sports and price markets, ~12 s otherwise | every push |
| Trades with buyer and seller addresses | WebSocket `trades` | every push |
| Outcome mids and perp mids | WebSocket `allMids` | every push |
| Perp marks (price markets settle on marks) | `metaAndAssetCtxs` (REST) | 60 s |
| Book and mirror audits | `l2Book`, `recentTrades` (REST), rotating subset | 60 s / 15 min |
| Matched Polymarket books (reference prices) | Polymarket CLOB WebSocket, with REST audits | every push |
| Broad reference context | Polymarket and Kalshi public APIs | hourly |
| Official settlements | `settledOutcome` (REST) | on removal |

Everything is written verbatim, append-only, to gzip JSON lines, then compacted daily into research tables (Parquet with `pyarrow`, gzip JSON lines without it). Activity tiering keeps quiet markets on the slower stream, and a disk budget, retention and per-source accounting keep storage bounded (expected 0.3–0.6 GB a day, measured per source in every report).

## Quick start

Needs Python 3.9 or newer and nothing else.

```bash
python3 -m pip install -e .        # optional; or run with PYTHONPATH=src
keel init                          # writes keel.json and creates data/
keel doctor                        # Python, disk, clock, Hyperliquid and reference connectivity
keel record                        # runs until Ctrl-C; or: keel record --duration 30m
keel status                        # from another terminal
keel health                        # read-only Health page on 127.0.0.1:8787
keel quality                       # data-quality report in data/reports/
keel compact --all                 # daily research tables in data/tables/
```

Research on compacted days:

```bash
keel research census               # S1a: universe, liquidity, markouts, reference-gap episodes, MDE table
keel research makers               # S1b: incumbent makers' P&L ex-rewards, kill-only
keel research validate             # checks the engine against ground truth
keel research mde --per-week 50,100 --test-days 45,90
```

If `keel doctor` reports a certificate error on macOS with a python.org build of Python, run `/Applications/Python 3.x/Install Certificates.command` once. For 24/7 recording see [docs/recorder.md](docs/recorder.md) and [deploy/](deploy/) (systemd on Linux, launchd on macOS).

## Tests

```bash
python3 -m pip install -e ".[dev]"
python3 -m pytest
```

The tests run the real recorder against local stand-ins for Hyperliquid's info API and WebSocket and Polymarket's market channel, fed with real mainnet payloads captured read-only (`tests/fixtures/`). Research code is tested on constructed markets with known answers. `keel research validate` goes further on real recordings: it replays real orders through the simulator and compares the fills with what the exchange printed, checks that the accounting conserves money, and recovers a planted edge.

## Layout

```
src/keel/
  hl/          Hyperliquid: coin encoding, outcomeMeta parsing, info client, WebSocket client
  refs/        reference feeds: Polymarket matching, live books, freshness, alignment
  recorder/    recorder process and crash-tolerant raw storage
  research/    snapshots, censuses, MDE, simulator, registry, evaluators, validation
  compact.py   daily research tables        quality.py   data-quality report
  ops.py       retention, disk budget, daily job, bench        health.py   Health page
deploy/        systemd and launchd units
docs/          operator and research documentation
tests/         unit, end-to-end and safety-invariant tests
```

## Documentation

- [docs/recorder.md](docs/recorder.md): operating the recorder
- [docs/data-quality.md](docs/data-quality.md): every metric in the quality report, defined
- [docs/references.md](docs/references.md): reference feeds, market matching and the freshness rule
- [docs/research.md](docs/research.md): snapshots, censuses, MDE, simulator, registry, evaluators and validation
- [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [CHANGELOG.md](CHANGELOG.md)

## Safety

This release cannot trade. There is no signing code, no key handling and no exchange client in the package, and `tests/test_safety_invariants.py` fails the build if any appear. Wallet creation, paid services, conformance trades and live capital each need the owner's separate approval. Nothing in this repository is investment advice, and no result in it is a claim of profitability.

## License

[Apache License 2.0](LICENSE).
