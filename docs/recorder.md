# Operating the recorder

## Commands

| Command | What it does | Exit codes |
| --- | --- | --- |
| `keel init [--force]` | Writes `keel.json` with defaults and creates `data/` | 0 ok, 2 file exists |
| `keel doctor [--offline] [--probe-references]` | Python version, writable data folder, free disk, clock offset vs exchange time, Hyperliquid info API and WebSocket, each reference feed | 0 ok, 2 a check failed |
| `keel record [--duration 30m]` | Runs the recorder; stops cleanly on Ctrl-C, SIGTERM or the duration | 0 |
| `keel status` | Reads `data/meta/status.json` (refreshed every 10 s) | 0, 4 no status |
| `keel quality [--hours N] [--out DIR]` | Writes the data-quality report (Markdown + JSON) | 0, 4 no data |
| `keel compact [--day D \| --all] [--include-today] [--jsonl]` | Builds daily tables and verifies them against a manifest | 0, 4 no raw data |
| `keel retain [--raw-days N] [--reference-days N] [--dry-run]` | Deletes raw day folders past retention, only for days whose tables verify | 0 |
| `keel daily [--day D]` | Compacts yesterday (UTC), writes its quality report to `data/reports/daily/`, applies retention | 0, 2 a check failed |
| `keel health [--port 8787] [--once FILE]` | Serves the read-only Health page on 127.0.0.1: recorder state, connections, fast-stream and reference counts, latency, disk against the budget, the latest quality checks, the daily job's result and recent notable events. Refreshes every 15 s; `/health.json` has the same data. Binds to loopback only; from another machine use `ssh -L 8787:127.0.0.1:8787 USER@SERVER` and open port 8787 locally | 0, 2 refused a non-loopback address |
| `keel bench [--duration 10m]` | Runs the recorder in `data/bench/` and reports CPU, memory, disk growth, download volume, latency to Hyperliquid and Polymarket, reconnects | 0 |
| `keel quality --day D` / `--session S` | Report for one UTC day or one recorder run | 0 |
| `keel settlements` | Fetches official results (`settledOutcome`) for removed outcomes that have none recorded, e.g. after the recorder stopped mid-retry | 0 |

Global options: `--config keel.json`, `--data DIR`.

## Configuration (`keel.json`)

Durations need units (`"60s"`, `"15m"`); a bare number is refused.

| Key | Default | Meaning |
| --- | --- | --- |
| `data_root` | `data` | Where everything is written |
| `meta_every` | `60s` | `outcomeMeta` poll; also the settlement-detection resolution |
| `audit_every`, `audit_per_cycle` | `60s`, `10` | REST book audit: this many outcomes per cycle, rotating |
| `mirror_audit_every` | `15m` | Side-0 vs side-1 trade check on one random outcome |
| `perp_ctx_every` | `60s` | Perp mark prices |
| `max_subscriptions` | `900` | Hyperliquid allows 1,000 per IP; 100 are kept spare |
| `rest_budget_share` | `0.5` | Share of the 1,200-weight/min REST budget the recorder may use |
| `deployers` | `["out"]` | Outcome only (Keel's sole trading venue). `null` records every HIP-4 deployer; others stay visible through `outcomeMeta` either way |
| `references` | Polymarket sports events and Kalshi open events, hourly (context only; live prices come from the matched feed) | Public GET endpoints; each `{name, url, every, headers?}`; https only. Requests always ask for gzip (Polymarket refuses requests without it) |
| `unsubscribe_grace` | `10m` | Keep streaming a removed outcome this long, to capture the settlement tail |
| `settlement_checks` | `["5s","30s","2m","10m","30m"]` | When to ask `settledOutcome` for a removed outcome's official result |
| `book_mode` | `fast` (since 2026-11-24, approved for Outcome sports and price markets only) | `standard`: Hyperliquid's default book stream, one snapshot per outcome about every 12 s. `fast`: a snapshot every block (~0.5 s) for the outcomes selected below |
| `fast_venues`, `fast_categories` | `["out"]`, `["sports","price"]` | Which outcomes use the fast stream when `book_mode` is `fast` (`null` = all) |
| `book_heartbeat` | `10s` | In fast mode, unchanged snapshots are not stored, except one per outcome per heartbeat |
| `fast_tiering`, `fast_idle`, `fast_window`, `fast_min_dwell` | `true`, `1h`, `6h`, `10m` | An eligible market is on the fast stream while active (live trade, book change, or its Polymarket match moved within `fast_idle`) or within `fast_window` of its start/expiry; otherwise standard, after at least `fast_min_dwell` on fast. Every switch is a `tier_change` event |
| `disk_budget_gb` | `15` | Cap on the data folder. Over it: delete the oldest raw days whose tables verify (checked every 10 min and by the daily job); if still over, pause the broad reference polls. Unverified raw is never deleted |
| `raw_retention_days`, `broad_reference_retention_days` | `14`, `7` | Raw kept this long (after its tables verify); tables, events and reports are kept |
| `pm_live`, `match_every`, `match_min_level` | `true`, `10m`, `medium` | Live Polymarket references for matched Outcome markets (see references.md) |
| `pm_audit_every`, `pm_audit_per_cycle` | `60s`, `5` | Polymarket REST book audit |
| `max_reference_feed_age` | `15s` | A reference is unusable if its feed has been silent longer |
| `stall_exit_after` | `5m` | No Hyperliquid stream data this long: exit code 3 so the supervisor restarts it |
| `disk_warn_gb`, `disk_stop_gb` | `10`, `2` | Warn below; stop cleanly (exit 4) below |
| `health_ok_url`, `health_fail_url` | none | Optional external monitor (e.g. healthchecks.io), pinged every 30 s |

### Book stream modes and bandwidth

Measured on 2026-11-23 from Lagos:

| Mode | Book updates | Download (whole universe) | Stored (gzip) |
| --- | --- | --- | --- |
| `standard` | one snapshot per outcome every ~12 s, all outcomes in the same burst | ~30 KB/s, ~2.6 GB/day | ~0.3 GB/day |
| `fast`, all outcomes | every block (~0.5 s); ~25 of ~530 snapshots a second differ from the last | ~190 KB/s, ~16 GB/day | ~0.3–0.5 GB/day (changes + heartbeats only) |
| `fast`, Outcome sports + price only (~160 outcomes) | as above for those outcomes | ~100 KB/s, ~9 GB/day | similar |

Standard mode misses book changes between snapshots: in the first test, 42 of 260 REST snapshots differed from the latest WebSocket book, which was a median 6.5 s old. Research on short-lived price gaps needs fast mode. Fast mode's cost is download volume, which matters on a metered connection.

Each live outcome uses two subscriptions (book + trades), plus one for `allMids`, so the default cap covers 449 outcomes. Above that, new outcomes are refused and logged (`ws_subscription_refused`), never silently dropped; the quality report shows them.

## Resource use (measured 2026-11-24, Outcome-only data rates)

Stored per day before these controls: about 0.9 GB (Hyperliquid books 0.34, broad Polymarket poll 0.27, live Polymarket books 0.17, the rest 0.07). With Outcome-only recording, hourly broad polls and activity tiering the expected rate is about 0.3–0.4 GB a day; with 14 days of raw that settles near 5–6 GB plus slowly growing tables, inside the 15 GB budget. Download is dominated by fast-stream books (about 12 GB a day with all 159 eligible markets fast; tiering reduces it in proportion to the markets that are idle). The daily report lists bytes per source for the day so these figures stay measured, not assumed.

## Data layout

```
data/
  raw/<src>/<YYYY-MM-DD>/<HH>Z-<session>.jsonl.gz   verbatim records (hl_ws, hl_rest, ref_<name>)
  meta/events/<YYYY-MM-DD>.jsonl                    connects, disconnects, universe changes, settlements, errors
  meta/status.json                                  live status
  tables/<table>/date=<D>/part-0.(parquet|jsonl.gz)  daily research tables
  tables/manifest-<D>.json                          sources, row counts, SHA-256
  reports/quality-<ts>.(md|json)
```

A raw line is `{"seq","recv_ns","src","req","msg"}`, with `msg` exactly as received. Each process start writes new files, so a crash never corrupts earlier data; the reader skips an incomplete last line. Expect roughly 1 GB per day compressed with the full universe; compaction does not delete raw files (the PRD keeps raw for 90 days; deleting is left to the operator).

## Running continuously

On a Linux server use `deploy/systemd/` (service with automatic restart, hardening and a daily timer); full steps in `deploy/README.md`. On a Mac use `deploy/launchd/`.

macOS (launchd): save as `~/Library/LaunchAgents/xyz.keel.recorder.plist`, adjust the paths, then `launchctl load ~/Library/LaunchAgents/xyz.keel.recorder.plist`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>xyz.keel.recorder</string>
  <key>WorkingDirectory</key><string>/Users/YOU/Documents/keel</string>
  <key>ProgramArguments</key><array>
    <string>/usr/bin/env</string><string>PYTHONPATH=src</string>
    <string>python3</string><string>-m</string><string>keel</string><string>record</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/Users/YOU/Documents/keel/data/recorder.log</string>
  <key>StandardErrorPath</key><string>/Users/YOU/Documents/keel/data/recorder.log</string>
</dict></plist>
```

Linux (systemd): `ExecStart=/usr/bin/python3 -m keel record`, `WorkingDirectory=/opt/keel`, `Environment=PYTHONPATH=src`, `Restart=always`, `RestartSec=5`, plus `ProtectSystem=strict`, `ReadWritePaths=/opt/keel/data`, `NoNewPrivileges=true`.

A laptop that sleeps will show the sleep as masked minutes; the PRD's 7-day ≥ 95% acceptance needs an always-on machine or a small VPS (a VPS is a paid service and needs the owner's approval).

## Failure behaviour

| Failure | Behaviour |
| --- | --- |
| WebSocket drops | Reconnect with exponential backoff (1 s to 60 s, jittered), resubscribe everything; the gap is visible as masked minutes |
| No message for 45 s | Treated as dead; reconnect (`idle_timeout`) |
| REST error | Logged as `rest_error`; the next cycle retries; recording continues |
| Reference feed down | Logged as `reference_error`; Hyperliquid recording is unaffected |
| Subscription cap reached | New subscriptions refused and logged |
| Crash or kill | Raw data up to the last flush (2 s) is kept; restart writes new files |
