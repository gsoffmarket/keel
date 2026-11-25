"""Recorder configuration.

Configuration is a JSON file (`keel.json`) so it parses on any Python >= 3.9 without
extra packages. Durations must carry units ("60s", "15m"); bare numbers are refused.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from keel.hl.info import MAINNET_INFO
from keel.hl.ws import MAINNET_WS
from keel.units import parse_duration

# Identifying user-agent for reference feeds. keel.net.get always adds Accept-Encoding: gzip,
# which Polymarket requires (its HTTP 406 "gzip_required", diagnosed 2026-11-24).
HTTP_HEADERS = {"user-agent": "Mozilla/5.0 (compatible; keel-recorder/0.1; +https://github.com/gsoffmarket/keel)", "accept": "*/*"}

# Polymarket: tag_id=1 is "Sports" (tag_slug is ignored by /events). Ordered by 24 h volume.
DEFAULT_REFERENCES: List[Dict[str, Any]] = [
    {
        "name": "polymarket_sports",
        "url": "https://gamma-api.polymarket.com/events?active=true&closed=false&tag_id=1&order=volume24hr&ascending=false&limit=100",
        # Broad context only: live prices come from the matched market channel. Hourly, 100
        # events (~18 MB uncompressed, ~1.9 MB on the wire) keeps it near 0.05 GB/day stored.
        "every": "1h",
    },
    {
        "name": "kalshi_events",
        "url": "https://api.elections.kalshi.com/trade-api/v2/events?status=open&with_nested_markets=true&limit=200",
        # ~3.7 MB per response (0.31 MB gzipped on the wire, measured). Context only: Kalshi's
        # real-time feed needs an API key.
        # Milestone 2 replaces this with targeted polling of matched markets only.
        "every": "1h",
    },
]


@dataclass
class RecorderConfig:
    data_root: str = "data"
    info_url: str = MAINNET_INFO
    ws_url: str = MAINNET_WS
    meta_every: str = "60s"
    audit_every: str = "60s"
    audit_per_cycle: int = 10
    mirror_audit_every: str = "15m"
    perp_ctx_every: str = "60s"
    status_every: str = "10s"
    flush_every: str = "2s"
    unsubscribe_grace: str = "10m"
    max_subscriptions: int = 900
    rest_budget_share: float = 0.5
    record_all_mids: bool = True
    # Order-book stream. "standard": Hyperliquid's default l2Book stream, which for outcome
    # books is a snapshot every ~12 s (measured 2026-11-23). "fast": l2Book with fast=true,
    # a snapshot every block (~0.5 s); Keel stores only snapshots that changed, plus one
    # heartbeat per `book_heartbeat`. Fast mode downloads far more (about 190 KB/s for the
    # whole universe, ~16 GB/day) but stores little more than standard mode.
    book_mode: str = "fast"
    fast_venues: Optional[List[str]] = field(default_factory=lambda: ["out"])
    fast_categories: Optional[List[str]] = field(default_factory=lambda: ["sports", "price"])
    book_heartbeat: str = "10s"
    # Activity tiering (book_mode "fast"): an eligible market is on the fast stream while it
    # is active (trade, book change, or its Polymarket match moved within `fast_idle`) or
    # within `fast_window` of its scheduled start or expiry; otherwise standard. Every switch
    # is logged as a `tier_change` event, so research always knows each period's resolution.
    fast_tiering: bool = True
    fast_idle: str = "1h"
    fast_window: str = "6h"
    fast_min_dwell: str = "10m"
    settlement_checks: List[str] = field(default_factory=lambda: ["5s", "30s", "2m", "10m", "30m"])
    # Live reference prices for matched markets (Polymarket CLOB market channel, public).
    # Kalshi's WebSocket needs an API key (a credential), so it is not used without approval.
    pm_live: bool = True
    pm_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    pm_book_url: str = "https://clob.polymarket.com/book"
    match_every: str = "10m"
    match_min_level: str = "medium"
    pm_audit_every: str = "60s"
    pm_audit_per_cycle: int = 5
    max_reference_feed_age: str = "15s"
    # Operations
    stall_exit_after: str = "5m"  # no Hyperliquid stream data this long -> exit(3) so the supervisor restarts
    disk_warn_gb: float = 10.0
    disk_stop_gb: float = 2.0  # stop cleanly (exit 4) before the disk fills
    # Total disk Keel may use for its data folder. Over budget: delete the oldest raw days
    # whose tables are built and verified; if still over, pause the broad reference polls;
    # research streams are never deleted unverified.
    disk_budget_gb: Optional[float] = 15.0
    raw_retention_days: int = 14
    broad_reference_retention_days: int = 7
    health_ok_url: Optional[str] = None  # e.g. a healthchecks.io ping URL; pinged every minute while healthy
    health_fail_url: Optional[str] = None  # pinged when unhealthy
    # Keel trades only on Outcome. Other HIP-4 deployers stay visible through outcomeMeta;
    # set to null to record every deployer's books and trades.
    deployers: Optional[List[str]] = field(default_factory=lambda: ["out"])
    references: List[Dict[str, Any]] = field(default_factory=lambda: [dict(r) for r in DEFAULT_REFERENCES])

    def seconds(self, name: str) -> float:
        return parse_duration(getattr(self, name))

    def validate(self) -> None:
        for name in ("meta_every", "audit_every", "mirror_audit_every", "perp_ctx_every", "status_every", "flush_every", "unsubscribe_grace", "book_heartbeat", "match_every", "pm_audit_every", "max_reference_feed_age", "stall_exit_after", "fast_idle", "fast_window", "fast_min_dwell"):
            self.seconds(name)
        for u in (self.health_ok_url, self.health_fail_url):
            if u is not None and not u.startswith("https://"):
                raise ValueError("health URLs must be https")
        if self.disk_budget_gb is not None and self.disk_budget_gb <= 0:
            raise ValueError("disk_budget_gb must be positive or null")
        if self.raw_retention_days < 2:
            raise ValueError("raw_retention_days must be at least 2 (yesterday must be compacted first)")
        if not 0 <= self.disk_stop_gb < self.disk_warn_gb:
            raise ValueError("need 0 <= disk_stop_gb < disk_warn_gb")
        if self.match_min_level not in ("high", "medium"):
            raise ValueError('match_min_level must be "high" or "medium"')
        if self.book_mode not in ("standard", "fast"):
            raise ValueError('book_mode must be "standard" or "fast"')
        for d in self.settlement_checks:
            parse_duration(d)
        for r in self.references:
            if not {"name", "url", "every"} <= set(r):
                raise ValueError(f"reference needs name, url, every: {r}")
            parse_duration(r["every"])
            if "headers" in r and not isinstance(r["headers"], dict):
                raise ValueError(f"reference headers must be an object: {r['name']}")
            if not r["url"].startswith("https://"):
                raise ValueError(f"reference url must be https: {r['url']}")
        if not 1 <= self.max_subscriptions <= 1000:
            raise ValueError("max_subscriptions must be 1..1000 (Hyperliquid caps 1,000 per IP)")
        if self.audit_per_cycle < 0:
            raise ValueError("audit_per_cycle must be >= 0")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def load_config(path: Optional[Path]) -> RecorderConfig:
    cfg = RecorderConfig()
    if path and Path(path).exists():
        data = json.loads(Path(path).read_text())
        unknown = set(data) - set(cfg.to_dict())
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        for k, v in data.items():
            setattr(cfg, k, v)
    cfg.validate()
    return cfg


def write_default_config(path: Path) -> None:
    path.write_text(json.dumps(RecorderConfig().to_dict(), indent=2) + "\n")
