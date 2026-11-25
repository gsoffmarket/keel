"""keel-recorder: records every public stream Keel's research needs. Read-only.

Streams (PRD, Data infrastructure):
- `outcomeMeta` every 60 s (universe, templates, settlement detection)
- WebSocket `l2Book` + `trades` for side 0 of every live outcome (side 1 is mirrored)
- WebSocket `allMids` (outcome mids and every perp mid: price-market references)
- `metaAndAssetCtxs` every 60 s (perp mark prices; price markets settle on marks)
- REST `l2Book` audit of a rotating subset (checks the WebSocket book)
- REST `recentTrades` mirror audit (side 0 vs side 1)
- public reference feeds (Polymarket, Kalshi) on their own cadence

The recorder holds no keys and sends no orders. It never calls the exchange endpoint.
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import signal
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

from keel import __version__, net
from keel.config import HTTP_HEADERS, RecorderConfig
from keel.hl.info import InfoClient, InfoError
from keel.hl.metadata import Instance, Universe, event_time_ns, parse_outcome_meta, summarize
from keel.hl.ws import HLStream
from keel.recorder.store import RawStore
from keel.refs.contracts import contracts_from_meta
from keel.refs.pmstream import PMStream
from keel.refs.polymarket import GammaClient, Mapping, match_all
from keel.refs.refbook import PMBooks
from keel.units import parse_duration

_CHANNEL = re.compile(r'"channel"\s*:\s*"(\w+)"')
_COIN = re.compile(r'"coin"\s*:\s*"([^"]+)"')


def infer_settlement(last_mid: Optional[float]) -> Dict[str, Any]:
    """Best available settlement label from the final observed mid.

    No public endpoint for settlement results has been verified yet, so the label
    is marked `final_mid_inferred` and is only set when the mid is at an extreme.
    """
    if last_mid is None:
        return {"settle_fraction": None, "settle_source": "unknown_no_mid"}
    if last_mid >= 0.99:
        return {"settle_fraction": 1.0, "settle_source": "final_mid_inferred"}
    if last_mid <= 0.01:
        return {"settle_fraction": 0.0, "settle_source": "final_mid_inferred"}
    return {"settle_fraction": None, "settle_source": "unknown_mid_not_extreme"}


class Recorder:
    def __init__(self, cfg: RecorderConfig, info: Optional[InfoClient] = None, stream_factory=None, gamma: Optional[GammaClient] = None) -> None:
        cfg.validate()
        self.cfg = cfg
        self.root = Path(cfg.data_root)
        self.store = RawStore(self.root, flush_every=cfg.seconds("flush_every"))
        self.info = info or InfoClient(cfg.info_url, budget_share=cfg.rest_budget_share)
        self.universe = Universe()
        self.stream = (stream_factory or HLStream)(self._on_ws, self._on_ws_event, url=cfg.ws_url, max_subscriptions=cfg.max_subscriptions)
        self.last_mid: Dict[str, float] = {}
        self.last_book_ns: Dict[str, int] = {}
        self.ws_counts: Dict[str, int] = {}
        self.pending_unsub: Dict[str, float] = {}
        self.started_ns = 0
        self.errors: Dict[str, int] = {}
        self._stop = asyncio.Event()
        self._audit_cursor = 0
        self.fast_coins: set = set()
        self.book_subs: Dict[str, dict] = {}
        self._last_levels: Dict[str, str] = {}
        self._last_book_write: Dict[str, int] = {}
        self._heartbeat_ns = int(cfg.seconds("book_heartbeat") * 1e9)
        self._side_tasks: set = set()
        self.wire_bytes: Dict[str, int] = {}
        self.meta = None
        self.mappings: Dict[int, Mapping] = {}
        self.unmatched: Dict[int, str] = {}
        self.pm_books = PMBooks()
        self.pm = PMStream(self._on_pm, self._on_pm_event, url=cfg.pm_ws_url)
        self.gamma = gamma or GammaClient()
        self._pm_audit_cursor = 0
        self.exit_code = 0
        self.last_hl_msg_mono = time.monotonic()
        self.hl_rtt_ms: List[float] = []
        self._disk_warned_mono = 0.0
        self.disk_free_gb: Optional[float] = None
        self.health_pings = {"ok": 0, "fail": 0, "errors": 0}
        # activity tiering
        self.eligible_fast: set = set()
        self.tier: Dict[str, str] = {}
        self.tier_since: Dict[str, float] = {}
        self.activity: Dict[str, float] = {}
        self.event_ns: Dict[str, Optional[int]] = {}
        self._std_levels: Dict[str, str] = {}
        self.token_coin: Dict[str, str] = {}
        self.tier_changes = 0
        # disk budget
        self.broad_refs_paused = False
        self.data_gb: Optional[float] = None
        self._budget_checked_mono = 0.0
        self._ssl = ssl.create_default_context()

    # ---------- WebSocket path ----------
    def _on_ws(self, text: str, recv_ns: int) -> None:
        self.last_hl_msg_mono = time.monotonic()
        m = _CHANNEL.search(text, 0, 80)
        ch = m.group(1) if m else "other"
        if ch == "l2Book":
            c = _COIN.search(text, 0, 200)
            coin = c.group(1) if c else ""
            if coin:
                self.last_book_ns[coin] = recv_ns
            if coin in self.fast_coins and not self._book_changed(coin, text, recv_ns):
                self._count("l2Book_unchanged_not_stored")
                return
            if coin and coin in self.eligible_fast and coin not in self.fast_coins:
                i = text.find('"levels"')
                lv = text[i:] if i >= 0 else text
                prev = self._std_levels.get(coin)
                self._std_levels[coin] = lv
                if prev is not None and prev != lv:
                    self._mark_active(coin, "book_change")
        elif ch == "trades":
            c = _COIN.search(text, 0, 200)
            if c and c.group(1) in self.eligible_fast:
                # subscription backfill also arrives here; a real trade within fast_idle is what counts
                self._mark_active(c.group(1), "trade", text)
        self.store.write("hl_ws", text, recv_ns)
        # light parsing for live state only; research reads the raw store
        if ch == "allMids":
            try:
                mids = json.loads(text)["data"]["mids"]
                for k, v in mids.items():
                    if k.startswith("#"):
                        self.last_mid[k] = float(v)
            except (ValueError, KeyError, TypeError):
                self._err("parse_allMids")
        elif ch == "pong" and self.stream.ping_sent_mono:
            self.hl_rtt_ms.append((time.monotonic() - self.stream.ping_sent_mono) * 1000)
            del self.hl_rtt_ms[:-500]
        elif ch == "error":
            self.store.event("ws_error_message", {"text": text[:500]})
        self._count(ch if ch in ("allMids", "l2Book", "trades", "pong", "error", "subscriptionResponse") else "other")

    def _book_changed(self, coin: str, text: str, recv_ns: int) -> bool:
        """Fast mode: store a snapshot only if its levels differ from the last stored one,
        or if `book_heartbeat` has passed (so liveness stays visible in the data)."""
        i = text.find('"levels"')
        levels = text[i:] if i >= 0 else text
        if levels != self._last_levels.get(coin) or recv_ns - self._last_book_write.get(coin, 0) >= self._heartbeat_ns:
            self._last_levels[coin] = levels
            self._last_book_write[coin] = recv_ns
            return True
        return False

    def _eligible(self, venue: str, category: str) -> bool:
        c = self.cfg
        return c.book_mode == "fast" and (c.fast_venues is None or venue in c.fast_venues) and (c.fast_categories is None or category in c.fast_categories)

    def _book_sub(self, coin: str, venue: str, category: str) -> dict:
        """Initial book subscription. Without tiering every eligible market is fast; with
        tiering it starts fast only if it is inside its event window."""
        if self._eligible(venue, category):
            self.eligible_fast.add(coin)
            fast = not self.cfg.fast_tiering or self._in_window(coin)
        else:
            fast = False
        self._set_tier_state(coin, "fast" if fast else "standard", "initial")
        if fast:
            self.fast_coins.add(coin)
            return {"type": "l2Book", "coin": coin, "fast": True}
        return {"type": "l2Book", "coin": coin}

    # ---------- activity tiering ----------
    def _in_window(self, coin: str) -> bool:
        t = self.event_ns.get(coin)
        if t is None:
            return False
        return abs(time.time_ns() - t) <= self.cfg.seconds("fast_window") * 1e9

    def _set_tier_state(self, coin: str, tier: str, reason: str) -> None:
        if self.tier.get(coin) == tier:
            return
        self.tier[coin] = tier
        self.tier_since[coin] = time.monotonic()
        self.tier_changes += 1
        self.store.event("tier_change", {"coin": coin, "tier": tier, "reason": reason})

    def _mark_active(self, coin: str, why: str, text: Optional[str] = None) -> None:
        if why == "trade" and text is not None:
            # ignore subscription backfill: only trades newer than fast_idle count
            m = re.search(r'"time"\s*:\s*(\d+)', text)
            if m and time.time() * 1000 - int(m.group(1)) > self.cfg.seconds("fast_idle") * 1000:
                return
        self.activity[coin] = time.monotonic()
        if self.cfg.fast_tiering and self.tier.get(coin) == "standard":
            self._spawn(self._switch_tier(coin, "fast", why))

    async def _switch_tier(self, coin: str, tier: str, reason: str) -> None:
        if self.tier.get(coin) == tier or coin not in self.book_subs:
            return
        new = {"type": "l2Book", "coin": coin, "fast": True} if tier == "fast" else {"type": "l2Book", "coin": coin}
        old = self.book_subs[coin]
        self._set_tier_state(coin, tier, reason)  # before awaiting, so concurrent triggers do not repeat it
        if tier == "fast":
            self.fast_coins.add(coin)
        else:
            self.fast_coins.discard(coin)
            self._std_levels.pop(coin, None)
        self.book_subs[coin] = new
        await self.stream.subscribe(new)  # subscribe first: no gap
        await self.stream.unsubscribe(old)

    async def retier(self) -> None:
        """Every 30 s: demote fast markets idle for fast_idle (after fast_min_dwell) and
        outside their window; promote standard markets entering their window."""
        if not self.cfg.fast_tiering:
            return
        now = time.monotonic()
        idle = self.cfg.seconds("fast_idle")
        dwell = self.cfg.seconds("fast_min_dwell")
        for coin in list(self.eligible_fast):
            active = now - self.activity.get(coin, -1e18) < idle
            want = active or self._in_window(coin)
            t = self.tier.get(coin)
            if t == "standard" and want:
                await self._switch_tier(coin, "fast", "window" if not active else "activity")
            elif t == "fast" and not want and now - self.tier_since.get(coin, now) >= dwell:
                await self._switch_tier(coin, "standard", "idle")

    def _on_ws_event(self, kind: str, data: dict) -> None:
        self.store.event(f"ws_{kind}", data)

    def _count(self, k: str) -> None:
        self.ws_counts[k] = self.ws_counts.get(k, 0) + 1

    def _err(self, k: str) -> None:
        self.errors[k] = self.errors.get(k, 0) + 1

    # ---------- REST path ----------
    async def _rest(self, body: Dict[str, Any], tag: str) -> Optional[str]:
        try:
            text, ms = await self.info.post_raw(body)
        except InfoError as exc:
            self._err(f"rest_{tag}")
            self.store.event("rest_error", {"req": body, "error": str(exc)})
            return None
        self.store.write("hl_rest", text, time.time_ns(), req=body)
        return text

    async def poll_meta(self) -> None:
        text = await self._rest({"type": "outcomeMeta"}, "outcomeMeta")
        if text is None:
            return
        ts = time.time_ns()
        try:
            meta = parse_outcome_meta(json.loads(text))
        except (ValueError, KeyError, TypeError) as exc:
            self._err("parse_outcomeMeta")
            self.store.event("meta_parse_error", {"error": repr(exc)})
            return
        if self.cfg.deployers is not None:
            meta.outcomes = {k: o for k, o in meta.outcomes.items() if o.venue in self.cfg.deployers}
        self.meta = meta
        change = self.universe.apply(meta, ts)
        for inst in change.added:
            self.store.event("outcome_added", {"instance_id": inst.instance_id, "coin": inst.outcome.coin0, "venue": inst.outcome.venue, "category": inst.outcome.category, "name": inst.outcome.name, "description": inst.outcome.description, "question": inst.outcome.question, "role": inst.outcome.role}, ts)
            self.pending_unsub.pop(inst.outcome.coin0, None)
            coin = inst.outcome.coin0
            q = meta.questions.get(inst.outcome.question) if inst.outcome.question is not None else None
            self.event_ns[coin] = event_time_ns(inst.outcome, q)
            self.book_subs[coin] = self._book_sub(coin, inst.outcome.venue, inst.outcome.category)
            await self.stream.subscribe(self.book_subs[coin])
            await self.stream.subscribe({"type": "trades", "coin": coin})
        for inst, prev in change.changed:
            self.store.event("outcome_changed", {"instance_id": inst.instance_id, "coin": inst.outcome.coin0, "before": prev.description, "after": inst.outcome.description, "fee_scale_before": prev.fee_scale, "fee_scale_after": inst.outcome.fee_scale}, ts)
        for inst in change.removed:
            self._record_removal(inst, ts)
        for qid, oid in change.newly_settled_named:
            self.store.event("question_named_settled", {"question": qid, "outcome": oid}, ts)
        if change.added or change.removed:
            self.store.event("universe", {"live": len(self.universe.live), **summarize(i.outcome for i in self.universe.live.values())}, ts)

    def _record_removal(self, inst: Instance, ts: int) -> None:
        coin = inst.outcome.coin0
        mid = self.last_mid.get(coin)
        self.store.event(
            "outcome_removed",
            {
                "instance_id": inst.instance_id,
                "coin": coin,
                "venue": inst.outcome.venue,
                "category": inst.outcome.category,
                "first_seen_ns": inst.first_seen_ns,
                "last_listed_ns": inst.last_seen_ns,
                "removed_seen_ns": ts,
                "last_mid": mid,
                "last_book_recv_ns": self.last_book_ns.get(coin),
                "observed_from_listing": inst.first_seen_ns > self.started_ns,
                **infer_settlement(mid),
            },
            ts,
        )
        self.pending_unsub[coin] = time.monotonic() + self.cfg.seconds("unsubscribe_grace")
        self._spawn(self.fetch_settlement(inst))
        m = self.mappings.pop(inst.outcome.outcome, None)
        if m is not None:
            self.store.event("mapping_removed", {"outcome": m.outcome, "coin": m.coin, "token_id": m.token_id, "reason": "outcome removed"}, ts)
            self._spawn(self.pm.set_assets({x.token_id for x in self.mappings.values()}))

    def _spawn(self, coro) -> None:
        t = asyncio.ensure_future(coro)
        self._side_tasks.add(t)
        t.add_done_callback(self._side_tasks.discard)

    async def fetch_settlement(self, inst: Instance) -> None:
        """Official result from the documented `settledOutcome` info request, retried
        because it can lag the outcome's removal from `outcomeMeta`."""
        oid = inst.outcome.outcome
        waited = 0.0
        for d in self.cfg.settlement_checks:
            delay = parse_duration(d) - waited
            await self._sleep(max(0.0, delay))
            waited += max(0.0, delay)
            if self._stop.is_set():
                break
            text = await self._rest({"type": "settledOutcome", "outcome": oid}, "settledOutcome")
            if not text or text.strip() == "null":
                continue
            try:
                doc = json.loads(text)
                spec = doc.get("spec") or {}
                self.store.event("outcome_settled", {
                    "instance_id": inst.instance_id, "coin": inst.outcome.coin0, "outcome": oid,
                    "settle_fraction": float(doc["settleFraction"]), "details": doc.get("details"),
                    "description_at_settlement": spec.get("description"), "waited_s": waited,
                    "last_mid": self.last_mid.get(inst.outcome.coin0),
                })
                return
            except (ValueError, KeyError, TypeError) as exc:
                self._err("parse_settledOutcome")
                self.store.event("settlement_parse_error", {"outcome": oid, "error": repr(exc)})
                return
        self.store.event("outcome_settlement_missing", {"instance_id": inst.instance_id, "coin": inst.outcome.coin0, "outcome": oid, "waited_s": waited})

    async def audit_books(self) -> None:
        coins = self.universe.coins()
        if not coins or self.cfg.audit_per_cycle == 0:
            return
        n = min(self.cfg.audit_per_cycle, len(coins))
        for k in range(n):
            coin = coins[(self._audit_cursor + k) % len(coins)]
            await self._rest({"type": "l2Book", "coin": coin}, "l2Book_audit")
        self._audit_cursor = (self._audit_cursor + n) % len(coins)

    async def mirror_audit(self) -> None:
        coins = self.universe.coins()
        if not coins:
            return
        coin = random.choice(coins)
        side1 = "#" + str(int(coin[1:]) + 1)
        await self._rest({"type": "recentTrades", "coin": coin}, "mirror0")
        await self._rest({"type": "recentTrades", "coin": side1}, "mirror1")

    async def perp_ctx(self) -> None:
        await self._rest({"type": "metaAndAssetCtxs"}, "metaAndAssetCtxs")

    async def poll_reference(self, ref: Dict[str, Any]) -> None:
        if self.broad_refs_paused:
            return
        try:
            text, wire, _ = await asyncio.get_running_loop().run_in_executor(None, net.get, ref["url"], ref.get("headers") or HTTP_HEADERS)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            self._err(f"ref_{ref['name']}")
            detail = str(exc)[:300]
            if isinstance(exc, urllib.error.HTTPError):
                try:
                    detail += " " + exc.read()[:200].decode(errors="replace")
                except Exception:
                    pass
            self.store.event("reference_error", {"name": ref["name"], "error": detail})
            return
        self.wire_bytes[ref["name"]] = self.wire_bytes.get(ref["name"], 0) + wire
        self.store.write(f"ref_{ref['name']}", text, time.time_ns(), req={"url": ref["url"], "wire_bytes": wire})

    # ---------- live references ----------
    def _on_pm(self, text: str, recv_ns: int) -> None:
        self.store.write("pm_ws", text, recv_ns)
        self.pm_books.apply_text(text, recv_ns)
        self._count("pm_messages")
        if self.token_coin and '"price_change"' in text[:200]:
            for tok in set(re.findall(r'"asset_id"\s*:\s*"(\d+)"', text)):
                coin = self.token_coin.get(tok)
                if coin and coin in self.eligible_fast:
                    self._mark_active(coin, "reference_moved")

    def _on_pm_event(self, kind: str, data: dict) -> None:
        now = time.time_ns()
        if kind == "pm_connected":
            self.pm_books.on_connect(now)
        elif kind == "pm_disconnected":
            self.pm_books.on_disconnect(now)
        self.store.event(kind, data, now)

    async def match_references(self) -> None:
        """Match live Outcome sports markets to Polymarket and keep the market-channel
        subscriptions equal to the matched tokens. Every change is logged with its
        confidence and the evidence behind it."""
        if self.meta is None or not self.cfg.pm_live:
            return
        contracts = contracts_from_meta(self.meta)
        maps, reasons = await asyncio.get_running_loop().run_in_executor(None, match_all, contracts, self.gamma)
        allowed = {"high"} if self.cfg.match_min_level == "high" else {"high", "medium"}
        new = {m.outcome: m for m in maps if m.level in allowed}
        ts = time.time_ns()
        for oid, m in new.items():
            old = self.mappings.get(oid)
            if old is None:
                self.store.event("mapping_added", m.to_dict(), ts)
            elif (old.token_id, old.level) != (m.token_id, m.level):
                self.store.event("mapping_changed", {"before": old.to_dict(), "after": m.to_dict()}, ts)
        for oid, old in self.mappings.items():
            if oid not in new:
                self.store.event("mapping_removed", {"outcome": oid, "coin": old.coin, "token_id": old.token_id, "reason": reasons.get(oid, "no longer matched")}, ts)
        self.mappings = new
        self.unmatched = reasons
        self.token_coin = {m.token_id: m.coin for m in new.values()}
        levels: Dict[str, int] = {}
        for m in new.values():
            levels[m.level] = levels.get(m.level, 0) + 1
        self.store.event("matching_summary", {"contracts": len(contracts), "matched": len(new), "levels": levels, "unmatched": len(reasons), "reasons": _top_reasons(reasons), "gamma_requests": self.gamma.requests}, ts)
        p = self.root / "meta" / "mappings.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps({"updated_ns": ts, "mappings": [m.to_dict() for m in new.values()], "unmatched": {str(k): v for k, v in reasons.items()}}, indent=1))
        tmp.replace(p)
        await self.pm.set_assets({m.token_id for m in new.values()})

    async def audit_pm_books(self) -> None:
        toks = sorted({m.token_id for m in self.mappings.values()})
        if not toks or self.cfg.pm_audit_per_cycle == 0:
            return
        n = min(self.cfg.pm_audit_per_cycle, len(toks))
        for k in range(n):
            tok = toks[(self._pm_audit_cursor + k) % len(toks)]
            url = f"{self.cfg.pm_book_url}?token_id={tok}"
            try:
                text, wire, _ = await asyncio.get_running_loop().run_in_executor(None, net.get, url, HTTP_HEADERS)
            except (urllib.error.URLError, OSError, ValueError) as exc:
                self._err("pm_rest_audit")
                self.store.event("reference_error", {"name": "pm_book_audit", "error": str(exc)[:200]})
                continue
            self.store.write("pm_rest", text, time.time_ns(), req={"url": url, "token_id": tok})
        self._pm_audit_cursor = (self._pm_audit_cursor + n) % len(toks)

    def reference_status(self) -> Dict[str, Any]:
        now = time.time_ns()
        max_age = self.cfg.seconds("max_reference_feed_age")
        usable = sum(1 for m in self.mappings.values() if self.pm_books.state(m.token_id, now, max_age)["usable"])
        return {
            "mapped_outcomes": len(self.mappings),
            "usable_now": usable,
            "pm_connected": self.pm.connected.is_set(),
            "pm_reconnects": self.pm.reconnects,
            "pm_pongs": self.pm.pongs,
            "pm_last_pong_age_s": round((now - self.pm.last_pong_ns) / 1e9, 1) if self.pm.last_pong_ns else None,
            "pm_bytes_in": self.pm.bytes_in,
        }

    # ---------- operations ----------
    def healthy(self) -> bool:
        return self.stream.connected.is_set() and time.monotonic() - self.last_hl_msg_mono < 30

    async def ops_check(self) -> None:
        """Stall exit, disk guard and external health ping (every 30 s)."""
        import shutil

        stalled = time.monotonic() - self.last_hl_msg_mono
        if stalled > self.cfg.seconds("stall_exit_after"):
            self.store.event("recorder_stalled", {"seconds_without_data": round(stalled, 1)})
            self.exit_code = 3
            self.stop()
            return
        free = shutil.disk_usage(self.root).free / 1e9
        self.disk_free_gb = round(free, 2)
        if free < self.cfg.disk_stop_gb:
            self.store.event("disk_critical", {"free_gb": self.disk_free_gb, "stop_below_gb": self.cfg.disk_stop_gb})
            self.exit_code = 4
            self.stop()
            return
        if free < self.cfg.disk_warn_gb and time.monotonic() - self._disk_warned_mono > 3600:
            self._disk_warned_mono = time.monotonic()
            self.store.event("disk_low", {"free_gb": self.disk_free_gb, "warn_below_gb": self.cfg.disk_warn_gb})
        if self.cfg.disk_budget_gb and time.monotonic() - self._budget_checked_mono > 600:
            self._budget_checked_mono = time.monotonic()
            await self.check_budget()
        url = self.cfg.health_ok_url if self.healthy() else self.cfg.health_fail_url
        if url:
            try:
                await asyncio.get_running_loop().run_in_executor(None, net.get, url, HTTP_HEADERS, 10.0)
                self.health_pings["ok" if self.healthy() else "fail"] += 1
            except Exception:
                self.health_pings["errors"] += 1

    async def check_budget(self) -> None:
        from keel.ops import data_bytes, enforce_budget

        budget = int(self.cfg.disk_budget_gb * 1e9)
        loop = asyncio.get_running_loop()
        used = await loop.run_in_executor(None, data_bytes, self.root)
        self.data_gb = round(used / 1e9, 3)
        if used <= budget:
            if self.broad_refs_paused and used < 0.9 * budget:
                self.broad_refs_paused = False
                self.store.event("budget_ok", {"data_gb": self.data_gb, "broad_reference_polls": "resumed"})
            return
        res = await loop.run_in_executor(None, enforce_budget, self.root, budget)
        self.store.event("budget_enforced", {k: v for k, v in res.items()})
        self.data_gb = round(res["used_after"] / 1e9, 3)
        if not res["within_budget"] and not self.broad_refs_paused:
            self.broad_refs_paused = True
            self.store.event("budget_exceeded", {"data_gb": self.data_gb, "budget_gb": self.cfg.disk_budget_gb, "action": "broad reference polls paused; research streams untouched", "unverified_days": res["skipped_unverified"]})

    async def housekeeping(self) -> None:
        now = time.monotonic()
        for coin, due in list(self.pending_unsub.items()):
            if now >= due:
                await self.stream.unsubscribe(self.book_subs.pop(coin, {"type": "l2Book", "coin": coin}))
                await self.stream.unsubscribe({"type": "trades", "coin": coin})
                self.fast_coins.discard(coin)
                self.eligible_fast.discard(coin)
                self.tier.pop(coin, None)
                self.pending_unsub.pop(coin)
                self.store.event("unsubscribed", {"coin": coin})

    def status(self) -> Dict[str, Any]:
        now = time.time_ns()
        ages = [now - t for t in self.last_book_ns.values()]
        return {
            "version": __version__,
            "session": self.store.session,
            "started_ns": self.started_ns,
            "now_ns": now,
            "uptime_s": round((now - self.started_ns) / 1e9, 1) if self.started_ns else 0,
            "connected": self.stream.connected.is_set(),
            "reconnects": self.stream.reconnects,
            "subscriptions": len(self.stream.subs),
            "subscriptions_refused": len(self.stream.refused),
            "live_outcomes": len(self.universe.live),
            "ws_counts": dict(self.ws_counts),
            "raw_counts": dict(self.store.counts),
            "rest_requests": self.info.requests,
            "errors": dict(self.errors),
            "reference_wire_bytes": dict(self.wire_bytes),
            "references_live": self.reference_status(),
            "healthy": self.healthy(),
            "exit_code": self.exit_code,
            "disk_free_gb": self.disk_free_gb,
            "health_pings": dict(self.health_pings),
            "hl_info_latency_ms_p50": _p(self.info.latency_ms, 0.5),
            "hl_info_latency_ms_p95": _p(self.info.latency_ms, 0.95),
            "hl_ws_ping_rtt_ms_p50": _p(self.hl_rtt_ms, 0.5),
            "hl_ws_ping_rtt_ms_p95": _p(self.hl_rtt_ms, 0.95),
            "pm_ws_ping_rtt_ms_p50": _p(self.pm.rtt_ms, 0.5),
            "hl_ws_bytes_in": getattr(self.stream.ws, "bytes_in", None) if self.stream.ws else None,
            "book_mode": self.cfg.book_mode,
            "fast_book_coins": len(self.fast_coins),
            "fast_eligible": len(self.eligible_fast),
            "tier_changes": self.tier_changes,
            "data_gb": self.data_gb,
            "disk_budget_gb": self.cfg.disk_budget_gb,
            "broad_refs_paused": self.broad_refs_paused,
            "book_age_s_p50": round(sorted(ages)[len(ages) // 2] / 1e9, 1) if ages else None,
        }

    def _write_status(self) -> None:
        p = self.root / "meta" / "status.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.status(), indent=2))
        tmp.replace(p)

    async def _every(self, seconds: float, fn, initial_delay: float = 0.0) -> None:
        await self._sleep(initial_delay)
        while not self._stop.is_set():
            t0 = time.monotonic()
            try:
                await fn()
            except Exception as exc:  # a failing poller must never stop the recorder
                self._err(f"task_{getattr(fn, '__name__', 'fn')}")
                self.store.event("task_error", {"task": getattr(fn, "__name__", "fn"), "error": repr(exc)[:300]})
            await self._sleep(max(0.0, seconds - (time.monotonic() - t0)))

    async def _sleep(self, s: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=s)
        except asyncio.TimeoutError:
            pass

    async def _status_loop(self) -> None:
        self._write_status()

    def stop(self) -> None:
        self._stop.set()
        self.stream.stop()

    async def run(self, duration: Optional[float] = None) -> Dict[str, Any]:
        self.started_ns = time.time_ns()
        self.store.event("session_start", {"version": __version__, "config": self.cfg.to_dict()})
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop)
            except (NotImplementedError, RuntimeError):
                pass
        if self.cfg.record_all_mids:
            await self.stream.subscribe({"type": "allMids"})
        await self.poll_meta()
        c = self.cfg
        tasks: List[asyncio.Future] = [
            asyncio.ensure_future(self.stream.run()),
            asyncio.ensure_future(self._every(c.seconds("meta_every"), self.poll_meta, c.seconds("meta_every"))),
            asyncio.ensure_future(self._every(c.seconds("audit_every"), self.audit_books, min(5.0, c.seconds("audit_every")))),
            asyncio.ensure_future(self._every(c.seconds("mirror_audit_every"), self.mirror_audit, min(20.0, c.seconds("mirror_audit_every")))),
            asyncio.ensure_future(self._every(c.seconds("perp_ctx_every"), self.perp_ctx, 1.0)),
            asyncio.ensure_future(self._every(c.seconds("status_every"), self._status_loop)),
            asyncio.ensure_future(self._every(min(30.0, c.seconds("unsubscribe_grace")), self.housekeeping)),
            asyncio.ensure_future(self._every(min(30.0, c.seconds("stall_exit_after")), self.ops_check, min(30.0, c.seconds("stall_exit_after")))),
            asyncio.ensure_future(self._every(30.0, self.retier, 30.0)),
        ]
        if c.pm_live:
            tasks += [
                asyncio.ensure_future(self.pm.run()),
                asyncio.ensure_future(self._every(c.seconds("match_every"), self.match_references, 1.0)),
                asyncio.ensure_future(self._every(c.seconds("pm_audit_every"), self.audit_pm_books, min(30.0, c.seconds("pm_audit_every")))),
            ]
        for ref in c.references:
            fn = (lambda r=ref: self.poll_reference(r))
            fn.__name__ = f"ref_{ref['name']}"  # type: ignore[attr-defined]
            tasks.append(asyncio.ensure_future(self._every(parse_duration(ref["every"]), fn, min(2.0, parse_duration(ref["every"])))))
        if duration is not None:
            tasks.append(asyncio.ensure_future(self._stop_after(duration)))
        await self._stop.wait()
        self.stream.stop()
        self.pm.stop()
        for t in list(self._side_tasks):
            t.cancel()
        await asyncio.wait(tasks, timeout=10)
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        final = self.status()
        self._write_status()
        self.store.event("session_end", {"status": final})
        self.store.close()
        return final

    async def _stop_after(self, s: float) -> None:
        await self._sleep(s)
        self.stop()


def _top_reasons(reasons: Dict[int, str], n: int = 8) -> Dict[str, int]:
    c: Dict[str, int] = {}
    for r in reasons.values():
        k = r.split(" (")[0][:80]
        c[k] = c.get(k, 0) + 1
    return dict(sorted(c.items(), key=lambda kv: -kv[1])[:n])


def _p(xs: List[float], q: float) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    return round(s[min(len(s) - 1, int(q * len(s)))], 1)
