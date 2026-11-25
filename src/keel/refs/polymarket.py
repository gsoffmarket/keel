"""Polymarket reference matching (Gamma API, public, read-only).

Matching is deliberately conservative: a mapping needs both teams (or the competition
and team) in the right event, a start time that agrees, and compatible resolution
rules. Every mapping carries a confidence score and its components; nothing here is
"signed" (the PRD requires George's sign-off before any mapping can inform trading).
"""
from __future__ import annotations

import json
import re
import time
import urllib.parse
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from keel import net
from keel.config import HTTP_HEADERS
from keel.refs.contracts import Contract
from keel.refs.names import coverage

GAMMA = "https://gamma-api.polymarket.com"

# Polymarket's own names for competitions whose Outcome names differ (found in the live
# run of 2026-11-24: "EPL: 2027 Champion", "Pro Football: 2027 Champion").
COMPETITION_ALIASES = {
    "english premier league": ["EPL", "Premier League"],
    "usa national football league (nfl)": ["Pro Football", "NFL"],
    "national football league": ["Pro Football", "NFL"],
    "nba": ["NBA"],
    "uefa champions league": ["UEFA Champions League", "Champions League", "UCL"],
    "uefa europa league": ["UEFA Europa League", "Europa League", "UEL"],
}


def competition_names(c: str) -> List[str]:
    key = (c or "").strip().lower()
    return [c.strip()] + COMPETITION_ALIASES.get(key, [])


def nickname(team: str) -> str:
    """Last word of a club name ("Philadelphia 76ers" -> "76ers"); Polymarket titles US
    games by nickname ("Heat vs. 76ers")."""
    parts = (team or "").split()
    return parts[-1] if parts else ""
HIGH, MEDIUM = 0.90, 0.75
# Side markets of a game ("X vs. Y - Exact Score", "- Player Props", ...). Esports main
# events also use " - " ("Counter-Strike: A vs B (BO1) - PGL Major ..."), so only these
# suffixes mark a derivative.
_DERIVATIVE = re.compile(r" - (exact score|halftime|first team|player props|total corners|total goals|both teams|correct score|more markets|map \d|.*props|.*handicap|.*totals?)\b", re.I)


@dataclass
class Mapping:
    outcome: int
    coin: str
    kind: str
    venue: str
    event_slug: str
    event_title: str
    market_question: str
    condition_id: str
    token_id: str
    token_outcome: str
    confidence: float
    level: str  # high | medium
    components: Dict[str, Any]
    matched_ns: int
    signed: bool = False

    def key(self) -> Tuple[int, str]:
        return (self.outcome, self.token_id)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["contract_kind"] = d.pop("kind")  # "kind" is reserved for the event type in the log
        return d


def _json_list(v: Any) -> List[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    try:
        return [str(x) for x in json.loads(v or "[]")]
    except (ValueError, TypeError):
        return []


def _ts(s: Optional[str]) -> Optional[int]:
    """Polymarket times: '2026-11-24 20:00:00+00' or ISO '2026-11-24T20:00:00Z'."""
    if not s:
        return None
    s = s.strip().replace("Z", "+00:00")
    if re.match(r".*[+-]\d\d$", s):
        s += ":00"
    s = s.replace(" ", "T", 1)
    try:
        return int(datetime.fromisoformat(s).astimezone(timezone.utc).timestamp()) * 10**9
    except ValueError:
        return None


def _time_score(contract_ns: Optional[int], market: Dict[str, Any], event: Dict[str, Any]) -> Tuple[float, Optional[float]]:
    ref = _ts(market.get("gameStartTime")) or _ts(event.get("startTime")) or _ts(event.get("endDate"))
    if contract_ns is None or ref is None:
        return 0.5, None
    diff_h = abs(ref - contract_ns) / 3.6e12
    if diff_h <= 0.25:
        return 1.0, diff_h
    if diff_h <= 3:
        return 0.6, diff_h
    return 0.0, diff_h


def _rules_score(c: Contract, market: Dict[str, Any], event: Dict[str, Any]) -> Tuple[float, str]:
    text = ((market.get("description") or "") + " " + (event.get("description") or "")).lower()
    cp = c.counted_play.lower()
    if c.kind in ("team_win", "draw"):
        if "regulation" in cp or "90 minutes" in cp:
            if re.search(r"90 minutes|regular time|regulation", text):
                return 1.0, "both settle on 90 minutes plus stoppage time"
            if re.search(r"extra time|penalt|overtime", text):
                return 0.0, "reference appears to count extra time"
            return 0.5, "reference rule text not found"
    if c.kind == "match_winner_side0":
        if "overtime" in cp:
            if "overtime" in text or "ot " in text:
                if re.search(r"\btie\b|50-50", text) and "tie" not in cp:
                    # e.g. NFL: Polymarket resolves a tie 50-50; Outcome's tie handling is not stated in its fields
                    return 0.5, "both include overtime; tie handling differs or is unverified"
                return 1.0, "both include overtime"
            return 0.5, "reference overtime rule not found"
    if c.kind == "tournament_winner":
        return (0.8, "tournament winner; rule text not compared")
    return 0.5, "rules not compared"


def _yes_token(m: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    outs, toks = _json_list(m.get("outcomes")), _json_list(m.get("clobTokenIds"))
    for o, t in zip(outs, toks):
        if o.strip().lower() == "yes":
            return t, o
    return None


def _primary_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [e for e in events if not _DERIVATIVE.search(e.get("title") or "") and e.get("active", True) and not e.get("closed", False)]


def match_contract(c: Contract, events: List[Dict[str, Any]], now_ns: Optional[int] = None) -> Tuple[Optional[Mapping], str]:
    """Best Polymarket mapping for one Outcome contract among candidate events,
    or (None, reason)."""
    now_ns = now_ns or time.time_ns()
    best: Optional[Mapping] = None
    reason = "no candidate event"
    for e in _primary_events(events):
        title = e.get("title") or ""
        markets = e.get("markets") or []
        if c.kind in ("team_win", "draw", "match_winner_side0"):
            cov_a, cov_b = coverage(c.team_a, title), coverage(c.team_b, title)
            name_event = min(cov_a, cov_b)
            slug_parts = set((e.get("slug") or "").lower().split("-"))
            slug_confirmed = False
            if c.short_a and c.short_b and c.short_a.lower() in slug_parts and c.short_b.lower() in slug_parts and name_event > 0:
                # both official short codes in the event slug (e.g. nfl-kc-buf-...) plus a name
                # word from each team in the title ("Chiefs vs. Bills"): same game
                name_event = 1.0
                slug_confirmed = True
            if name_event < 0.5:
                continue
            for m in markets:
                q = m.get("question") or ""
                if c.kind == "draw":
                    if "draw" not in q.lower():
                        continue
                    yt = _yes_token(m)
                    if not yt:
                        continue
                    tok, tout = yt
                    name = name_event
                elif c.kind == "team_win":
                    mm = re.match(r"^Will (.+?) win\b", q)
                    if not mm or "draw" in q.lower():
                        continue
                    other = c.team_b if coverage(c.team, c.team_a) >= coverage(c.team, c.team_b) else c.team_a
                    own, opp = coverage(c.team, mm.group(1)), coverage(other, mm.group(1))
                    if own < 0.5 or opp >= own:
                        continue
                    yt = _yes_token(m)
                    if not yt:
                        continue
                    tok, tout = yt
                    name = min(name_event, own)
                else:  # match_winner_side0: two-way moneyline, token of the side-0 team
                    if (m.get("sportsMarketType") or "") != "moneyline":
                        continue
                    outs, toks = _json_list(m.get("outcomes")), _json_list(m.get("clobTokenIds"))
                    scored = sorted(((coverage(c.team, o), o, t) for o, t in zip(outs, toks)), reverse=True)
                    if len(scored) != 2 or scored[0][0] < (0.3 if slug_confirmed else 0.5) or scored[1][0] >= scored[0][0]:
                        continue
                    _, tout, tok = scored[0]
                    # outcome labels are nicknames ("Heat"); the event-level score carries identity
                    name = name_event
                ts, diff_h = _time_score(c.start_ns, m, e)
                if ts == 0.0:
                    reason = f"start time differs by {diff_h:.1f} h"
                    continue
                rs, rnote = _rules_score(c, m, e)
                if rs == 0.0:
                    reason = rnote
                    continue
                conf = round(0.45 * name + 0.30 * ts + 0.25 * rs, 3)
                cand = _mk(c, e, m, tok, tout, conf, {"name": round(name, 3), "time": ts, "start_diff_h": None if diff_h is None else round(diff_h, 3), "rules": rs, "rules_note": rnote}, now_ns)
                if cand and (best is None or cand.confidence > best.confidence):
                    best = cand
        elif c.kind == "tournament_winner":
            if max(coverage(n, title) for n in competition_names(c.competition)) < 0.5 or not re.search(r"\b(champion|winner)\b", title, re.I):
                continue
            if re.search(r"phase|place|scorer|advance|most |top |runner|relegat|qualif|country", title, re.I):
                continue
            years = set(re.findall(r"20\d\d", c.season or ""))
            m2 = re.match(r"(20\d\d)\s*[/-]\s*(\d\d)$", (c.season or "").strip())
            if m2:
                years.add("20" + m2.group(2))
                years.add(f"{m2.group(1)}-{m2.group(2)}")
            texts = [title] + [mm.get("question") or "" for mm in markets[:1]]
            season_ok = 1.0 if years and any(y in t for y in years for t in texts) else 0.7
            for m in markets:
                q = m.get("question") or ""
                own = coverage(c.team, q)
                if own < 0.6:
                    continue
                yt = _yes_token(m)
                if not yt:
                    continue
                rs, rnote = _rules_score(c, m, e)
                conf = round(0.45 * own + 0.30 * season_ok + 0.25 * rs, 3)
                cand = _mk(c, e, m, yt[0], yt[1], conf, {"name": round(own, 3), "season": season_ok, "rules": rs, "rules_note": rnote}, now_ns)
                if cand and (best is None or cand.confidence > best.confidence):
                    best = cand
        else:
            return None, c.reason or "unsupported contract"
    if best is None:
        return None, reason
    if best.confidence < MEDIUM:
        return None, f"best candidate below threshold ({best.confidence})"
    return best, "matched"


def _mk(c: Contract, e: Dict[str, Any], m: Dict[str, Any], tok: str, tout: str, conf: float, comps: Dict[str, Any], now_ns: int) -> Optional[Mapping]:
    if not tok:
        return None
    return Mapping(
        outcome=c.outcome, coin=c.coin, kind=c.kind, venue="polymarket",
        event_slug=e.get("slug") or "", event_title=e.get("title") or "",
        market_question=m.get("question") or "", condition_id=m.get("conditionId") or "",
        token_id=tok, token_outcome=tout, confidence=conf, level="high" if conf >= HIGH else "medium",
        components=comps, matched_ns=now_ns,
    )


def search_queries(c: Contract) -> List[str]:
    if c.kind in ("team_win", "draw"):
        return [f"{c.team_a} {c.team_b}", c.team_a, c.team_b]
    if c.kind == "match_winner_side0":
        # full names surface season-long futures first; nicknames find the game
        return [f"{nickname(c.team_a)} {nickname(c.team_b)}", f"{c.team_a} vs {c.team_b}", f"{c.team_a} {c.team_b}"]
    if c.kind == "tournament_winner":
        out: List[str] = []
        for n in competition_names(c.competition):
            out += [f"{n} champion", f"{n} winner"]
        return out
    return []


class GammaClient:
    """Rate-limited Gamma search with a small cache, so matching stays polite."""

    def __init__(self, base: str = GAMMA, min_interval: float = 1.0, ttl: float = 1800.0, get: Callable = net.get) -> None:
        self.base = base
        self.min_interval = min_interval
        self.ttl = ttl
        self._get = get
        self._last = 0.0
        self._cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
        self.requests = 0

    def search_events(self, q: str) -> List[Dict[str, Any]]:
        now = time.monotonic()
        hit = self._cache.get(q)
        if hit and now - hit[0] < self.ttl:
            return hit[1]
        wait = self.min_interval - (now - self._last)
        if wait > 0:
            time.sleep(wait)
        url = f"{self.base}/public-search?limit_per_type=10&events_status=active&q={urllib.parse.quote(q)}"
        text, _, _ = self._get(url, HTTP_HEADERS)
        self._last = time.monotonic()
        self.requests += 1
        try:
            events = json.loads(text).get("events") or []
        except (ValueError, AttributeError):
            events = []
        self._cache[q] = (time.monotonic(), events)
        return events


def match_all(contracts: List[Contract], client: GammaClient) -> Tuple[List[Mapping], Dict[int, str]]:
    """Blocking; run in an executor. Groups contracts by query so each match
    (3 outcomes) costs one search in the common case."""
    maps: List[Mapping] = []
    reasons: Dict[int, str] = {}
    for c in contracts:
        if c.kind == "unsupported":
            reasons[c.outcome] = c.reason
            continue
        found = None
        why = "no candidate event"
        for q in search_queries(c):
            try:
                events = client.search_events(q)
            except Exception as exc:  # network: report, keep going
                why = f"search failed: {exc!r}"[:200]
                continue
            found, why = match_contract(c, events)
            if found:
                break
        if found:
            maps.append(found)
        else:
            reasons[c.outcome] = why
    return maps, reasons
