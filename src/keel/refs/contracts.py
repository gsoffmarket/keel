"""What each Outcome market pays on, in venue-neutral terms, so it can be matched.

Supported (Outcome templates seen live on 2026-11-24):
- soccer match result questions (`sportsContestResult`): team-win and draw outcomes
- two-way match winners (`sportsContestWinner`, e.g. NBA, overtime included)
- tournament winner questions (`sportsTournamentWinner`)
Anything else is reported as unsupported rather than guessed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from keel.hl.metadata import Meta, Outcome, Question


@dataclass(frozen=True)
class Contract:
    outcome: int
    coin: str
    kind: str  # team_win | draw | match_winner_side0 | tournament_winner | unsupported
    competition: str = ""
    sport: str = ""
    team: str = ""  # the team this outcome pays on (team_win, tournament_winner, match_winner_side0)
    team_a: str = ""
    team_b: str = ""
    start_ns: Optional[int] = None
    counted_play: str = ""
    season: str = ""
    short_a: str = ""
    short_b: str = ""
    reason: str = ""


def parse_start(s: str) -> Optional[int]:
    """'20261125-2000' (UTC) -> ns."""
    try:
        d = datetime.strptime(s, "%Y%m%d-%H%M").replace(tzinfo=timezone.utc)
        return int(d.timestamp()) * 10**9
    except (ValueError, TypeError):
        return None


def contracts_from_meta(meta: Meta, venues: Optional[List[str]] = None) -> List[Contract]:
    venues = venues or ["out"]
    out: List[Contract] = []
    for o in meta.outcomes.values():
        if o.venue not in venues or o.category != "sports":
            continue
        q: Optional[Question] = meta.questions.get(o.question) if o.question is not None else None
        out.append(_contract(o, q))
    return out


def _contract(o: Outcome, q: Optional[Question]) -> Contract:
    base = dict(outcome=o.outcome, coin=o.coin0)
    if q is not None and q.name == "template:sportsContestResult":
        f = q.fields
        common = dict(competition=f.get("competition", ""), sport=f.get("sport", ""), team_a=f.get("participantA", ""), team_b=f.get("participantB", ""), start_ns=parse_start(f.get("scheduledStart", "")), counted_play=f.get("countedPlay", ""), season=f.get("season", ""))
        if o.role == "fallback":
            return Contract(**base, kind="unsupported", reason="fallback outcome has no reference equivalent", **common)
        if "Draw" in o.name:
            return Contract(**base, kind="draw", **common)
        team = o.fields.get("participant", "")
        if team:
            return Contract(**base, kind="team_win", team=team, **common)
        return Contract(**base, kind="unsupported", reason=f"unrecognised named outcome {o.name}", **common)
    if o.name == "template:sportsContestWinner":
        f = o.fields
        # side 0 pays if side_names[0] wins; side names are the participants
        # side names are templates ("template:{shortNameA}"): side 0 is participant A
        s0 = o.side_names[0] if o.side_names else ""
        if s0.startswith("template:") or s0 in ("", "Yes"):
            side0 = f.get("participantA", "")
        else:
            side0 = s0
        return Contract(**base, kind="match_winner_side0", competition=f.get("competition", ""), sport=f.get("sport", ""), team=side0, team_a=f.get("participantA", ""), team_b=f.get("participantB", ""), start_ns=parse_start(f.get("scheduledStart", "")), counted_play=f.get("countedPlay", ""), season=f.get("season", ""), short_a=f.get("shortNameA", ""), short_b=f.get("shortNameB", ""))
    if q is not None and q.name == "template:sportsTournamentWinner":
        f = q.fields
        if o.role == "fallback":
            return Contract(**base, kind="unsupported", reason="fallback outcome has no reference equivalent", competition=f.get("competition", ""))
        return Contract(**base, kind="tournament_winner", competition=f.get("competition", "").strip(), sport=f.get("sport", ""), team=o.fields.get("participant", ""), season=f.get("season", ""))
    return Contract(**base, kind="unsupported", reason=f"template {o.name} not supported for matching")
