"""Team/participant name comparison for cross-venue matching."""
from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Set

# Words that carry no identity ("FC", "SK", "Club"...). Kept small and explicit: removing
# too much merges different clubs (e.g. "Real Madrid" vs "Real Betis").
STOP = {"fc", "cf", "sc", "ac", "afc", "club", "de", "the", "sk", "fk", "bv", "vs", "and", "kv", "ssc", "cp", "gnk", "pfk", "rc", "as", "osc", "sfp", "fk", "if", "bk", "calcio", "futbol", "football", "balompie"}

# Known aliases where token overlap is not enough. Extend as mismatches are found;
# every alias is a claim that two names denote the same team.
ALIASES = {
    "inter milan": "internazionale milano",
    "inter": "internazionale",
    "psg": "paris saint germain",
    "olympiacos": "olympiakos",
    "bayern munich": "bayern munchen",
    "man city": "manchester city",
    "man united": "manchester united",
    "spurs": "tottenham hotspur",
    "losc lille": "lille",
}


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("ø", "o").replace("Ø", "o").replace("ł", "l").replace("ß", "ss")
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    s = re.sub(r"\s+", " ", s).strip()
    return ALIASES.get(s, s)


def tokens(s: str) -> Set[str]:
    out = set()
    for w in norm(s).split(" "):
        if len(w) > 1 and w not in STOP and not w.isdigit():
            out.add(ALIASES.get(w, w))
    return out


def _lev1(a: str, b: str) -> bool:
    """True if edit distance <= 1."""
    if abs(len(a) - len(b)) > 1:
        return False
    if len(a) > len(b):
        a, b = b, a
    i = j = diff = 0
    while i < len(a) and j < len(b):
        if a[i] == b[j]:
            i += 1
            j += 1
            continue
        diff += 1
        if diff > 1:
            return False
        if len(a) == len(b):
            i += 1
        j += 1
    return diff + (len(b) - j) <= 1


def token_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if len(a) >= 4 and len(b) >= 4 and (a.startswith(b) or b.startswith(a)):
        return True
    return len(a) >= 6 and len(b) >= 6 and _lev1(a, b)


def coverage(name: str, text: str) -> float:
    """Share of `name`'s identifying tokens found in `text` (0..1)."""
    want = tokens(name)
    have = tokens(text)
    if not want:
        return 0.0
    hit = sum(1 for w in want if any(token_match(w, h) for h in have))
    return hit / len(want)


def best_coverage(name: str, texts: Iterable[str]) -> float:
    return max((coverage(name, t) for t in texts), default=0.0)
