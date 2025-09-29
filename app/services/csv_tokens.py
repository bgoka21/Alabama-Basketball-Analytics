import re
from typing import Iterable, Tuple

# Normalize common dash variants to ASCII hyphen for consistent matching
_DASHES = {
    "\u2012": "-",  # figure dash
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u2212": "-",  # minus sign
}


def _normalize(s: str) -> str:
    if not s:
        return ""
    s = str(s)
    for d, repl in _DASHES.items():
        s = s.replace(d, repl)
    return " ".join(s.split())  # collapse weird spaces


# Compile patterns once (case-insensitive, tolerate extra spaces)
PAT_BUMP_PLUS = re.compile(r"\bBump\s*\+(?=\W|$)", re.IGNORECASE)
PAT_BUMP_MINUS = re.compile(r"\bBump\s*-(?=\W|$)", re.IGNORECASE)
PAT_LOW_MAN_PLUS = re.compile(r"\bLow\s*Man\s*\+(?=\W|$)", re.IGNORECASE)
PAT_LOW_MAN_MINUS = re.compile(r"\bLow\s*Man\s*-(?=\W|$)", re.IGNORECASE)


def count_bump_tokens_in_cells(cells: Iterable[str]) -> Tuple[int, int]:
    """
    Count bump + and bump - occurrences across an iterable of cell strings.
    We count each match occurrence (if a cell contains 'Bump +' twice, count 2).
    """
    plus = minus = 0
    for raw in cells:
        s = _normalize(raw)
        if not s:
            continue
        plus += len(PAT_BUMP_PLUS.findall(s))
        minus += len(PAT_BUMP_MINUS.findall(s))
    return plus, minus


def count_low_man_tokens_in_cells(cells: Iterable[str]) -> Tuple[int, int]:
    """Count Low Man + and Low Man - occurrences across iterable cell strings."""
    plus = minus = 0
    for raw in cells:
        s = _normalize(raw)
        if not s:
            continue
        plus += len(PAT_LOW_MAN_PLUS.findall(s))
        minus += len(PAT_LOW_MAN_MINUS.findall(s))
    return plus, minus
