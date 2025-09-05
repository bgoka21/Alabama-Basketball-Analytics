from __future__ import annotations
import re
from typing import Any, Dict, Iterable, Optional
import pandas as pd

# Canonical header mapping: add synonyms here one time and reuse everywhere.
HEADER_SYNONYMS: Dict[str, list[str]] = {
    # identity
    "coach": ["coach", "coach name"],
    "player": ["player", "prospect", "athlete"],
    "team": ["team", "school", "program", "college"],
    "year": ["year", "recruiting year", "class year"],
    # money
    "projected_money": ["projected money", "proj money", "projected $", "proj $", "projected_amount"],
    "actual_money": ["actual money", "act money", "actual $", "act $", "actual_amount"],
    "net": ["net", "difference", "net $"],
    # picks (raw string)
    "projected_pick_raw": ["projected pick", "proj pick", "projected draft pick", "proj draft pick", "projected_pick"],
    "actual_pick_raw": ["actual pick", "draft pick", "actual draft pick", "actual_pick"],
    # optional coach context
    "coach_current_team": ["coach current team", "coach team", "coach_team", "current team"],
    "coach_current_conference": ["coach current conference", "coach conference", "coach_conf", "current conference"],
    # optional sheet conference tag if used
    "sheet": ["sheet", "conference", "sheet name"],
}

_REQUIRED_CANON = ("coach", "player", "team", "year")

_CURRENCY_RX = re.compile(r"[\,\s\$]")
_PARENS_RX = re.compile(r"^\((.*)\)$")


def _norm(s: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()


def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    inv = {}
    for canon, alts in HEADER_SYNONYMS.items():
        for a in alts:
            inv[_norm(a)] = canon
    col_map = {}
    for c in df.columns:
        key = _norm(c)
        col_map[c] = inv.get(key, c)
    return df.rename(columns=col_map)


def parse_currency(val: Any) -> Optional[float]:
    """
    Parse $, commas, spaces; treats '(123)' as -123.
    Returns float or None if blank/NA-ish.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in {"n/a", "na", "-", "null"}:
        return None
    # handle parentheses negative
    m = _PARENS_RX.match(s)
    neg = False
    if m:
        s = m.group(1)
        neg = True
    s = _CURRENCY_RX.sub("", s)
    if not s:
        return None
    try:
        x = float(s)
        return -x if neg else x
    except ValueError:
        try:
            x = float(pd.to_numeric(s, errors="coerce"))
            if pd.isna(x):
                return None
            return -x if neg else float(x)
        except Exception:
            return None


def parse_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        x = int(float(s))
        return x
    except Exception:
        x = pd.to_numeric([s], errors="coerce")[0]
        if pd.isna(x):
            return None
        return int(x)


def parse_pick_to_int(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"udfa", "ufa", "undrafted", "n/a", "na", "-"}:
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def strip_cols(df: pd.DataFrame, cols: Iterable[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()


def validate_required(df: pd.DataFrame) -> tuple[bool, list[str]]:
    missing = [c for c in _REQUIRED_CANON if c not in df.columns]
    return (len(missing) == 0, missing)
