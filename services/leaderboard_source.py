from __future__ import annotations

"""Shared leaderboard data access helpers."""

from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, Iterable, List, Mapping, Optional

from flask import current_app
from sqlalchemy import inspect

from models.database import Roster, db


@lru_cache(maxsize=1)
def _load_compute_leaderboard():
    """Return the compute function without creating import cycles."""

    module = import_module("admin.routes")
    compute = getattr(module, "compute_leaderboard", None)
    if compute is None:
        raise AttributeError("admin.routes.compute_leaderboard is required")
    return compute


@lru_cache(maxsize=1)
def _load_leaderboard_builder():
    module = import_module("admin._leaderboard_helpers")
    builder = getattr(module, "build_leaderboard_table", None)
    if builder is None:
        raise AttributeError("admin._leaderboard_helpers.build_leaderboard_table is required")
    return builder


def _resolve_primary_column(table_payload: Mapping[str, Any]) -> tuple[Optional[str], Optional[str]]:
    """Return ``(column_key, value_key)`` for the primary leaderboard metric."""

    columns: List[Mapping[str, Any]] = list(table_payload.get("columns") or [])
    default_sort = str(table_payload.get("default_sort") or "")
    primary_key: Optional[str] = None
    for part in default_sort.split(";"):
        segment = part.strip()
        if not segment:
            continue
        key = segment.split(":", 1)[0].strip()
        if key and key not in {"rank", "player"}:
            primary_key = key
            break

    value_key: Optional[str] = None
    if primary_key:
        for column in columns:
            if column.get("key") == primary_key:
                value_key = column.get("value_key")
                break

    if value_key is None:
        for column in columns:
            key = column.get("key")
            if key and key not in {"rank", "player"}:
                primary_key = key
                value_key = column.get("value_key")
                if value_key:
                    break

    return primary_key, value_key


def _pick_roster_number_column() -> Optional[Any]:
    """
    Return a Roster column suitable for jersey numbers, or None if not found.
    Tries common names, then inspects mapped attributes for anything containing
    'jersey' or 'number'.
    """

    # Common field names first
    for cand in ("jersey_number", "number", "uniform_number", "player_number"):
        if hasattr(Roster, cand):
            return getattr(Roster, cand)
    # Fallback: inspect mapped attributes
    try:
        mapper = inspect(Roster)
        for attr in getattr(mapper, "attrs", []):
            key = getattr(attr, "key", "")
            if not key:
                continue
            low = key.lower()
            if "jersey" in low or "number" in low:
                return getattr(Roster, key, None)
    except Exception:
        pass
    return None


def _coerce_num_text(num: Any) -> str:
    """Return a clean jersey number string like '12' (not '12.0')."""

    if num is None:
        return ""
    try:
        f = float(num)
        if f.is_integer():
            return str(int(f))
        s = str(f)
        if s.endswith(".0"):
            s = s[:-2]
        return s
    except Exception:
        return str(num).strip()


def _load_roster_numbers(season_id: Optional[int]) -> Dict[str, Any]:
    """
    Returns {player_name: jersey_number_text}. If there is no number-like column
    on Roster, logs a warning and returns an empty mapping.
    """

    if season_id is None:
        return {}
    num_col = _pick_roster_number_column()
    if not num_col:
        current_app.logger.warning(
            "No jersey-number-like column on Roster; proceeding without numbers."
        )
        return {}
    query = (
        db.session.query(Roster.player_name, num_col)
        .filter(Roster.season_id == season_id)
    )
    mapping: Dict[str, Any] = {}
    for name, num in query.all():
        if not name:
            continue
        text = _coerce_num_text(num)
        if text:
            mapping[name] = text
    return mapping


def fetch_stat_rows(stat_key: str, season_id: int) -> Iterable[Dict[str, Any]]:
    """Return raw leaderboard rows for ``stat_key`` and ``season_id``.

    The rows match the season leaderboard view's underlying data and include
    identifiers plus a numeric stat value suitable for cache normalisation.
    """

    compute = _load_compute_leaderboard()
    builder = _load_leaderboard_builder()

    cfg, rows, team_totals = compute(stat_key, season_id)
    table_payload = builder(
        config=cfg,
        rows=rows,
        team_totals=team_totals,
        table_id=f"leaderboard-{stat_key}",
    )

    primary_key, value_key = _resolve_primary_column(table_payload)
    roster_numbers = _load_roster_numbers(season_id)

    prepared: List[Dict[str, Any]] = []
    for entry in table_payload.get("rows", []):
        if not isinstance(entry, Mapping):
            continue
        player_name = entry.get("player")
        if not player_name:
            continue

        numeric_value: Any = None
        if value_key:
            numeric_value = entry.get(value_key)
        if numeric_value is None and primary_key:
            numeric_value = entry.get(f"{primary_key}_value")
        if numeric_value is None and primary_key:
            numeric_value = entry.get(primary_key)

        row: Dict[str, Any] = {
            "player_name": player_name,
            "player": player_name,
        }
        jersey = roster_numbers.get(player_name)
        if jersey not in (None, ""):
            row["player_number"] = jersey
            row["number"] = jersey

        if numeric_value is not None:
            row["value"] = numeric_value
            row[stat_key] = numeric_value
        elif stat_key in entry:
            fallback = entry.get(stat_key)
            cleaned = None
            if isinstance(fallback, (int, float)):
                cleaned = float(fallback)
            elif isinstance(fallback, str):
                text = fallback.strip()
                if text.endswith("%"):
                    text = text[:-1]
                try:
                    cleaned = float(text)
                except (TypeError, ValueError):
                    cleaned = None
            if cleaned is not None:
                row["value"] = cleaned
                row[stat_key] = cleaned
            else:
                row["value"] = None
        else:
            row["value"] = None

        prepared.append(row)

    return prepared
