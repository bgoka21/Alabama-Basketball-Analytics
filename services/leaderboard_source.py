from __future__ import annotations

"""Shared leaderboard data access helpers."""

from functools import lru_cache
from importlib import import_module
from typing import Any, Dict, Iterable, List, Mapping, Optional

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


def _load_roster_numbers(season_id: Optional[int]) -> Dict[str, Any]:
    if season_id is None:
        return {}
    query = (
        db.session.query(Roster.player_name, Roster.jersey_number)
        .filter(Roster.season_id == season_id)
    )
    return {name: number for name, number in query.all()}


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
            numeric_value = entry.get(primary_key)

        row: Dict[str, Any] = {
            "player_name": player_name,
        }
        jersey = roster_numbers.get(player_name)
        if jersey not in (None, ""):
            row["player_number"] = jersey

        if numeric_value is not None:
            row["value"] = numeric_value
            row[stat_key] = numeric_value
        elif stat_key in entry:
            numeric_value = entry.get(stat_key)
            row["value"] = numeric_value
        else:
            row["value"] = None

        prepared.append(row)

    return prepared
