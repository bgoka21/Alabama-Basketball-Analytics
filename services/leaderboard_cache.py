"""Caching helpers for simple leaderboard dropdown tables.

This module normalises leaderboard query results into the compact row
structure expected by the lightweight leaderboard templates. Cached payloads
include a ``schema_version`` so format changes can invalidate stale entries
automatically. When Flask-Caching is unavailable the module falls back to an
in-memory cache, which is primarily used for unit tests.
"""

from __future__ import annotations

import importlib
import logging
from datetime import datetime
from decimal import Decimal
from time import perf_counter
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2
CACHE_TTL = 60 * 15  # 15 minutes

PLAYER_NUMBER_KEYS = ("player_number", "jersey", "jersey_number", "number", "num")
PLAYER_NAME_KEYS = ("player_name", "player", "name")
VALUE_KEYS = ("value", "stat_value", "metric_value")

# Keys that should be formatted as percentages even though they may not end in
# ``_pct``.
PERCENT_KEYS = {
    "efg_on",
    "efg_off",
    "turnover_rate",
    "off_reb_rate",
    "individual_turnover_rate",
    "bamalytics_turnover_rate",
    "individual_team_turnover_pct",
    "fouls_drawn_rate",
}

# Keys that are rates/averages (no percent sign) and should be rounded to one
# decimal place.
RATE_KEYS = {
    "ppp_on",
    "ppp_off",
    "assist_turnover_ratio",
    "adj_assist_turnover_ratio",
    "assist_rate",
    "def_rating",
    "off_rating",
    "offensive_rating",
    "defensive_rating",
}


class _InMemoryCache:
    """A very small stand-in for Flask-Caching in test environments."""

    def __init__(self) -> None:
        self._store: MutableMapping[str, Any] = {}

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def set(self, key: str, value: Any, timeout: int | None = None) -> None:  # pragma: no cover - timeout unused in stub
        self._store[key] = value

    def delete(self, key: str) -> None:
        self._store.pop(key, None)


def _load_cache_backend() -> Any:  # pragma: no cover - depends on application
    module_spec = importlib.util.find_spec("app.extensions")
    if module_spec is not None:
        module = importlib.import_module("app.extensions")
        found = getattr(module, "cache", None)
        if found is not None:
            return found
    return _InMemoryCache()


cache = _load_cache_backend()


def _coerce_numeric(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coalesce(row: Mapping[str, Any], keys: Sequence[str], *, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return default


def _row_value(row: Any, *, stat_key: str) -> Any:
    if isinstance(row, Mapping):
        if stat_key in row and row[stat_key] not in (None, ""):
            return row[stat_key]
        return _coalesce(row, VALUE_KEYS)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
        if len(row) >= 3:
            return row[2]
    return None


def _row_player_number(row: Any) -> Any:
    if isinstance(row, Mapping):
        return _coalesce(row, PLAYER_NUMBER_KEYS)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
        return row[0] if row else None
    return None


def _row_player_name(row: Any) -> Any:
    if isinstance(row, Mapping):
        return _coalesce(row, PLAYER_NAME_KEYS)
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
        return row[1] if len(row) >= 2 else None
    return None


def format_stat_value(stat_key: str, raw: Any) -> str:
    """Return a template-ready string for ``raw``.

    * Integers display without decimals ("838").
    * Percentages have one decimal place and a trailing ``%``.
    * Rates/averages have one decimal place without ``%``.
    """

    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, bool):  # pragma: no cover - defensive
        raw = int(raw)
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, Decimal):
        value = float(raw)
    elif isinstance(raw, float):
        value = raw
    else:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return str(raw)

    if stat_key.endswith("_pct") or stat_key in PERCENT_KEYS:
        text = f"{round(value, 1):.1f}%"
        return text

    if stat_key in RATE_KEYS:
        return f"{round(value, 1):.1f}"

    if float(value).is_integer():
        return str(int(round(value)))

    text = f"{round(value, 1):.1f}"
    if text.endswith(".0"):
        text = text[:-2]
    return text


def format_rows(stat_key: str, raw_rows: Iterable[Any]) -> list[dict[str, Any]]:
    """Convert ``raw_rows`` into template-ready leaderboard rows."""

    prepared: list[dict[str, Any]] = []
    for row in raw_rows or []:
        player_name = _row_player_name(row)
        player_number = _row_player_number(row)
        raw_value = _row_value(row, stat_key=stat_key)
        numeric_value = _coerce_numeric(raw_value)

        if player_name is None and player_number is None:
            continue

        number_text = str(player_number).strip() if player_number is not None else ""
        if number_text.startswith("#"):
            number_text = number_text[1:]
        display_name = str(player_name).strip() if player_name is not None else ""
        if number_text:
            display_player = f"#{number_text} {display_name}".strip()
        else:
            display_player = display_name

        prepared.append(
            {
                "player": display_player,
                "value": format_stat_value(stat_key, raw_value),
                "value_sort": numeric_value,
            }
        )

    prepared.sort(key=lambda row: row["value_sort"], reverse=True)

    for idx, row in enumerate(prepared, start=1):
        row["rank"] = str(idx)

    return prepared


def query_stat_rows(stat_key: str, season_id: int) -> Iterable[Any]:  # pragma: no cover - application-specific
    """Fetch raw leaderboard rows for ``stat_key``.

    Applications should override this with their actual query implementation.
    """

    raise NotImplementedError("query_stat_rows must be provided by the application")


def build_leaderboard_cache(stat_key: str, season_id: int) -> dict[str, Any]:
    """Build and store the cached payload for ``stat_key``."""

    start = perf_counter()
    raw_rows = query_stat_rows(stat_key, season_id) or []
    rows = format_rows(stat_key, raw_rows)
    built_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stat_key": stat_key,
        "season_id": season_id,
        "rows": rows,
        "built_at": built_at,
    }

    cache_key = f"leaderboard:{SCHEMA_VERSION}:{season_id}:{stat_key}"
    cache.set(cache_key, payload, timeout=CACHE_TTL)
    cache.delete(f"leaderboard:{season_id}:{stat_key}")

    duration = perf_counter() - start
    logger.info(
        "Built leaderboard cache stat=%s season=%s rows=%s in %.3fs",
        stat_key,
        season_id,
        len(rows),
        duration,
    )

    return payload


def get_leaderboard_payload(stat_key: str, season_id: int) -> dict[str, Any]:
    """Return the cached payload, rebuilding if necessary."""

    cache_keys = [
        f"leaderboard:{SCHEMA_VERSION}:{season_id}:{stat_key}",
        f"leaderboard:{season_id}:{stat_key}",
    ]

    payload: dict[str, Any] | None = None
    for key in cache_keys:
        cached = cache.get(key)
        if not cached:
            continue
        if cached.get("schema_version") != SCHEMA_VERSION:
            cache.delete(key)
            continue
        payload = cached
        break

    if payload is None:
        logger.info(
            "Leaderboard cache miss (stat=%s season=%s schema_version=%s)",
            stat_key,
            season_id,
            SCHEMA_VERSION,
        )
        payload = build_leaderboard_cache(stat_key, season_id)
    else:
        logger.info(
            "Leaderboard cache hit (stat=%s season=%s schema_version=%s)",
            stat_key,
            season_id,
            SCHEMA_VERSION,
        )

    return payload
