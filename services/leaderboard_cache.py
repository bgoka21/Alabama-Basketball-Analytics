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

from constants import LEADERBOARD_STAT_KEYS

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2
CACHE_TTL = 60 * 15  # 15 minutes

PLAYER_NUMBER_KEYS = ("player_number", "jersey", "jersey_number", "number", "num")
PLAYER_NAME_KEYS = ("player_name", "player", "name")
VALUE_KEYS = ("value", "stat_value", "metric_value")


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


def list_all_leaderboard_stats() -> list[str]:
    """Return the canonical ordered list of leaderboard stat keys."""

    return list(LEADERBOARD_STAT_KEYS)


_PERCENT_LIKE_KEYS = {
    "oreb_pct",
    "dreb_pct",
    "tov_pct",
    "ft_pct",
    "fg2_fg_pct",
    "fg3_fg_pct",
    "efg_on",
    "efg_off",
    "turnover_rate",
    "off_reb_rate",
    "individual_turnover_rate",
    "bamalytics_turnover_rate",
    "individual_team_turnover_pct",
    "fouls_drawn_rate",
}


def _is_percent_stat(stat_key: str) -> bool:
    return stat_key.endswith("_pct") or stat_key in _PERCENT_LIKE_KEYS


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
    """Return a display-ready string for ``raw``."""

    if raw is None:
        return "0"

    if isinstance(raw, str):
        try:
            raw_val = float(raw)
        except (TypeError, ValueError):
            return raw
    elif isinstance(raw, Decimal):
        raw_val = float(raw)
    else:
        try:
            raw_val = float(raw)
        except (TypeError, ValueError):
            return str(raw)

    if _is_percent_stat(stat_key):
        text = f"{round(raw_val, 1):.1f}%"
        if text.endswith(".0%"):
            text = text.replace(".0%", "%")
        return text

    if abs(raw_val - int(raw_val)) < 1e-9:
        return str(int(raw_val))

    text = f"{round(raw_val, 1):.1f}"
    if text.endswith(".0"):
        text = text[:-2]
    return text


def query_stat_rows(stat_key: str, season_id: int) -> Iterable[Any]:  # pragma: no cover - application-specific
    """Fetch raw leaderboard rows for ``stat_key``.

    Applications should override this with their actual query implementation.
    """

    raise NotImplementedError("query_stat_rows must be provided by the application")


def _payload_key_v2(season_id: int, stat_key: str) -> str:
    return f"leaderboard:{SCHEMA_VERSION}:{season_id}:{stat_key}"


def build_leaderboard_cache(stat_key: str, season_id: int) -> dict[str, Any]:
    """Build and store the cached payload for ``stat_key``."""

    start = perf_counter()
    raw_rows = list(query_stat_rows(stat_key, season_id) or [])

    prepared: list[dict[str, Any]] = []
    for row in raw_rows:
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
            player_display = f"#{number_text} {display_name}".strip()
        else:
            player_display = display_name

        prepared.append(
            {
                "player": player_display,
                "value": format_stat_value(stat_key, raw_value),
                "value_sort": numeric_value,
            }
        )

    prepared.sort(key=lambda row: row["value_sort"], reverse=True)

    for idx, row in enumerate(prepared, start=1):
        row["rank"] = str(idx)

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stat_key": stat_key,
        "season_id": season_id,
        "rows": prepared,
        "built_at": datetime.utcnow().isoformat() + "Z",
    }

    key_v2 = _payload_key_v2(season_id, stat_key)
    cache.set(key_v2, payload, timeout=CACHE_TTL)
    cache.delete(f"leaderboard:{season_id}:{stat_key}")

    duration = perf_counter() - start
    logger.info(
        "Built leaderboard cache stat=%s season=%s rows=%s in %.3fs",
        stat_key,
        season_id,
        len(prepared),
        duration,
    )

    return payload


def get_leaderboard_payload(stat_key: str, season_id: int) -> dict[str, Any]:
    """Return the cached payload, rebuilding if necessary."""

    key_v2 = _payload_key_v2(season_id, stat_key)
    payload = cache.get(key_v2)
    if payload and payload.get("schema_version") == SCHEMA_VERSION:
        logger.info(
            "Leaderboard cache hit (stat=%s season=%s schema_version=%s)",
            stat_key,
            season_id,
            SCHEMA_VERSION,
        )
        return payload

    legacy_key = f"leaderboard:{season_id}:{stat_key}"
    legacy_payload = cache.get(legacy_key)
    if legacy_payload:
        if legacy_payload.get("schema_version") != SCHEMA_VERSION:
            cache.delete(legacy_key)
        else:
            cache.set(key_v2, legacy_payload, timeout=CACHE_TTL)
            cache.delete(legacy_key)
            logger.info(
                "Leaderboard cache migrated legacy key stat=%s season=%s",
                stat_key,
                season_id,
            )
            return legacy_payload

    logger.info(
        "Leaderboard cache miss (stat=%s season=%s schema_version=%s)",
        stat_key,
        season_id,
        SCHEMA_VERSION,
    )
    return build_leaderboard_cache(stat_key, season_id)
