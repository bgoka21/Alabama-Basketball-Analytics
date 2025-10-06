"""Leaderboard cache utilities.

This module persists pre-formatted leaderboard payloads in the
``cached_leaderboards`` table so the application can serve cached tables
without recomputing expensive aggregates on every request.

Key helpers:

``cache_get_leaderboard``
    Return a cached payload for ``(season_id, stat_key)`` or ``None``.

``cache_build_one``
    Compute, format, and persist a single leaderboard payload.

``cache_build_all``
    Build payloads for every stat key for a season.

``format_leaderboard_payload``
    Normalize compute results into the table payload structure used by the
    templates. Internally this leverages ``admin._leaderboard_helpers`` so we
    keep one source of truth for column definitions and formatting.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from constants import LEADERBOARD_STAT_KEYS
from models.database import CachedLeaderboard, db

logger = logging.getLogger(__name__)

# Default freshness window for cached payloads. When a cache entry is older than
# this window, ``maybe_schedule_refresh`` will trigger a rebuild the next time a
# user hits the leaderboard API.
_CACHE_TTL = timedelta(hours=6)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _json_default(value: Any) -> Any:
    """Return JSON-serialisable representations of supported types."""

    if isinstance(value, datetime):
        return _isoformat(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serialisable")


def _import_leaderboard_builder():
    """Import the leaderboard table builder lazily to avoid cycles."""

    from admin._leaderboard_helpers import build_leaderboard_table

    return build_leaderboard_table


def _import_compute_leaderboard():
    """Return the default compute function used to populate payloads."""

    from admin.routes import build_leaderboard_cache_payload

    return build_leaderboard_cache_payload


def _normalize_compute_result(result: Any) -> tuple[dict[str, Any] | None, Any, Any]:
    """Return ``(config, rows, team_totals, extra)`` from a compute function."""

    variant = None

    if isinstance(result, Mapping):
        cfg = result.get("config")
        rows = result.get("rows")
        totals = result.get("team_totals")
        variant = result.get("variant")
        return cfg, rows, totals, variant

    if isinstance(result, (list, tuple)):
        if len(result) == 4:
            cfg, rows, totals, variant = result
            return cfg, rows, totals, variant
        if len(result) == 3:
            cfg, rows, totals = result
            return cfg, rows, totals, variant

    raise TypeError(
        "Leaderboard compute functions must return a mapping with"
        " 'config', 'rows', and 'team_totals' keys or a tuple"
        " (config, rows, team_totals)."
    )


def format_leaderboard_payload(
    stat_key: str,
    compute_result: Any,
    *,
    season_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a serialisable payload for the given compute result."""

    cfg, rows, team_totals, variant = _normalize_compute_result(compute_result)
    builder = _import_leaderboard_builder()

    table = builder(
        config=cfg,
        rows=rows,
        team_totals=team_totals,
        table_id=f"leaderboard-{stat_key}",
    )

    payload: Dict[str, Any] = {
        "stat_key": stat_key,
        "season_id": season_id,
        "columns": table.get("columns", []),
        "column_keys": [col.get("key") for col in table.get("columns", [])],
        "rows": table.get("rows", []),
        "totals": table.get("totals"),
        "default_sort": table.get("default_sort"),
        "has_data": table.get("has_data", False),
        "table_id": table.get("id"),
        "last_built_at": _isoformat(_utcnow()),
    }

    if variant is not None:
        payload["variant"] = variant

    return payload


def _save_payload(season_id: int, stat_key: str, payload: Mapping[str, Any]) -> None:
    """Persist ``payload`` into the ``cached_leaderboards`` table."""

    json_payload = json.dumps(payload, default=_json_default)
    entry = CachedLeaderboard.query.filter_by(season_id=season_id, stat_key=stat_key).first()
    if entry is None:
        entry = CachedLeaderboard(season_id=season_id, stat_key=stat_key, payload_json=json_payload)
        db.session.add(entry)
    else:
        entry.payload_json = json_payload


def cache_get_leaderboard(season_id: int, stat_key: str) -> Optional[Dict[str, Any]]:
    """Return cached payload for ``(season_id, stat_key)`` if present."""

    if season_id is None:
        return None

    entry = CachedLeaderboard.query.filter_by(season_id=season_id, stat_key=stat_key).first()
    if not entry:
        return None

    try:
        payload = json.loads(entry.payload_json)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        logger.warning(
            "Failed to decode cached leaderboard payload for season %s stat %s; ignoring.",
            season_id,
            stat_key,
        )
    return None


def cache_build_one(
    stat_key: str,
    season_id: int,
    compute_fn: Callable[[str, int], Any],
    *,
    commit: bool = True,
) -> Dict[str, Any]:
    """Compute and persist a single leaderboard payload."""

    if season_id is None:
        raise ValueError("season_id is required to build leaderboard cache")

    logger.info("Building leaderboard cache for stat=%s season=%s", stat_key, season_id)

    compute_result = compute_fn(stat_key, season_id)
    payload = format_leaderboard_payload(stat_key, compute_result, season_id=season_id)

    try:
        _save_payload(season_id, stat_key, payload)
        if commit:
            db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("Failed to persist leaderboard cache for stat=%s season=%s", stat_key, season_id)
        raise

    return payload


def cache_build_all(
    season_id: int,
    compute_fn: Optional[Callable[[str, int], Any]] = None,
    stat_keys: Optional[Iterable[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Build cache entries for ``stat_keys`` (defaults to all keys)."""

    if season_id is None:
        raise ValueError("season_id is required to rebuild leaderboards")

    compute = compute_fn or _import_compute_leaderboard()
    keys = list(stat_keys or LEADERBOARD_STAT_KEYS)

    logger.info(
        "Rebuilding %s leaderboard caches for season %s", len(keys), season_id
    )

    built: Dict[str, Dict[str, Any]] = {}
    try:
        for key in keys:
            payload = cache_build_one(key, season_id, compute, commit=False)
            built[key] = payload
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.exception("Failed to rebuild leaderboard caches for season %s", season_id)
        raise

    return built


def rebuild_leaderboards_for_season(season_id: int) -> Dict[str, Dict[str, Any]]:
    """Compatibility helper used by CLI commands."""

    return cache_build_all(season_id)


def rebuild_leaderboards_after_parse(season_id: Optional[int]) -> Dict[str, Dict[str, Any]]:
    """Backwards-compatible helper invoked by older parsers.

    Parsing now completes without automatically rebuilding leaderboards, but we
    keep the function so legacy code can opt-in by calling it explicitly.
    """

    if season_id is None:
        logger.info("Skipping leaderboard rebuild after parse; season_id is None")
        return {}

    return cache_build_all(season_id)


def _parse_iso_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def maybe_schedule_refresh(
    stat_key: str,
    season_id: int,
    variant: Optional[str],
    last_built_at: Optional[str],
) -> None:
    """Trigger a refresh when the cached payload is considered stale."""

    built_dt = _parse_iso_timestamp(last_built_at)
    if built_dt is None:
        logger.info(
            "Cache for stat=%s season=%s missing timestamp; scheduling refresh immediately.",
            stat_key,
            season_id,
        )
        schedule_refresh(stat_key, season_id)
        return

    age = _utcnow() - built_dt.astimezone(timezone.utc)
    if age >= _CACHE_TTL:
        logger.info(
            "Cache for stat=%s season=%s is %s old (variant=%s); scheduling refresh.",
            stat_key,
            season_id,
            age,
            variant,
        )
        schedule_refresh(stat_key, season_id)


def schedule_refresh(stat_key: str, season_id: int) -> None:
    """Rebuild a single cache entry synchronously."""

    compute = _import_compute_leaderboard()
    try:
        cache_build_one(stat_key, season_id, compute)
    except Exception:
        logger.exception(
            "Failed to refresh leaderboard cache for stat=%s season=%s", stat_key, season_id
        )
        raise


def expand_cached_rows_for_template(payload: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], Optional[dict[str, Any]]]:
    """Return ``(columns, column_keys, rows, totals)`` for template rendering."""

    columns = list(payload.get("columns", []))
    column_keys = list(payload.get("column_keys") or [col.get("key") for col in columns])
    rows_raw = payload.get("rows") or []
    totals_raw = payload.get("totals")

    rows: list[dict[str, Any]] = []
    for row in rows_raw:
        if isinstance(row, Mapping):
            rows.append(dict(row))
        else:
            rows.append({key: value for key, value in zip(column_keys, row)})

    if isinstance(totals_raw, Mapping):
        totals = dict(totals_raw)
    elif totals_raw is None:
        totals = None
    else:
        totals = {key: value for key, value in zip(column_keys, totals_raw)}

    return columns, column_keys, rows, totals


__all__ = [
    "LEADERBOARD_STAT_KEYS",
    "cache_build_all",
    "cache_build_one",
    "cache_get_leaderboard",
    "expand_cached_rows_for_template",
    "format_leaderboard_payload",
    "maybe_schedule_refresh",
    "schedule_refresh",
    "rebuild_leaderboards_after_parse",
    "rebuild_leaderboards_for_season",
    "_import_compute_leaderboard",
]

