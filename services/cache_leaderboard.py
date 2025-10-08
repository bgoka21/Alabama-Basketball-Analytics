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

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from constants import LEADERBOARD_STAT_KEYS
from models.database import CachedLeaderboard, db
from services.leaderboard_cache import (
    FORMATTER_VERSION,
    SCHEMA_VERSION,
    build_leaderboard_payload,
    delete_snapshots_after_etag,
    fetch_latest_snapshot,
    load_latest_snapshots_for_season,
    save_snapshot,
)

logger = logging.getLogger(__name__)

# Default freshness window for cached payloads. When a cache entry is older than
# this window, ``maybe_schedule_refresh`` will trigger a rebuild the next time a
# user hits the leaderboard API.
_CACHE_TTL = timedelta(hours=6)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


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


def _is_current_payload(payload: Mapping[str, Any]) -> bool:
    try:
        schema_version = int(payload.get("schema_version", 0))
    except (TypeError, ValueError):
        schema_version = 0
    try:
        formatter_version = int(payload.get("formatter_version", 0))
    except (TypeError, ValueError):
        formatter_version = 0
    return schema_version == SCHEMA_VERSION and formatter_version == FORMATTER_VERSION


def cache_get_leaderboard(season_id: int, stat_key: str) -> Optional[Dict[str, Any]]:
    """Return cached payload for ``(season_id, stat_key)`` if present."""

    if season_id is None:
        return None

    payload = fetch_latest_snapshot(season_id, stat_key)
    if isinstance(payload, Mapping):
        if _is_current_payload(payload):
            return dict(payload)

        logger.info(
            "Cached leaderboard payload for stat=%s season=%s is stale (schema=%s formatter=%s); refreshing.",
            stat_key,
            season_id,
            payload.get("schema_version"),
            payload.get("formatter_version"),
        )
        try:
            schedule_refresh(stat_key, season_id)
        except Exception:
            logger.exception(
                "Failed to rebuild stale leaderboard cache for stat=%s season=%s",
                stat_key,
                season_id,
            )
        else:
            refreshed = fetch_latest_snapshot(season_id, stat_key)
            if isinstance(refreshed, Mapping) and _is_current_payload(refreshed):
                return dict(refreshed)
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
    payload = build_leaderboard_payload(
        season_id,
        stat_key,
        compute_result={
            "config": compute_result.get("config") if isinstance(compute_result, Mapping) else None,
            "rows": compute_result.get("rows") if isinstance(compute_result, Mapping) else None,
            "team_totals": compute_result.get("team_totals") if isinstance(compute_result, Mapping) else None,
            "variant": compute_result.get("variant") if isinstance(compute_result, Mapping) else None,
            "aux_table": compute_result.get("aux_table") if isinstance(compute_result, Mapping) else None,
        }
        if isinstance(compute_result, Mapping)
        else compute_result,
    )

    try:
        save_snapshot(season_id, stat_key, payload, commit=commit)
    except Exception:
        db.session.rollback()
        logger.exception(
            "Failed to persist leaderboard cache for stat=%s season=%s", stat_key, season_id
        )
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
    columns_manifest = [
        col
        for col in payload.get("columns_manifest", [])
        if isinstance(col, Mapping)
    ]

    if not column_keys:
        column_keys = [col.get("key") for col in columns_manifest if col.get("key")]
    if not column_keys:
        column_keys = [col.get("key") for col in columns if isinstance(col, Mapping) and col.get("key")]

    column_value_keys: dict[str, Optional[str]] = {}
    column_order: list[str] = []
    for source in (columns_manifest, columns):
        for column in source:
            if not isinstance(column, Mapping):
                continue
            key = column.get("key")
            if not key:
                continue
            if key not in column_value_keys:
                column_order.append(key)
                column_value_keys[key] = column.get("value_key")
            elif column_value_keys[key] in (None, ""):
                value_key = column.get("value_key")
                if value_key:
                    column_value_keys[key] = value_key

    def _coerce_metric(metric: Any) -> tuple[str, Any]:
        if isinstance(metric, Mapping):
            text = metric.get("text")
            raw = metric.get("raw")
        else:
            text = metric
            raw = None
        text_str = "" if text is None else str(text)
        return text_str, raw

    def _rehydrate_entry(entry: Mapping[str, Any]) -> Optional[dict[str, Any]]:
        metrics = entry.get("metrics")
        if not isinstance(metrics, Mapping):
            return None

        hydrated: dict[str, Any] = {}
        display = entry.get("display")
        if isinstance(display, Mapping):
            for key in ("player", "rank"):
                if key in display:
                    hydrated[key] = display.get(key)

        if "rank" not in hydrated and entry.get("rank") is not None:
            hydrated["rank"] = entry.get("rank")

        seen: set[str] = set()
        for key in column_order:
            metric = metrics.get(key)
            if metric is None:
                continue
            seen.add(key)
            text, raw = _coerce_metric(metric)
            if key not in hydrated or hydrated[key] in (None, ""):
                hydrated[key] = text
            value_key = column_value_keys.get(key)
            if value_key and raw is not None:
                hydrated[value_key] = raw

        for key, metric in metrics.items():
            if key in seen:
                continue
            text, raw = _coerce_metric(metric)
            if key not in hydrated or hydrated[key] in (None, ""):
                hydrated[key] = text
            value_key = column_value_keys.get(key)
            if value_key and raw is not None:
                hydrated[value_key] = raw

        return hydrated

    rows_raw = payload.get("rows") or []
    totals_raw = payload.get("totals")

    rows: list[dict[str, Any]] = []
    for row in rows_raw:
        hydrated_row: Optional[dict[str, Any]] = None
        if isinstance(row, Mapping):
            hydrated_row = _rehydrate_entry(row)
        if hydrated_row is not None:
            rows.append(hydrated_row)
            continue
        if isinstance(row, Mapping):
            rows.append(dict(row))
        else:
            rows.append({key: value for key, value in zip(column_keys, row)})

    totals: Optional[dict[str, Any]]
    if isinstance(totals_raw, Mapping):
        hydrated_total = _rehydrate_entry(totals_raw)
        if hydrated_total is not None:
            totals = hydrated_total
        else:
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

