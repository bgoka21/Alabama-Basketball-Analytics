"""Caching helpers for simple leaderboard dropdown tables.

This module normalises leaderboard query results into the compact row
structure expected by the lightweight leaderboard templates. Cached payloads
include a ``schema_version`` so format changes can invalidate stale entries
automatically. When Flask-Caching is unavailable the module falls back to an
in-memory cache, which is primarily used for unit tests.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from time import perf_counter
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

from flask import current_app

from constants import LEADERBOARD_STAT_KEYS
from models.database import CachedLeaderboard, db
from services.leaderboard_source import fetch_stat_rows

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
FORMATTER_VERSION = 1
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
    try:
        module_spec = importlib.util.find_spec("app.extensions")
    except ImportError:
        module_spec = None
    if module_spec is not None:
        try:
            module = importlib.import_module("app.extensions")
        except ImportError:
            module = None
        if module is not None:
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


def query_stat_rows(stat_key: str, season_id: int) -> Iterable[Any]:
    """Fetch and normalise leaderboard rows for cache building."""

    raw = list(fetch_stat_rows(stat_key, season_id))
    out = []
    for r in raw:
        get = (r.get if hasattr(r, "get") else lambda k, d=None: getattr(r, k, d))
        num = get("player_number") or get("number")
        name = get("player_name") or get("name")
        val = get("value")
        if val is None:
            val = get(stat_key)
        out.append({"player_number": num, "player_name": name, "value": val})

    current_app.logger.info(
        "query_stat_rows stat=%s season=%s count=%s", stat_key, season_id, len(out)
    )
    return out


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


# ─── Snapshot Builder Helpers ────────────────────────────────────────────────


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _import_table_builder():  # pragma: no cover - tested via callers
    from admin._leaderboard_helpers import build_leaderboard_table

    return build_leaderboard_table


def _import_cache_compute():  # pragma: no cover - tested via callers
    from admin.routes import build_leaderboard_cache_payload

    return build_leaderboard_cache_payload


def _normalize_compute_result(result: Any) -> tuple[Dict[str, Any] | None, Any, Any, Optional[Any]]:
    cfg = None
    rows = None
    totals = None
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
        "Leaderboard compute functions must return a mapping with 'config', "
        "'rows', and 'team_totals' keys or a tuple (config, rows, team_totals).",
    )


def _build_table_payload(
    stat_key: str,
    season_id: int,
    *,
    compute_result: Any | None = None,
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any], Optional[Any]]:
    compute = compute_result
    if compute is None:
        compute_fn = _import_cache_compute()
        compute = compute_fn(stat_key, season_id)

    cfg, rows, totals, variant = _normalize_compute_result(compute)

    builder = _import_table_builder()
    table = builder(
        config=cfg,
        rows=rows,
        team_totals=totals,
        table_id=f"leaderboard-{stat_key}",
    )

    aux_table = None
    aux_source = None
    if isinstance(compute, Mapping):
        aux_source = compute.get("aux_table")
    if aux_source:
        aux_cfg = aux_source.get("config") if isinstance(aux_source, Mapping) else None
        aux_rows = aux_source.get("rows") if isinstance(aux_source, Mapping) else None
        aux_totals = aux_source.get("team_totals") if isinstance(aux_source, Mapping) else None
        if aux_cfg or aux_rows or aux_totals:
            aux_table = builder(
                config=aux_cfg,
                rows=aux_rows,
                team_totals=aux_totals,
                table_id=f"leaderboard-{stat_key}-aux",
            )

    return table, aux_table, cfg or {}, variant


def compute_columns_for(
    stat_key: str,
    *,
    table: Optional[Mapping[str, Any]] = None,
    season_id: Optional[int] = None,
) -> list[dict[str, Any]]:
    if table is None:
        if season_id is None:
            raise ValueError("season_id is required when table is not provided")
        table, _, _, _ = _build_table_payload(stat_key, season_id=season_id)

    columns = []
    for column in table.get("columns", []):
        if not isinstance(column, Mapping):
            continue
        key = column.get("key")
        if not key:
            continue
        columns.append(
            {
                "key": key,
                "label": column.get("label", key),
                "align": column.get("align"),
                "value_key": column.get("value_key"),
            }
        )
    return columns


def _coerce_raw_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if text.endswith("%"):
                return float(text[:-1]) / 100
            return float(text)
        except ValueError:
            return None
    return None


def _normalize_rows(
    table_rows: Iterable[Mapping[str, Any]],
    columns_manifest: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in table_rows:
        if not isinstance(row, Mapping):
            continue
        metrics: Dict[str, Dict[str, Any]] = {}
        for column in columns_manifest:
            key = column.get("key")
            if not key:
                continue
            text_value = row.get(key)
            value_key = column.get("value_key")
            raw_value = row.get(value_key) if value_key else None
            if raw_value is None:
                raw_value = _coerce_raw_value(text_value)
            metrics[key] = {
                "raw": raw_value,
                "text": "" if text_value is None else str(text_value),
            }

        normalized.append(
            {
                "rank": row.get("rank"),
                "display": {
                    "player": row.get("player"),
                    "rank": row.get("rank"),
                },
                "metrics": metrics,
            }
        )
    return normalized


def _normalize_totals(
    totals: Optional[Mapping[str, Any]],
    columns_manifest: Sequence[Mapping[str, Any]],
) -> Optional[dict[str, Any]]:
    if not isinstance(totals, Mapping):
        return None

    metrics: Dict[str, Dict[str, Any]] = {}
    for column in columns_manifest:
        key = column.get("key")
        if not key:
            continue
        text_value = totals.get(key)
        value_key = column.get("value_key")
        raw_value = totals.get(value_key) if value_key else None
        if raw_value is None:
            raw_value = _coerce_raw_value(text_value)
        metrics[key] = {
            "raw": raw_value,
            "text": "" if text_value is None else str(text_value),
        }

    return {
        "display": {
            "player": totals.get("player", "Team Totals"),
            "rank": totals.get("rank", ""),
        },
        "metrics": metrics,
    }


def compute_rows_for(
    season_id: int,
    stat_key: str,
    *,
    table: Optional[Mapping[str, Any]] = None,
    columns_manifest: Optional[Sequence[Mapping[str, Any]]] = None,
) -> list[dict[str, Any]]:
    current_table = table
    if current_table is None:
        current_table, _, _, _ = _build_table_payload(stat_key, season_id)
    manifest = list(
        columns_manifest
        or compute_columns_for(stat_key, table=current_table, season_id=season_id)
    )
    return _normalize_rows(current_table.get("rows", []), manifest)


def compute_aux_table_if_any(
    season_id: int,
    stat_key: str,
    *,
    aux_table: Optional[Mapping[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    table_payload = aux_table
    if table_payload is None:
        _, table_payload, _, _ = _build_table_payload(stat_key, season_id)
    if not isinstance(table_payload, Mapping):
        return None

    columns_manifest = compute_columns_for(
        stat_key,
        table=table_payload,
        season_id=season_id,
    )
    rows = _normalize_rows(table_payload.get("rows", []), columns_manifest)
    totals = _normalize_totals(table_payload.get("totals"), columns_manifest)

    return {
        "columns_manifest": columns_manifest,
        "rows": rows,
        "totals": totals,
        "table_id": table_payload.get("id"),
        "default_sort": table_payload.get("default_sort"),
        "has_data": table_payload.get("has_data", bool(rows)),
    }


def build_manifest_for(season_id: int, stat_key: str) -> dict[str, Any]:
    compute_fn = _import_cache_compute()
    return {
        "season_id": season_id,
        "stat_key": stat_key,
        "builder": "services.leaderboard_cache.build_leaderboard_payload",
        "compute_function": f"{compute_fn.__module__}.{compute_fn.__name__}",
        "built_at": _isoformat(_utcnow()),
    }


def build_leaderboard_payload(
    season_id: int,
    stat_key: str,
    *,
    compute_result: Any | None = None,
) -> Dict[str, Any]:
    table, aux_table, config, variant = _build_table_payload(
        stat_key,
        season_id,
        compute_result=compute_result,
    )

    columns_manifest = compute_columns_for(
        stat_key,
        table=table,
        season_id=season_id,
    )
    rows = compute_rows_for(
        season_id,
        stat_key,
        table=table,
        columns_manifest=columns_manifest,
    )
    totals = _normalize_totals(table.get("totals"), columns_manifest)

    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "formatter_version": FORMATTER_VERSION,
        "season_id": season_id,
        "stat_key": stat_key,
        "columns_manifest": columns_manifest,
        "columns": table.get("columns", []),
        "rows": rows,
        "totals": totals,
        "table_id": table.get("id"),
        "default_sort": table.get("default_sort"),
        "has_data": table.get("has_data", bool(rows) or bool(totals)),
        "built_at": _isoformat(_utcnow()),
        "config": config or {},
    }

    column_keys = [col.get("key") for col in table.get("columns", []) if isinstance(col, Mapping)]
    if column_keys:
        payload["column_keys"] = column_keys

    if variant is not None:
        payload["variant"] = variant

    aux_payload = compute_aux_table_if_any(
        season_id,
        stat_key,
        aux_table=aux_table,
    )
    if aux_payload:
        payload["aux_table"] = aux_payload

    return payload


def _snapshot_query(season_id: int, stat_key: str):
    return (
        CachedLeaderboard.query.filter_by(season_id=season_id, stat_key=stat_key)
        .order_by(CachedLeaderboard.created_at.desc(), CachedLeaderboard.id.desc())
    )


def save_snapshot(
    season_id: int,
    stat_key: str,
    payload: Dict[str, Any],
    *,
    retain: int = 5,
    commit: bool = True,
) -> CachedLeaderboard:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    etag = hashlib.sha256(body.encode("utf-8")).hexdigest()
    manifest_json = json.dumps(build_manifest_for(season_id, stat_key))
    now = datetime.utcnow()

    entry = CachedLeaderboard.query.filter_by(
        season_id=season_id, stat_key=stat_key
    ).first()

    if entry is None:
        entry = CachedLeaderboard(
            season_id=season_id,
            stat_key=stat_key,
            schema_version=int(payload.get("schema_version", SCHEMA_VERSION)),
            formatter_version=int(payload.get("formatter_version", FORMATTER_VERSION)),
            etag=etag,
            payload_json=body,
            build_manifest=manifest_json,
            created_at=now,
            updated_at=now,
        )
        db.session.add(entry)
    else:
        entry.schema_version = int(payload.get("schema_version", SCHEMA_VERSION))
        entry.formatter_version = int(payload.get("formatter_version", FORMATTER_VERSION))
        entry.etag = etag
        entry.payload_json = body
        entry.build_manifest = manifest_json
        entry.updated_at = now

    db.session.flush()

    _prune_old_snapshots(season_id, stat_key, retain)

    if commit:
        db.session.commit()
    else:
        db.session.flush()

    return entry


def _prune_old_snapshots(season_id: int, stat_key: str, retain: int) -> None:
    if retain <= 0:
        retain = 1
    query = _snapshot_query(season_id, stat_key)
    stale = query.offset(retain).all()
    for row in stale:
        db.session.delete(row)


def fetch_latest_snapshot(season_id: int, stat_key: str) -> Optional[Dict[str, Any]]:
    row = _snapshot_query(season_id, stat_key).first()
    if not row:
        return None
    try:
        payload = json.loads(row.payload_json)
    except json.JSONDecodeError:
        logger.warning(
            "Failed to decode cached leaderboard snapshot season=%s stat=%s", season_id, stat_key
        )
        return None
    return payload


def list_snapshots(season_id: int, stat_key: str) -> list[CachedLeaderboard]:
    return list(_snapshot_query(season_id, stat_key).all())


def delete_snapshots_after_etag(
    season_id: int,
    stat_key: str,
    etag: str,
    *,
    commit: bool = True,
) -> int:
    snapshots = _snapshot_query(season_id, stat_key).all()
    target = None
    for snap in snapshots:
        if snap.etag == etag:
            target = snap
            break

    if target is None:
        raise ValueError(f"No snapshot found for {season_id=} {stat_key=} with etag {etag}")

    to_delete = [snap for snap in snapshots if snap.created_at > target.created_at or (snap.created_at == target.created_at and snap.id > target.id)]

    for snap in to_delete:
        db.session.delete(snap)

    if commit:
        db.session.commit()

    return len(to_delete)


def load_latest_snapshots_for_season(season_id: int) -> Dict[str, Dict[str, Any]]:
    entries = (
        CachedLeaderboard.query.filter_by(season_id=season_id)
        .order_by(CachedLeaderboard.stat_key.asc(), CachedLeaderboard.created_at.desc(), CachedLeaderboard.id.desc())
        .all()
    )
    latest: Dict[str, Dict[str, Any]] = {}
    for entry in entries:
        if entry.stat_key in latest:
            continue
        try:
            payload = json.loads(entry.payload_json)
        except json.JSONDecodeError:
            logger.warning(
                "Failed to decode cached leaderboard snapshot for season=%s stat=%s", entry.season_id, entry.stat_key
            )
            continue
        latest[entry.stat_key] = payload
    return latest
