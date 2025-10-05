import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from flask import current_app

from constants import LEADERBOARD_STAT_KEYS, LEADERBOARD_STATS
from models.database import CachedLeaderboard, Season, db

from admin._leaderboard_helpers import build_leaderboard_table

JsonDict = Dict[str, Any]

def _lookup_stat_config(stat_key: str) -> Dict[str, Any]:
    """Return a mutable copy of the stat config for ``stat_key``."""

    for cfg in LEADERBOARD_STATS:
        if cfg.get("key") == stat_key:
            return dict(cfg)
    # Fallback: minimal config so ``build_leaderboard_table`` still works.
    return {"key": stat_key, "label": stat_key.replace("_", " ").title()}


def _extract_compute_components(
    stat_key: str, compute_result: Any
) -> Tuple[Dict[str, Any], Sequence[Any], Optional[Any]]:
    """Normalise ``compute_result`` into ``(config, rows, totals)``."""

    def _is_row_sequence(value: Any) -> bool:
        return isinstance(value, Sequence) and not isinstance(value, (str, bytes, dict))

    config: Optional[Dict[str, Any]] = None
    rows: Sequence[Any] = []
    totals: Optional[Any] = None

    if compute_result is None:
        return _lookup_stat_config(stat_key), rows, totals

    if isinstance(compute_result, dict):
        cfg = compute_result.get("config")
        if isinstance(cfg, dict):
            config = dict(cfg)
        rows = compute_result.get("rows") or []
        totals = compute_result.get("team_totals") or compute_result.get("totals")
        return (config or _lookup_stat_config(stat_key), rows, totals)

    if isinstance(compute_result, (list, tuple)):
        values = list(compute_result)

        # Identify config from any dict entry with a ``key`` attribute first.
        for item in values:
            if isinstance(item, dict) and item.get("key"):
                config = dict(item)
                break
        if config is None:
            for item in values:
                if isinstance(item, dict):
                    config = dict(item)
                    break

        for item in values:
            if _is_row_sequence(item):
                rows = item
                break

        # Totals is whichever remaining entry is not the config or rows.
        for item in values:
            if item is config or item is rows:
                continue
            if _is_row_sequence(item) and item is rows:
                continue
            if isinstance(item, dict) and config is not None and item.get("key") == config.get("key"):
                continue
            totals = item
            break

        return (config or _lookup_stat_config(stat_key), rows or [], totals)

    if _is_row_sequence(compute_result):
        rows = compute_result
    else:
        rows = [compute_result]

    return _lookup_stat_config(stat_key), rows, totals


def format_leaderboard_payload(stat_key: str, compute_result: Any) -> Dict[str, Any]:
    """Return a cache payload containing display-ready leaderboard rows."""

    config, rows, totals = _extract_compute_components(stat_key, compute_result)

    table = build_leaderboard_table(config=config, rows=rows, team_totals=totals)

    column_entries = table.get("columns") or []
    column_keys: List[str] = []
    column_labels: List[str] = []
    for column in column_entries:
        key = column.get("key")
        if not key:
            continue
        column_keys.append(str(key))
        column_labels.append(str(column.get("label", "")))

    formatted_rows: List[List[str]] = []
    for row in table.get("rows") or []:
        formatted_rows.append(
            [
                "" if row.get(key) is None else str(row.get(key))
                for key in column_keys
            ]
        )

    totals_row: Optional[List[str]] = None
    totals_entry = table.get("totals")
    if isinstance(totals_entry, Mapping):
        totals_row = [
            "" if totals_entry.get(key) is None else str(totals_entry.get(key))
            for key in column_keys
        ]
    elif isinstance(totals_entry, Sequence) and not isinstance(totals_entry, (str, bytes)):
        totals_row = [
            "" if idx >= len(totals_entry) or totals_entry[idx] is None else str(totals_entry[idx])
            for idx, _ in enumerate(column_keys)
        ]

    payload: Dict[str, Any] = {
        "stat_key": stat_key,
        "columns": column_labels,
        "column_keys": column_keys,
        "rows": formatted_rows,
        "last_built_at": datetime.utcnow().isoformat() + "Z",
    }
    if totals_row is not None:
        payload["totals"] = totals_row
    default_sort = table.get("default_sort")
    if default_sort:
        payload["default_sort"] = default_sort
    if table.get("has_data") is not None:
        payload["has_data"] = table.get("has_data")

    return payload


def expand_cached_rows_for_template(payload: JsonDict) -> tuple[Sequence[Any], Any]:
    """Return rows/totals suitable for feeding back into templates."""

    rows = payload.get("rows") or []
    column_keys = payload.get("column_keys") or []
    totals = payload.get("totals")

    expanded_rows: List[Dict[str, Any]] = []
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                expanded_rows.append(dict(row))
                continue
            if isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
                entry: Dict[str, Any] = {}
                for idx, key in enumerate(column_keys):
                    entry[key] = row[idx] if idx < len(row) else ""
                expanded_rows.append(entry)
                continue
            if column_keys:
                expanded_rows.append({column_keys[0]: row})

    expanded_totals: Any = None
    if isinstance(totals, Mapping):
        expanded_totals = dict(totals)
    elif isinstance(totals, Sequence) and not isinstance(totals, (str, bytes)):
        expanded_totals = {
            key: totals[idx] if idx < len(totals) else ""
            for idx, key in enumerate(column_keys)
        }

    return expanded_rows, expanded_totals


def _import_compute_leaderboard():
    """
    Compute function lives in public.routes or admin.routes depending on build.
    Import safely and return the callable.
    """
    try:
        from public.routes import compute_leaderboard

        return compute_leaderboard
    except Exception:
        pass
    from admin.routes import compute_leaderboard  # fallback

    return compute_leaderboard


# --- END PATCH ---

def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def cache_get_leaderboard(season_id: Optional[int], stat_key: str) -> Optional[JsonDict]:
    row = CachedLeaderboard.query.filter_by(season_id=season_id, stat_key=stat_key).first()
    if not row:
        return None
    try:
        return json.loads(row.payload_json)
    except Exception:
        current_app.logger.exception(
            "Failed to decode cached leaderboard for season=%s stat=%s", season_id, stat_key
        )
        return None


def cache_set_leaderboard(season_id: Optional[int], stat_key: str, payload_dict: JsonDict) -> None:
    payload_str = json.dumps(
        payload_dict,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )
    existing = CachedLeaderboard.query.filter_by(season_id=season_id, stat_key=stat_key).first()
    now = datetime.utcnow()
    if existing:
        existing.payload_json = payload_str
        existing.updated_at = now
    else:
        db.session.add(
            CachedLeaderboard(
                season_id=season_id,
                stat_key=stat_key,
                payload_json=payload_str,
                updated_at=now,
            )
        )
    db.session.commit()


def cache_build_one(
    stat_key: str,
    season_id: Optional[int],
    compute_fn: Callable[[str, Optional[int]], JsonDict],
) -> JsonDict:
    compute_result = compute_fn(stat_key, season_id)
    payload = format_leaderboard_payload(stat_key, compute_result)
    payload["season_id"] = season_id

    cache_set_leaderboard(season_id, stat_key, payload)
    return payload


def cache_build_all(
    season_id: Optional[int],
    compute_fn: Callable[[str, Optional[int]], JsonDict],
    stat_keys,
) -> Dict[str, JsonDict]:
    results: Dict[str, JsonDict] = {}
    for sk in stat_keys:
        results[sk] = cache_build_one(sk, season_id, compute_fn)
    return results


# --- BEGIN PATCH: rebuild helpers ---


def rebuild_leaderboards_for_season(
    season_id: Optional[int],
    stat_keys: Sequence[str] = LEADERBOARD_STAT_KEYS,
    *,
    compute_fn: Optional[Callable[[str, Optional[int]], JsonDict]] = None,
) -> None:
    """Rebuild cached leaderboards for ``season_id``."""

    if season_id is None:
        return

    compute = compute_fn or _import_compute_leaderboard()
    for sk in stat_keys:
        cache_build_one(sk, season_id, compute)


def rebuild_leaderboards_after_parse(
    season_id: Optional[int],
    *,
    stat_keys: Sequence[str] = LEADERBOARD_STAT_KEYS,
) -> None:
    """Rebuild caches after a successful parse without raising to the caller."""

    logger = getattr(current_app, "logger", None)

    try:
        resolved_id = season_id
        if resolved_id is None:
            try:
                season = Season.query.filter_by(is_current=True).first()
            except Exception:  # pragma: no cover - defensive
                season = None
            if not season:
                season = Season.query.order_by(Season.start_date.desc()).first()
            if not season:
                if logger:
                    logger.warning("Skipped leaderboard cache rebuild: no seasons available.")
                return
            resolved_id = season.id

        rebuild_leaderboards_for_season(resolved_id, stat_keys)
        if logger:
            logger.info("Rebuilt leaderboard cache for season %s", resolved_id)
    except Exception as exc:  # pragma: no cover - defensive
        if logger:
            logger.exception("Cache rebuild failed after parse: %s", exc)


# --- END PATCH ---
