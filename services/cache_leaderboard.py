import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from flask import current_app

from constants import LEADERBOARD_STAT_KEYS, LEADERBOARD_STATS
from models.database import CachedLeaderboard, Season, db

from admin._leaderboard_helpers import build_leaderboard_table as _build_leaderboard_table

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


def build_leaderboard_table(stat_key: str, compute_result: Any, *, variant: Optional[str] = None):
    config, rows, totals = _extract_compute_components(stat_key, compute_result)
    table = _build_leaderboard_table(config=config, rows=rows, team_totals=totals)
    column_keys: List[str] = []
    for column in table.get("columns") or []:
        key = column.get("key") if isinstance(column, Mapping) else None
        if key:
            column_keys.append(str(key))
    table = dict(table)
    table.setdefault("column_keys", column_keys)
    return table


# --- START: authoritative payload formatter (cache uses this) ---

def format_leaderboard_payload(stat_key, compute_result, *, variant=None):
    """
    Produce a payload that the templates/JS can render directly without further formatting.
    Shape:
      {
        "stat_key": str,
        "variant": str|None,
        "columns": [str, ...],         # human labels
        "column_keys": [str, ...],     # stable keys in same order
        "rows": [ [str, ...], ... ],   # flat string lists aligned with column_keys
        "last_built_at": ISO8601Z
      }
    """
    # IMPORTANT: use the same display builder the templates use for live rendering.
    # The project already has this (e.g., build_leaderboard_table). Find it via search.
    table = build_leaderboard_table(stat_key, compute_result, variant=variant) \
        if "build_leaderboard_table" in globals() else build_leaderboard_table(stat_key, compute_result)

    columns = table["columns"]
    column_keys = table["column_keys"]
    # table["rows"] should be a list of dicts keyed by column_keys.
    rows_list = []
    for r in table["rows"]:
        row = []
        for k in column_keys:
            v = r.get(k)
            row.append("" if v is None else str(v))
        rows_list.append(row)

    totals_row = None
    totals = table.get("totals")
    if isinstance(totals, Mapping):
        totals_row = ["" if totals.get(k) is None else str(totals.get(k)) for k in column_keys]
    elif isinstance(totals, Sequence) and not isinstance(totals, (str, bytes)):
        totals_row = [
            "" if idx >= len(totals) or totals[idx] is None else str(totals[idx])
            for idx, _ in enumerate(column_keys)
        ]

    payload = {
        "stat_key": stat_key,
        "variant": variant,
        "columns": columns,
        "column_keys": column_keys,
        "rows": rows_list,
        "last_built_at": datetime.utcnow().isoformat() + "Z",
    }
    if totals_row is not None:
        payload["totals"] = totals_row

    default_sort = table.get("default_sort")
    if default_sort:
        payload["default_sort"] = default_sort
    has_data = table.get("has_data")
    if has_data is not None:
        payload["has_data"] = has_data

    return payload

# --- END: authoritative payload formatter ---


def _assert_payload_shape(payload: Dict[str, Any]) -> None:
    rows = payload.get("rows") or []
    columns = payload.get("columns") or []
    column_keys = payload.get("column_keys") or []
    assert len(columns) == len(column_keys)
    if rows:
        first_row = rows[0]
        assert isinstance(first_row, list), "rows must be list[list[str]]"
        assert len(first_row) == len(columns) == len(column_keys)
        assert all(isinstance(v, str) for v in first_row)


def expand_cached_rows_for_template(payload: JsonDict) -> tuple[Sequence[Any], Sequence[str], List[Dict[str, str]], Optional[Dict[str, str]]]:
    """Return cached columns/rows without reformatting numeric values."""

    columns = payload.get("columns") or []
    column_keys = list(payload.get("column_keys") or [])
    rows = payload.get("rows") or []
    totals = payload.get("totals")

    expanded_rows: List[Dict[str, str]] = []
    for row in rows:
        if isinstance(row, Mapping):
            entry = {}
            for key in column_keys:
                value = row.get(key)
                entry[key] = "" if value is None else str(value)
            expanded_rows.append(entry)
        elif isinstance(row, Sequence) and not isinstance(row, (str, bytes)):
            entry = {
                key: row[idx] if idx < len(row) else ""
                for idx, key in enumerate(column_keys)
            }
            expanded_rows.append(entry)
        else:
            if column_keys:
                expanded_rows.append({column_keys[0]: "" if row is None else str(row)})

    expanded_totals: Optional[Dict[str, str]] = None
    if isinstance(totals, Mapping):
        expanded_totals = {
            key: "" if totals.get(key) is None else str(totals.get(key))
            for key in column_keys
        }
    elif isinstance(totals, Sequence) and not isinstance(totals, (str, bytes)):
        expanded_totals = {
            key: totals[idx] if idx < len(totals) else ""
            for idx, key in enumerate(column_keys)
        }

    return columns, column_keys, expanded_rows, expanded_totals


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


def schedule_refresh(stat_key: str, season_id: Optional[int], variant: Optional[str] = None) -> None:
    """Queue a background rebuild job for the specified leaderboard."""

    try:
        from app import scheduler
    except Exception:  # pragma: no cover - defensive import guard
        scheduler = None

    if not getattr(scheduler, "add_job", None):
        return

    job_id = f"lb-rebuild-{season_id}-{stat_key}-{variant or 'base'}"
    try:
        scheduler.add_job(
            func=cache_build_one,
            args=[stat_key, season_id],
            kwargs={"variant": variant, "force": True},
            id=job_id,
            replace_existing=True,
            next_run_time=datetime.utcnow(),
        )
    except Exception:  # pragma: no cover - scheduler errors should not break requests
        if current_app:
            current_app.logger.exception("Failed to schedule leaderboard refresh")


def maybe_schedule_refresh(
    stat_key: str,
    season_id: Optional[int],
    variant: Optional[str],
    last_built_at_iso: Optional[str],
    staleness_min: int = 10,
) -> None:
    try:
        if not last_built_at_iso:
            schedule_refresh(stat_key, season_id, variant)
            return
        ts = datetime.fromisoformat(last_built_at_iso.replace("Z", ""))
        if datetime.utcnow() - ts > timedelta(minutes=staleness_min):
            schedule_refresh(stat_key, season_id, variant)
    except Exception:  # pragma: no cover - defensive
        pass


def cache_get_leaderboard(season_id: Optional[int], stat_key: str, *, variant: Optional[str] = None) -> Optional[JsonDict]:
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


def cache_set_leaderboard(
    season_id: Optional[int],
    stat_key: str,
    payload_dict: JsonDict,
    *,
    variant: Optional[str] = None,
) -> None:
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
    compute_fn: Optional[Callable[[str, Optional[int]], JsonDict]] = None,
    *,
    variant: Optional[str] = None,
    force: bool = False,
) -> JsonDict:
    compute = compute_fn or _import_compute_leaderboard()
    kwargs: Dict[str, Any] = {}
    if variant is not None:
        kwargs["variant"] = variant
    try:
        compute_result = compute(stat_key, season_id, **kwargs)
    except TypeError:
        compute_result = compute(stat_key, season_id)

    payload = format_leaderboard_payload(stat_key, compute_result, variant=variant)
    payload["season_id"] = season_id
    _assert_payload_shape(payload)

    cache_set_leaderboard(season_id, stat_key, payload, variant=variant)
    return payload


def cache_build_all(
    season_id: Optional[int],
    compute_fn: Optional[Callable[[str, Optional[int]], JsonDict]] = None,
    stat_keys: Optional[Sequence[str]] = None,
    *,
    variant: Optional[str] = None,
    force: bool = False,
) -> Dict[str, JsonDict]:
    results: Dict[str, JsonDict] = {}
    keys = stat_keys or LEADERBOARD_STAT_KEYS
    compute = compute_fn or _import_compute_leaderboard()
    for sk in keys:
        results[sk] = cache_build_one(
            sk,
            season_id,
            compute,
            variant=variant,
            force=force,
        )
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
    cache_build_all(season_id, compute, stat_keys, force=True)


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
