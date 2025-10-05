import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence

from flask import current_app

from models.database import db, CachedLeaderboard, Season
from admin._leaderboard_helpers import build_leaderboard_table

JsonDict = Dict[str, Any]

# --- BEGIN PATCH: robust normalizer + compute import helper ---
def _format_for_display(
    cfg: Optional[Dict[str, Any]],
    raw_rows: Optional[Sequence[Any]],
    raw_totals: Optional[Any],
) -> Dict[str, Any]:
    """Return cache payload with pre-formatted leaderboard rows."""

    table = build_leaderboard_table(
        config=cfg,
        rows=raw_rows,
        team_totals=raw_totals,
    )

    columns = table.get("columns") or []
    key_label_pairs = [
        (col.get("key"), col.get("label"))
        for col in columns
        if col.get("key")
    ]
    column_keys = [pair[0] for pair in key_label_pairs]
    column_labels = [pair[1] for pair in key_label_pairs]

    display_rows = []
    for row in table.get("rows") or []:
        display_rows.append([row.get(key, "") for key in column_keys])

    display_totals = None
    totals_entry = table.get("totals")
    if totals_entry:
        display_totals = [totals_entry.get(key, "") for key in column_keys]

    payload: Dict[str, Any] = {
        "config": cfg,
        "columns": column_labels,
        "column_keys": column_keys,
        "rows": display_rows,
        "team_totals": display_totals,
        "default_sort": table.get("default_sort"),
        "has_data": table.get("has_data"),
    }

    return payload


def expand_cached_rows_for_template(payload: JsonDict) -> tuple[Sequence[Any], Any]:
    """Return rows/totals suitable for feeding back into templates."""

    rows = payload.get("rows")
    totals = payload.get("team_totals")
    column_keys = payload.get("column_keys")

    if not isinstance(column_keys, list) or not isinstance(rows, list):
        return rows or [], totals

    expanded_rows: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            expanded_rows.append(dict(row))
            continue
        if isinstance(row, list):
            entry: Dict[str, Any] = {}
            for idx, key in enumerate(column_keys):
                entry[key] = row[idx] if idx < len(row) else ""
            expanded_rows.append(entry)
            continue
        if column_keys:
            expanded_rows.append({column_keys[0]: row})

    expanded_totals: Any = totals
    if isinstance(totals, list):
        totals_entry: Dict[str, Any] = {}
        for idx, key in enumerate(column_keys):
            totals_entry[key] = totals[idx] if idx < len(totals) else ""
        expanded_totals = totals_entry

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
    raw = compute_fn(stat_key, season_id)

    cfg: Optional[Dict[str, Any]] = None
    raw_rows: Sequence[Any] = []
    raw_totals: Any = None

    if raw is None:
        raw_rows = []
    elif isinstance(raw, dict):
        cfg = raw.get("config")
        raw_rows = raw.get("rows") or []
        raw_totals = raw.get("team_totals")
    elif isinstance(raw, (list, tuple)):
        if len(raw) == 3:
            cfg, raw_rows, raw_totals = raw  # type: ignore[assignment]
        elif len(raw) == 2:
            cfg, raw_rows = raw  # type: ignore[assignment]
        else:
            raw_rows = list(raw)
    else:
        raw_rows = [raw]

    payload = _format_for_display(cfg, raw_rows, raw_totals)
    payload["stat_key"] = stat_key
    payload["season_id"] = season_id
    payload["updated_at"] = datetime.utcnow().isoformat()

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


def rebuild_leaderboards_for_season(season_id, stat_keys, compute_fn=None):
    """
    Rebuild cache for all stat_keys for a given season_id.
    """
    compute = compute_fn or _import_compute_leaderboard()
    for sk in stat_keys:
        cache_build_one(sk, season_id, compute)


def rebuild_leaderboards_after_parse():
    """
    Safe to call at the end of parsing. Never raises.
    Picks current season if available, else the first season.
    """
    from constants import LEADERBOARD_STAT_KEYS

    try:
        # prefer current season if field exists
        s = None
        try:
            s = Season.query.filter_by(is_current=True).first()
        except Exception:
            s = None
        if not s:
            s = Season.query.first()
        if not s:
            print("[cache] No seasons found; skipping cache rebuild.")
            return
        season_id = s.id
        print(f"[cache] Rebuilding cached leaderboards for season {season_id}...")
        rebuild_leaderboards_for_season(season_id, LEADERBOARD_STAT_KEYS)
        print("[cache] Rebuild complete.")
    except Exception as e:
        # absolutely never break parsing
        print(f"[cache] Rebuild failed (non-fatal): {e}")


# --- END PATCH ---
