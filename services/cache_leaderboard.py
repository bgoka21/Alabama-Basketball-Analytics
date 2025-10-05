import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

from flask import current_app

from models.database import db, CachedLeaderboard, Season

# --- BEGIN PATCH: robust normalizer + compute import helper ---
def _normalize_payload(result, stat_key, season_id):
    """
    Accepts tuple/list/dict/None and returns a dict for caching:
    { rows: [...], stat_key, season_id, updated_at }
    """
    if result is None:
        data = {"rows": []}
    elif isinstance(result, dict):
        data = dict(result)  # shallow copy
    elif isinstance(result, (list, tuple)):
        # Common pattern: (rows, meta_dict)
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], dict):
            rows, meta = result
            data = {"rows": list(rows), **meta}
        else:
            data = {"rows": list(result)}
    else:
        data = {"rows": [result]}

    data["stat_key"] = stat_key
    data["season_id"] = season_id
    data["updated_at"] = datetime.utcnow().isoformat()
    return data


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

JsonDict = Dict[str, Any]


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
    data = _normalize_payload(raw, stat_key, season_id)
    cache_set_leaderboard(season_id, stat_key, data)
    return data


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
