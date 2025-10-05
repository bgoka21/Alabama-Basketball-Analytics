import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional

from flask import current_app

from models.database import db, CachedLeaderboard

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
    data = compute_fn(stat_key, season_id)
    data.setdefault("stat_key", stat_key)
    data.setdefault("season_id", season_id)
    data["updated_at"] = datetime.utcnow().isoformat()
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
