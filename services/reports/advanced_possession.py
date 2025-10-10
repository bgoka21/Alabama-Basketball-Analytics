"""Advanced possession report computations and caching."""

from __future__ import annotations

from collections import OrderedDict
from contextlib import suppress
from datetime import datetime, timezone
import logging
import re
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from flask import current_app
from sqlalchemy import case, func

from models.database import db, Possession, ShotDetail


_LOGGER = logging.getLogger(__name__)
_CACHE_TTL_SECONDS = 60 * 60  # 1 hour
_IN_MEMORY_BACKEND = "_InMemoryCache"

_PAINT_LABELS: List[str] = ["0", "1", "2", "3+"]
_SHOT_CLOCK_BUCKETS = (
    (1, 6, ":01–:06"),
    (7, 12, ":07–:12"),
    (13, 18, ":13–:18"),
    (19, 24, ":19–:24"),
    (25, 30, ":25–:30"),
)
_POSSESSION_TYPE_LABELS: List[str] = [
    "Transition",
    "Man",
    "Zone",
    "Press",
    "UOB",
    "SLOB",
    "OREB Putback",
    "Garbage",
]
_POSSESSION_TYPE_LOOKUP: Dict[str, str] = {
    label.lower(): label for label in _POSSESSION_TYPE_LABELS
}
_POSSESSION_TYPE_LOOKUP.update(
    {
        "oreb put back": "OREB Putback",
        "oreb put-back": "OREB Putback",
        "oreb putback": "OREB Putback",
    }
)
_PRACTICE_TEAM_KEYS = ("crimson", "white")

_PRACTICE_CACHE: Dict[int, Dict[str, object]] = {}
_GAME_CACHE: Dict[int, Dict[str, object]] = {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_side(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _value_has_neutral(value: Optional[str]) -> bool:
    if not value:
        return False
    return "neutral" in str(value).lower()


def _is_neutral_row(row: Mapping[str, object], neutral_hits: int) -> bool:
    if neutral_hits:
        return True
    for candidate in (
        row.get("possession_type"),
        row.get("drill_labels"),
        row.get("shot_clock"),
        row.get("shot_clock_pt"),
    ):
        if _value_has_neutral(candidate):
            return True
    return False


def _bucket_paint_touches(value: Optional[object]) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.findall(r"-?\d+", text)
    if not match:
        return None
    try:
        number = abs(int(match[0]))
    except (TypeError, ValueError):
        return None
    if number >= 3:
        return "3+"
    return str(number)


def _bucket_shot_clock(primary: Optional[object], fallback: Optional[object]) -> Optional[str]:
    text = str(primary or fallback or "").strip()
    if not text:
        return None
    match = re.findall(r"\d+", text)
    if not match:
        return None
    try:
        first = int(match[0])
    except (TypeError, ValueError):
        return None
    if first <= 0:
        first = 1
    for lower, upper, label in _SHOT_CLOCK_BUCKETS:
        if lower <= first <= upper:
            return label
    if first < _SHOT_CLOCK_BUCKETS[0][0]:
        return _SHOT_CLOCK_BUCKETS[0][2]
    return _SHOT_CLOCK_BUCKETS[-1][2]


def _extract_possession_types(value: Optional[object]) -> List[str]:
    if not value:
        return []
    tokens = re.split(r",|/", str(value))
    labels: List[str] = []
    for raw in tokens:
        cleaned = raw.strip()
        if not cleaned:
            continue
        normalized = cleaned.lower().replace("–", "-")
        normalized = normalized.replace("  ", " ")
        normalized = normalized.replace("put back", "putback")
        canonical = _POSSESSION_TYPE_LOOKUP.get(normalized)
        if canonical:
            labels.append(canonical)
    return labels


def _fetch_event_counts(possession_ids: Iterable[int]) -> Dict[int, Dict[str, int]]:
    ids = [int(pid) for pid in possession_ids if pid is not None]
    if not ids:
        return {}
    rows = (
        db.session.query(
            ShotDetail.possession_id,
            func.sum(case((ShotDetail.event_type == "TEAM Off Reb", 1), else_=0)).label("team_oreb"),
            func.sum(case((ShotDetail.event_type.ilike("%Neutral%"), 1), else_=0)).label("neutral_hits"),
        )
        .filter(ShotDetail.possession_id.in_(ids))
        .group_by(ShotDetail.possession_id)
        .all()
    )
    return {
        row.possession_id: {
            "oreb": int(row.team_oreb or 0),
            "neutral": int(row.neutral_hits or 0),
        }
        for row in rows
    }


def _init_table(labels: Iterable[str]) -> "OrderedDict[str, MutableMapping[str, float]]":
    return OrderedDict((label, {"pts": 0.0, "chances": 0.0}) for label in labels)


def _finalize_table(
    table: "OrderedDict[str, MutableMapping[str, float]]",
) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    total_chances = sum(entry["chances"] for entry in table.values())
    total_points = sum(entry["pts"] for entry in table.values())
    for label, entry in table.items():
        chances = int(round(entry["chances"]))
        pts = int(round(entry["pts"]))
        ppc = round(entry["pts"] / entry["chances"], 2) if entry["chances"] else 0.0
        freq = round((entry["chances"] / total_chances) * 100, 1) if total_chances else 0.0
        rows.append(
            {
                "label": label,
                "pts": pts,
                "chances": chances,
                "ppc": ppc,
                "freq": freq,
            }
        )
    totals = {
        "label": "Totals",
        "pts": int(round(total_points)),
        "chances": int(round(total_chances)),
        "ppc": round(total_points / total_chances, 2) if total_chances else 0.0,
        "freq": 100.0 if total_chances else 0.0,
    }
    return rows, totals


def _row_to_dict(row: object) -> Dict[str, object]:
    if isinstance(row, Mapping):
        return dict(row)
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    return dict(row)


def _aggregate_rows(
    rows: Iterable[Mapping[str, object]],
    events: Mapping[int, Mapping[str, int]],
) -> Dict[str, object]:
    paint_table = _init_table(_PAINT_LABELS)
    shot_table = _init_table(bucket[2] for bucket in _SHOT_CLOCK_BUCKETS)
    type_table = _init_table(_POSSESSION_TYPE_LABELS)

    total_pts = 0
    total_chances = 0

    for original in rows:
        row = _row_to_dict(original)
        row_id = int(row["id"])
        row_events = events.get(row_id, {})
        oreb = int(row_events.get("oreb", 0))
        neutral_hits = int(row_events.get("neutral", 0))
        if _is_neutral_row(row, neutral_hits):
            continue
        points = int(row.get("points_scored") or 0)
        chance = 1 + max(0, oreb)
        total_pts += points
        total_chances += chance

        paint_label = _bucket_paint_touches(row.get("paint_touches"))
        if paint_label and paint_label in paint_table:
            paint_table[paint_label]["pts"] += points
            paint_table[paint_label]["chances"] += chance

        shot_label = _bucket_shot_clock(row.get("shot_clock"), row.get("shot_clock_pt"))
        if shot_label and shot_label in shot_table:
            shot_table[shot_label]["pts"] += points
            shot_table[shot_label]["chances"] += chance

        for label in _extract_possession_types(row.get("possession_type")):
            if label in type_table:
                type_table[label]["pts"] += points
                type_table[label]["chances"] += chance

    paint_rows, paint_totals = _finalize_table(paint_table)
    shot_rows, shot_totals = _finalize_table(shot_table)
    type_rows, type_totals = _finalize_table(type_table)

    return {
        "paint_touches": paint_rows,
        "shot_clock": shot_rows,
        "possession_type": type_rows,
        "totals": {
            "paint_touches": paint_totals,
            "shot_clock": shot_totals,
            "possession_type": type_totals,
        },
        "meta": {
            "total_pts": int(total_pts),
            "total_chances": int(total_chances),
        },
    }


def compute_advanced_possession_practice(practice_id: int) -> Dict[str, object]:
    rows = (
        db.session.query(
            Possession.id.label("id"),
            Possession.possession_side.label("possession_side"),
            Possession.paint_touches.label("paint_touches"),
            Possession.shot_clock.label("shot_clock"),
            Possession.shot_clock_pt.label("shot_clock_pt"),
            Possession.possession_type.label("possession_type"),
            Possession.points_scored.label("points_scored"),
            Possession.drill_labels.label("drill_labels"),
        )
        .filter(Possession.practice_id == practice_id)
        .all()
    )
    events = _fetch_event_counts(row.id for row in rows)

    team_rows: Dict[str, List[Mapping[str, object]]] = {key: [] for key in _PRACTICE_TEAM_KEYS}
    for row in rows:
        side = _normalize_side(row.possession_side)
        if side in team_rows:
            team_rows[side].append(_row_to_dict(row))

    if not any(team_rows.values()):
        offense: List[Mapping[str, object]] = []
        defense: List[Mapping[str, object]] = []
        for row in rows:
            side = _normalize_side(row.possession_side)
            if side == "offense":
                offense.append(_row_to_dict(row))
            elif side == "defense":
                defense.append(_row_to_dict(row))
        team_rows["crimson"] = offense
        team_rows["white"] = defense

    results: Dict[str, object] = {}
    for key in _PRACTICE_TEAM_KEYS:
        bucket_rows = team_rows.get(key, [])
        results[key] = _aggregate_rows(bucket_rows, events)
    return results


def compute_advanced_possession_game(game_id: int) -> Dict[str, object]:
    rows = (
        db.session.query(
            Possession.id.label("id"),
            Possession.paint_touches.label("paint_touches"),
            Possession.shot_clock.label("shot_clock"),
            Possession.shot_clock_pt.label("shot_clock_pt"),
            Possession.possession_type.label("possession_type"),
            Possession.points_scored.label("points_scored"),
            Possession.drill_labels.label("drill_labels"),
        )
        .filter(
            Possession.game_id == game_id,
            func.lower(Possession.possession_side) == "offense",
        )
        .all()
    )
    events = _fetch_event_counts(row.id for row in rows)
    payload = _aggregate_rows([_row_to_dict(row) for row in rows], events)
    return {"offense": payload}


def _get_cache_backend():
    try:
        app = current_app._get_current_object()
    except RuntimeError:  # pragma: no cover - only when called outside app context
        return None

    backend = None
    with suppress(AttributeError):
        backend = app.extensions.get("cache")  # type: ignore[assignment]
    if backend and backend.__class__.__name__ != _IN_MEMORY_BACKEND:
        return backend
    return None


def _cache_key_practice(practice_id: int) -> str:
    return f"adv_poss:practice:{practice_id}"


def _cache_key_game(game_id: int) -> str:
    return f"adv_poss:game:{game_id}"


def _store_cache_value(key: str, payload: Dict[str, object]) -> None:
    backend = _get_cache_backend()
    if backend is not None:
        try:
            backend.set(key, payload, timeout=_CACHE_TTL_SECONDS)
            return
        except Exception:  # pragma: no cover - backend failures are logged
            _LOGGER.exception("Failed to write advanced possession payload to cache backend.")
    # fall back to in-process dicts handled by caller


def _read_cache_value(key: str) -> Optional[Dict[str, object]]:
    backend = _get_cache_backend()
    if backend is None:
        return None
    try:
        value = backend.get(key)
    except Exception:  # pragma: no cover - backend failures are logged
        _LOGGER.exception("Failed to read advanced possession payload from cache backend.")
        return None
    if isinstance(value, dict):
        return value
    return None


def _delete_cache_value(key: str) -> None:
    backend = _get_cache_backend()
    if backend is None:
        return
    try:
        backend.delete(key)
    except Exception:  # pragma: no cover - backend failures are logged
        _LOGGER.exception("Failed to delete advanced possession payload from cache backend.")


def cache_get_or_compute_adv_poss_practice(practice_id: int):
    key = _cache_key_practice(practice_id)
    cached = _read_cache_value(key)
    if cached:
        meta = dict(cached.get("meta", {}))
        meta["source"] = "cache"
        return cached.get("data"), meta

    entry = _PRACTICE_CACHE.get(practice_id)
    if entry:
        meta = dict(entry["meta"])
        meta["source"] = "cache"
        return entry["data"], meta

    data = compute_advanced_possession_practice(practice_id)
    meta = {"source": "compute", "updated_at": _utc_now_iso(), "id": practice_id}
    payload = {"data": data, "meta": meta}
    _store_cache_value(key, payload)
    _PRACTICE_CACHE[practice_id] = payload
    return data, dict(meta)


def cache_get_or_compute_adv_poss_game(game_id: int):
    key = _cache_key_game(game_id)
    cached = _read_cache_value(key)
    if cached:
        meta = dict(cached.get("meta", {}))
        meta["source"] = "cache"
        return cached.get("data"), meta

    entry = _GAME_CACHE.get(game_id)
    if entry:
        meta = dict(entry["meta"])
        meta["source"] = "cache"
        return entry["data"], meta

    data = compute_advanced_possession_game(game_id)
    meta = {"source": "compute", "updated_at": _utc_now_iso(), "id": game_id}
    payload = {"data": data, "meta": meta}
    _store_cache_value(key, payload)
    _GAME_CACHE[game_id] = payload
    return data, dict(meta)


def invalidate_adv_poss_practice(practice_id: int) -> None:
    _PRACTICE_CACHE.pop(practice_id, None)
    _delete_cache_value(_cache_key_practice(practice_id))


def invalidate_adv_poss_game(game_id: int) -> None:
    _GAME_CACHE.pop(game_id, None)
    _delete_cache_value(_cache_key_game(game_id))
