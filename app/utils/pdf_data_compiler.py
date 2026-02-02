"""Compile player shot data for PDF generation.

These helpers read existing PlayerStats/shot_type_details data without mutating
the database, returning the structured payload needed for PDF reporting.
"""

from __future__ import annotations

from collections import defaultdict
import json
from typing import Any, Iterable, Mapping

from models.database import PlayerStats, Season
from routes import _load_shot_type_details
from utils.shot_location_map import normalize_shot_location
from utils.shottype import gather_labels_for_shot


_SHOT_CLASS_MAP = {
    "atr+": "atr",
    "atr-": "atr",
    "2fg+": "2fg",
    "2fg-": "2fg",
    "3fg+": "3fg",
    "3fg-": "3fg",
}


def _normalize_shot_class(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return _SHOT_CLASS_MAP.get(text, text)


def _normalize_possession_type(value: Any) -> str:
    if value is None:
        return "half_court"
    text = str(value).strip().lower()
    if not text:
        return "half_court"
    if "trans" in text:
        return "transition"
    if "half" in text or "hc" in text:
        return "half_court"
    return "half_court"


def _slugify(value: Any) -> str:
    text = "".join(
        ch if ch.isalnum() else "_"
        for ch in str(value or "").strip().lower()
    )
    return "_".join(token for token in text.split("_") if token)


def _shot_points(shot_class: str) -> int:
    if shot_class == "3fg":
        return 3
    if shot_class in {"atr", "2fg"}:
        return 2
    return 0


def compile_player_shot_data(player, db_session):
    """Return full player shot report payload based on PlayerStats details."""
    player_name = getattr(player, "player_name", None) or "Unknown"
    season_name = None
    if getattr(player, "season_id", None):
        season_name = (
            db_session.query(Season.season_name)
            .filter(Season.id == player.season_id)
            .scalar()
        )

    stats_rows = (
        db_session.query(PlayerStats)
        .filter(PlayerStats.player_name == player_name)
        .all()
    )

    ftm = sum((row.ftm or 0) for row in stats_rows)
    fta = sum((row.fta or 0) for row in stats_rows)
    atr_makes = sum((row.atr_makes or 0) for row in stats_rows)
    atr_attempts = sum((row.atr_attempts or 0) for row in stats_rows)
    fg2_makes = sum((row.fg2_makes or 0) for row in stats_rows)
    fg2_attempts = sum((row.fg2_attempts or 0) for row in stats_rows)
    fg3_makes = sum((row.fg3_makes or 0) for row in stats_rows)
    fg3_attempts = sum((row.fg3_attempts or 0) for row in stats_rows)
    points = sum((row.points or 0) for row in stats_rows)

    shot_details = _collect_shot_details(stats_rows)

    total_fga = atr_attempts + fg2_attempts + fg3_attempts
    ft_pct = round((ftm / fta) * 100, 1) if fta else 0.0
    efg = ((atr_makes + fg2_makes) + 1.5 * fg3_makes) / total_fga if total_fga else 0.0
    efg_pct = round(efg * 100, 1) if total_fga else 0.0
    ts_denom = (2 * (total_fga + 0.44 * fta)) if total_fga or fta else 0.0
    ts_pct = round((points / ts_denom) * 100, 1) if ts_denom else 0.0
    pps = round(points / total_fga, 2) if total_fga else 0.0

    return {
        "name": player_name,
        "number": _extract_jersey_number(player_name),
        "season": season_name or "",
        "ft_pct": ft_pct,
        "ts_pct": ts_pct,
        "pps": pps,
        "efg_pct": efg_pct,
        "atr": _compile_shot_type_data(shot_details, "atr"),
        "2fg": _compile_shot_type_data(shot_details, "2fg"),
        "3fg": _compile_shot_type_data(shot_details, "3fg"),
    }


def _load_shot_type_details(raw_value: Any) -> list[dict[str, Any]]:
    """Load shot_type_details JSON blobs into a list of dicts."""
    if not raw_value:
        return []
    try:
        data = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, ValueError):
        return []
    if isinstance(data, list):
        return [dict(item) for item in data if isinstance(item, Mapping)]
    if isinstance(data, Mapping):
        return [dict(data)]
    return []


def _collect_shot_details(stats_rows: Iterable[PlayerStats]) -> list[dict[str, Any]]:
    shots: list[dict[str, Any]] = []
    for row in stats_rows:
        shots.extend(_load_shot_type_details(getattr(row, "shot_type_details", None)))
    return shots


def _compile_shot_type_data(shots: Iterable[Mapping[str, Any]], shot_type: str):
    filtered = [
        shot
        for shot in shots
        if _normalize_shot_class(shot.get("shot_class")) == shot_type
    ]

    return {
        "total": _calculate_situation_stats(filtered, "total"),
        "transition": _calculate_situation_stats(filtered, "transition"),
        "half_court": _calculate_situation_stats(filtered, "half_court"),
        "shot_charts": {
            "total": _get_zone_data(filtered),
            "transition": _get_zone_data(
                [shot for shot in filtered if _normalize_possession_type(shot.get("possession_type")) == "transition"]
            ),
            "half_court": _get_zone_data(
                [shot for shot in filtered if _normalize_possession_type(shot.get("possession_type")) == "half_court"]
            ),
        },
        "breakdown": _get_breakdown_data(filtered),
    }


def _calculate_situation_stats(shots: Iterable[Mapping[str, Any]], situation: str):
    shots = list(shots)
    total_attempts = len(shots)

    if situation == "total":
        filtered = shots
    else:
        filtered = [
            shot for shot in shots
            if _normalize_possession_type(shot.get("possession_type")) == situation
        ]

    attempts = len(filtered)
    made = sum(1 for shot in filtered if (shot.get("result") or "").lower() == "made")
    points = sum(_shot_points(_normalize_shot_class(shot.get("shot_class")) or "") for shot in filtered)

    fg_pct = round((made / attempts) * 100, 1) if attempts else 0.0
    pps = round(points / attempts, 2) if attempts else 0.0
    freq = round((attempts / total_attempts) * 100, 1) if total_attempts else 0.0
    return {
        "fga": f"{made}-{attempts}",
        "fg_pct": fg_pct,
        "pps": pps,
        "freq": freq,
    }


def _get_zone_data(shots: Iterable[Mapping[str, Any]]):
    zone_totals: dict[str, dict[str, int]] = defaultdict(lambda: {"made": 0, "attempts": 0})
    for shot in shots:
        zone = normalize_shot_location(shot.get("shot_location"))
        zone_totals[zone]["attempts"] += 1
        if (shot.get("result") or "").lower() == "made":
            zone_totals[zone]["made"] += 1

    zone_payload: dict[str, dict[str, float | int]] = {}
    for zone, totals in zone_totals.items():
        attempts = totals["attempts"]
        made = totals["made"]
        pct = round((made / attempts) * 100, 1) if attempts else 0.0
        zone_payload[zone] = {"made": made, "attempts": attempts, "pct": pct}
    return zone_payload


def _get_breakdown_data(filtered: Iterable[Mapping[str, Any]]):
    filtered = list(filtered)

    total_attempts = len(filtered)
    breakdown_map: dict[str, dict[str, list[Mapping[str, Any]]]] = defaultdict(
        lambda: {"total": [], "transition": [], "half_court": []}
    )

    for shot in filtered:
        labels = gather_labels_for_shot(shot)
        situation = _normalize_possession_type(shot.get("possession_type"))
        for label in labels:
            key = _slugify(label)
            if not key:
                continue
            breakdown_map[key]["total"].append(shot)
            breakdown_map[key][situation].append(shot)

    breakdown_payload = {}
    for label_key, buckets in breakdown_map.items():
        breakdown_payload[label_key] = {
            "total": _calculate_bucket_stats(buckets["total"], total_attempts),
            "transition": _calculate_bucket_stats(buckets["transition"], total_attempts),
            "half_court": _calculate_bucket_stats(buckets["half_court"], total_attempts),
        }

    return breakdown_payload


def _calculate_bucket_stats(shots: Iterable[Mapping[str, Any]], total_attempts: int):
    shots = list(shots)
    attempts = len(shots)
    made = sum(1 for shot in shots if (shot.get("result") or "").lower() == "made")
    points = sum(_shot_points(_normalize_shot_class(shot.get("shot_class")) or "") for shot in shots)

    fg_pct = round((made / attempts) * 100, 1) if attempts else 0.0
    pps = round(points / attempts, 2) if attempts else 0.0
    freq = round((attempts / total_attempts) * 100, 1) if total_attempts else 0.0
    return {
        "fga": f"{made}-{attempts}",
        "fg_pct": fg_pct,
        "pps": pps,
        "freq": freq,
    }


def _extract_jersey_number(player_name: str | None) -> str:
    if not player_name:
        return ""
    text = player_name.strip()
    if text.startswith("#"):
        text = text[1:]
    number = ""
    for ch in text:
        if ch.isdigit():
            number += ch
        else:
            break
    return number
