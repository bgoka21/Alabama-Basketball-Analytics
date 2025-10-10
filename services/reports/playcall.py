"""Playcall report computation and caching utilities."""

from __future__ import annotations

from collections import OrderedDict
from contextlib import suppress
from datetime import datetime, timezone
import os
from typing import Any, Dict, Iterable, Tuple

import pandas as pd
from flask import current_app

from models.database import Game

_UNKNOWN_FAMILY = "UNKNOWN"
_FLOW_FAMILY = "FLOW"
_CACHE_FALLBACK: Dict[str, Dict[str, Any]] = {}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    with suppress(TypeError, ValueError):
        if pd.isna(value):  # type: ignore[attr-defined]
            return ""
    return str(value).strip() if value is not None else ""


def _extract_tokens(value: Any) -> Iterable[str]:
    text = _safe_str(value)
    if not text:
        return []
    return [token.strip() for token in text.split(",") if token.strip()]


def _row_points(row: pd.Series, columns: Iterable[str]) -> int:
    points = 0
    for col in columns:
        if not str(col).startswith("#"):
            continue
        for token in _extract_tokens(row.get(col)):
            token_upper = token.upper()
            if token_upper in {"ATR+", "2FG+"}:
                points += 2
            elif token_upper == "3FG+":
                points += 3
            elif token_upper == "FT+":
                points += 1
    return points


def _is_neutral(team_value: Any) -> bool:
    return "NEUTRAL" in _safe_str(team_value).upper()


def _classify_series(raw: Any) -> Tuple[str, str, bool, bool]:
    """Return ``(family, bucket, include_family, include_flow_table)``."""

    parts = [part.strip() for part in _safe_str(raw).split(",") if part.strip()]
    if not parts:
        return _UNKNOWN_FAMILY, "off_set", True, False

    has_flow = any(part.upper() == "FLOW" for part in parts)
    base = next((part for part in parts if part.upper() != "FLOW"), None)
    if not base:
        base = _FLOW_FAMILY

    family = base or _UNKNOWN_FAMILY
    if not family:
        family = _UNKNOWN_FAMILY

    family_upper = family.upper()
    include_family = family_upper != _FLOW_FAMILY
    bucket = "in_flow" if has_flow else "off_set"
    include_flow = has_flow or family_upper == _FLOW_FAMILY

    if family_upper == _FLOW_FAMILY:
        bucket = "in_flow"
        include_family = False
        include_flow = True

    return family, bucket, include_family, include_flow


def _compute_ppc(points: int, chances: int) -> float:
    if not chances:
        return 0.0
    return round(points / chances, 2)


def build_playcall_report_from_frame(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty or "Row" not in df.columns:
        return {
            "series": {
                _FLOW_FAMILY: {
                    "plays": [],
                    "totals": {"in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}},
                }
            },
            "meta": {"total_chances_off_set": 0, "total_chances_in_flow": 0},
        }

    offense_rows = df[df["Row"] == "Offense"]
    columns = list(df.columns)

    families: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    flow_plays: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    standalone_flow_chances = 0

    for _, row in offense_rows.iterrows():
        playcall_raw = row.get("PLAYCALL")
        playcall_text = _safe_str(playcall_raw)
        has_playcall = bool(playcall_text)
        if not playcall_text:
            playcall_text = "—"

        family, bucket, include_family, include_flow = _classify_series(row.get("SERIES"))
        family_key = family or _UNKNOWN_FAMILY

        is_neutral = _is_neutral(row.get("TEAM"))
        points = _row_points(row, columns)

        if include_family:
            family_entry = families.setdefault(
                family_key,
                {
                    "plays": OrderedDict(),
                    "totals": {
                        "off_set": {"pts": 0, "chances": 0, "ppc": 0.0},
                        "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0},
                    },
                },
            )
            plays = family_entry["plays"]
            play_entry = plays.setdefault(
                playcall_text,
                {
                    "ran": 0,
                    "off_set": {"pts": 0, "chances": 0, "ppc": 0.0},
                    "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0},
                },
            )
            play_entry["ran"] += 1
            if not is_neutral:
                target = play_entry[bucket]
                target["pts"] += points
                if has_playcall:
                    target["chances"] += 1

        if include_flow:
            flow_entry = flow_plays.setdefault(
                playcall_text,
                {
                    "playcall": playcall_text,
                    "ran_in_flow": 0,
                    "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0},
                },
            )
            flow_entry["ran_in_flow"] += 1
            if not is_neutral:
                flow_entry["in_flow"]["pts"] += points
                if has_playcall:
                    flow_entry["in_flow"]["chances"] += 1
                    if not include_family:
                        standalone_flow_chances += 1

    total_chances_off = 0
    total_chances_flow = 0

    for family_entry in families.values():
        totals = family_entry["totals"]
        totals_off = totals["off_set"]
        totals_in = totals["in_flow"]
        totals_off["pts"] = 0
        totals_off["chances"] = 0
        totals_in["pts"] = 0
        totals_in["chances"] = 0
        family_total_ran = 0

        for play_entry in family_entry["plays"].values():
            off = play_entry["off_set"]
            inflow = play_entry["in_flow"]
            off["ppc"] = _compute_ppc(off["pts"], off["chances"])
            inflow["ppc"] = _compute_ppc(inflow["pts"], inflow["chances"])
            totals_off["pts"] += off["pts"]
            totals_off["chances"] += off["chances"]
            totals_in["pts"] += inflow["pts"]
            totals_in["chances"] += inflow["chances"]
            family_total_ran += play_entry.get("ran", 0)

        totals_off["ppc"] = _compute_ppc(totals_off["pts"], totals_off["chances"])
        totals_in["ppc"] = _compute_ppc(totals_in["pts"], totals_in["chances"])
        totals["ran"] = family_total_ran

        total_chances_off += totals_off["chances"]
        total_chances_flow += totals_in["chances"]

    flow_totals = {"in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}}
    flow_total_ran = 0
    for flow_entry in flow_plays.values():
        inflow = flow_entry["in_flow"]
        inflow["ppc"] = _compute_ppc(inflow["pts"], inflow["chances"])
        flow_totals["in_flow"]["pts"] += inflow["pts"]
        flow_totals["in_flow"]["chances"] += inflow["chances"]
        flow_total_ran += flow_entry.get("ran_in_flow", 0)

    flow_totals["in_flow"]["ppc"] = _compute_ppc(
        flow_totals["in_flow"]["pts"], flow_totals["in_flow"]["chances"]
    )
    flow_totals["ran_in_flow"] = flow_total_ran

    series_payload: "OrderedDict[str, Any]" = OrderedDict()
    for name, entry in families.items():
        series_payload[name] = {
            "plays": entry["plays"],
            "totals": entry["totals"],
        }

    series_payload[_FLOW_FAMILY] = {
        "plays": list(flow_plays.values()),
        "totals": flow_totals,
    }

    return {
        "series": series_payload,
        "meta": {
            "total_chances_off_set": total_chances_off,
            "total_chances_in_flow": total_chances_flow + standalone_flow_chances,
        },
    }


def compute_playcall_report(game_id: int) -> Dict[str, Any]:
    if not game_id:
        raise ValueError("game_id is required")

    game = Game.query.get(game_id)
    if game is None:
        raise LookupError(f"Game {game_id} not found")

    csv_filename = game.csv_filename
    if not csv_filename:
        raise LookupError(f"Game {game_id} is missing a CSV filename")

    base_dir = current_app.config.get("UPLOAD_FOLDER")
    if not base_dir:
        raise RuntimeError("UPLOAD_FOLDER is not configured")

    csv_path = os.path.join(base_dir, csv_filename)
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    frame = pd.read_csv(csv_path)
    return build_playcall_report_from_frame(frame)


def cache_key_playcall_report(game_id: int) -> str:
    return f"playcall_report:{int(game_id)}"


def _get_cache_backend() -> Any:
    try:
        app = current_app._get_current_object()
    except RuntimeError:
        return None
    return getattr(app, "extensions", {}).get("cache")


def _cache_get(key: str) -> Dict[str, Any] | None:
    backend = _get_cache_backend()
    if backend is not None:
        with suppress(Exception):
            cached = backend.get(key)
            if isinstance(cached, dict):
                return cached
    return _CACHE_FALLBACK.get(key)


def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    backend = _get_cache_backend()
    if backend is not None:
        with suppress(Exception):
            backend.set(key, payload)
    _CACHE_FALLBACK[key] = payload


def cache_get_or_compute_playcall_report(game_id: int) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    key = cache_key_playcall_report(game_id)
    cached = _cache_get(key)
    if cached:
        meta = dict(cached.get("meta") or {})
        meta["source"] = "cache"
        meta.setdefault("updated_at", _utcnow_iso())
        meta.setdefault("id", int(game_id))
        return cached.get("data") or {}, meta

    data = compute_playcall_report(game_id)
    meta = store_playcall_report(game_id, data)
    return data, meta


def store_playcall_report(game_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
    meta = {"source": "compute", "updated_at": _utcnow_iso(), "id": int(game_id)}
    payload = {"data": data, "meta": meta}
    key = cache_key_playcall_report(game_id)
    _cache_set(key, payload)
    return meta


def invalidate_playcall_report_cache(game_id: int) -> None:
    key = cache_key_playcall_report(game_id)
    backend = _get_cache_backend()
    if backend is not None:
        with suppress(Exception):
            backend.delete(key)
    _CACHE_FALLBACK.pop(key, None)
