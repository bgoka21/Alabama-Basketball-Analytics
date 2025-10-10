"""Playcall report computations and caching."""

# BEGIN Playcall Report
from __future__ import annotations

import logging
import math
import os
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, Iterable, Mapping, MutableMapping, Optional, Tuple

import pandas as pd
from flask import current_app

from models.database import Game
_LOGGER = logging.getLogger(__name__)
_CACHE_TTL_SECONDS = 60 * 60  # 1 hour
_IN_MEMORY_CACHE: Dict[int, Dict[str, object]] = {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _get_cache_backend():
    try:
        app = current_app._get_current_object()
    except RuntimeError:  # pragma: no cover - outside app context
        return None
    return app.extensions.get("cache") if app else None


def _cache_key(game_id: int) -> str:
    return f"playcall-report:{game_id}"


def _store_cache_value(key: str, payload: Dict[str, object]) -> None:
    backend = _get_cache_backend()
    if backend is None:
        return
    try:
        backend.set(key, payload, timeout=_CACHE_TTL_SECONDS)
    except Exception:  # pragma: no cover - backend failures are logged
        _LOGGER.exception("Failed to store playcall report payload in cache backend.")


def _read_cache_value(key: str) -> Optional[Dict[str, object]]:
    backend = _get_cache_backend()
    if backend is None:
        return None
    try:
        value = backend.get(key)
    except Exception:  # pragma: no cover - backend failures are logged
        _LOGGER.exception("Failed to read playcall report payload from cache backend.")
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
        _LOGGER.exception("Failed to delete playcall report payload from cache backend.")


def _normalize_string(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    try:
        if isinstance(value, (float, int)) and math.isnan(value):
            return ""
    except TypeError:
        pass
    if pd.isna(value):  # type: ignore[arg-type]
        return ""
    return str(value).strip()


def _series_tokens(value: object) -> Tuple[str, bool, bool, Iterable[str]]:
    tokens = [tok.strip() for tok in _normalize_string(value).split(",") if tok.strip()]
    if not tokens:
        return "UNKNOWN", False, False, ()
    in_flow = any(tok.upper() == "FLOW" for tok in tokens)
    base = tokens[0]
    flow_only = in_flow and len(tokens) == 1 and base.upper() == "FLOW"
    return base, in_flow, flow_only, tokens


def _player_columns(df: pd.DataFrame) -> Iterable[str]:
    return [col for col in df.columns if isinstance(col, str) and col.startswith("#")]


def _extract_tokens(cell_value: object) -> Iterable[str]:
    if pd.isna(cell_value) or not isinstance(cell_value, str):
        return []
    return [token.strip().replace("–", "-") for token in cell_value.split(",") if token.strip()]


def _row_points(row: Mapping[str, object], player_cols: Iterable[str]) -> int:
    points = 0
    for col in player_cols:
        tokens = _extract_tokens(row.get(col, ""))
        for token in tokens:
            upper = token.upper()
            if upper in ("ATR+", "2FG+"):
                points += 2
            elif upper == "3FG+":
                points += 3
            elif upper == "FT+":
                points += 1
    return points


def _empty_payload() -> Dict[str, object]:
    return {
        "series": {
            "FLOW": {
                "plays": [],
                "totals": {"in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}},
            }
        },
        "meta": {"total_chances_off_set": 0, "total_chances_in_flow": 0},
    }


def _compute_from_dataframe(df: pd.DataFrame) -> Dict[str, object]:
    if df is None or df.empty:
        return _empty_payload()

    row_series = df.get("Row")
    if row_series is None:
        return _empty_payload()

    offense_rows = df[row_series == "Offense"]
    if offense_rows.empty:
        return _empty_payload()

    player_cols = list(_player_columns(offense_rows))

    families: "OrderedDict[str, Dict[str, object]]" = OrderedDict()
    flow_map: "OrderedDict[str, Dict[str, object]]" = OrderedDict()
    flow_only_totals = {"pts": 0, "chances": 0}

    for _, row in offense_rows.iterrows():
        row_mapping: Mapping[str, object] = row.to_dict()
        series_value, in_flow, flow_only, _tokens = _series_tokens(row_mapping.get("SERIES"))
        playcall_raw = _normalize_string(row_mapping.get("PLAYCALL"))
        has_playcall = bool(playcall_raw)
        playcall_label = playcall_raw or "UNKNOWN"
        team_val = _normalize_string(row_mapping.get("TEAM"))
        team_lower = team_val.lower()
        is_neutral = "neutral" in team_lower
        is_off_reb = "off reb" in team_lower

        points = _row_points(row_mapping, player_cols)

        chance_increment = 0
        if has_playcall and not is_neutral:
            # Treat Off Reb rows as valid chances whenever a playcall exists.
            chance_increment = 1

        family_key = None if flow_only else series_value or "UNKNOWN"
        if family_key:
            if family_key not in families:
                families[family_key] = {
                    "plays": OrderedDict(),
                    "totals": {
                        "off_set": {"pts": 0, "chances": 0},
                        "in_flow": {"pts": 0, "chances": 0},
                    },
                }
            family_payload = families[family_key]
            plays_map: MutableMapping[str, Dict[str, object]] = family_payload["plays"]  # type: ignore[assignment]
            if playcall_label not in plays_map:
                plays_map[playcall_label] = {
                    "ran": 0,
                    "off_set": {"pts": 0, "chances": 0},
                    "in_flow": {"pts": 0, "chances": 0},
                }
            play_payload = plays_map[playcall_label]
            play_payload["ran"] += 1
            if not is_neutral:
                bucket = "in_flow" if in_flow else "off_set"
                play_payload[bucket]["pts"] += points
                play_payload[bucket]["chances"] += chance_increment
                family_totals = family_payload["totals"][bucket]
                family_totals["pts"] += points
                family_totals["chances"] += chance_increment
        # Flow aggregation (includes pure FLOW and X, FLOW entries)
        if in_flow:
            if playcall_label not in flow_map:
                flow_map[playcall_label] = {
                    "playcall": playcall_label,
                    "ran_in_flow": 0,
                    "in_flow": {"pts": 0, "chances": 0},
                }
            flow_entry = flow_map[playcall_label]
            flow_entry["ran_in_flow"] += 1
            if not is_neutral:
                flow_entry["in_flow"]["pts"] += points
                flow_entry["in_flow"]["chances"] += chance_increment
            if flow_only and not is_neutral:
                flow_only_totals["pts"] += points
                flow_only_totals["chances"] += chance_increment

    # Finalize calculations: compute PPC values and ensure ints
    for family_payload in families.values():
        plays_map = family_payload["plays"]
        for entry in plays_map.values():
            entry["ran"] = int(entry.get("ran", 0))
            for bucket in ("off_set", "in_flow"):
                pts = entry[bucket]["pts"]
                chances = entry[bucket]["chances"]
                entry[bucket]["pts"] = int(pts)
                entry[bucket]["chances"] = int(chances)
                entry[bucket]["ppc"] = round(pts / chances, 2) if chances else 0.0
        totals = family_payload["totals"]
        for bucket in ("off_set", "in_flow"):
            pts = totals[bucket]["pts"]
            chances = totals[bucket]["chances"]
            totals[bucket]["pts"] = int(pts)
            totals[bucket]["chances"] = int(chances)
            totals[bucket]["ppc"] = round(pts / chances, 2) if chances else 0.0

    flow_list = []
    flow_total_pts = 0
    flow_total_chances = 0
    for entry in flow_map.values():
        entry["ran_in_flow"] = int(entry.get("ran_in_flow", 0))
        pts = int(entry["in_flow"].get("pts", 0))
        chances = int(entry["in_flow"].get("chances", 0))
        entry["in_flow"]["pts"] = pts
        entry["in_flow"]["chances"] = chances
        entry["in_flow"]["ppc"] = round(pts / chances, 2) if chances else 0.0
        flow_total_pts += pts
        flow_total_chances += chances
        flow_list.append(entry)

    flow_totals = {
        "in_flow": {
            "pts": int(flow_total_pts),
            "chances": int(flow_total_chances),
            "ppc": round(flow_total_pts / flow_total_chances, 2) if flow_total_chances else 0.0,
        }
    }

    series_payload = OrderedDict()
    for family_key, payload in families.items():
        series_payload[family_key] = {
            "plays": dict(payload["plays"]),
            "totals": payload["totals"],
        }
    series_payload["FLOW"] = {"plays": flow_list, "totals": flow_totals}

    total_off_set_chances = sum(
        payload["totals"]["off_set"]["chances"] for payload in families.values()
    )
    total_in_flow_chances = sum(
        payload["totals"]["in_flow"]["chances"] for payload in families.values()
    ) + int(flow_only_totals["chances"])

    return {
        "series": dict(series_payload),
        "meta": {
            "total_chances_off_set": int(total_off_set_chances),
            "total_chances_in_flow": int(total_in_flow_chances),
        },
    }


def _persist_payload(game_id: int, data: Dict[str, object]):
    meta = {"source": "compute", "updated_at": _utc_now_iso(), "id": game_id}
    payload = {"data": data, "meta": meta}
    key = _cache_key(game_id)
    _store_cache_value(key, payload)
    _IN_MEMORY_CACHE[game_id] = payload
    return data, dict(meta)


def compute_playcall_report_from_dataframe(game_id: int, df: pd.DataFrame):
    data = _compute_from_dataframe(df)
    return _persist_payload(game_id, data)


def _load_dataframe_for_game(game_id: int) -> pd.DataFrame:
    game = Game.query.get(game_id)
    if not game or not game.csv_filename:
        return pd.DataFrame()
    upload_folder = current_app.config.get("UPLOAD_FOLDER")
    if not upload_folder:
        return pd.DataFrame()
    csv_path = os.path.join(upload_folder, game.csv_filename)
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    try:
        return pd.read_csv(csv_path)
    except Exception:  # pragma: no cover - pandas may raise
        _LOGGER.exception("Failed to load game CSV for playcall report (game_id=%s)", game_id)
        return pd.DataFrame()


def compute_playcall_report(game_id: int):
    df = _load_dataframe_for_game(game_id)
    data = _compute_from_dataframe(df)
    return _persist_payload(game_id, data)


def cache_get_or_compute_playcall_report(game_id: int):
    key = _cache_key(game_id)
    cached = _read_cache_value(key)
    if cached:
        meta = dict(cached.get("meta", {}))
        meta["source"] = "cache"
        return cached.get("data"), meta

    entry = _IN_MEMORY_CACHE.get(game_id)
    if entry:
        meta = dict(entry["meta"])
        meta["source"] = "cache"
        return entry["data"], meta

    data, meta = compute_playcall_report(game_id)
    return data, meta


def invalidate_playcall_report(game_id: int) -> None:
    _IN_MEMORY_CACHE.pop(game_id, None)
    _delete_cache_value(_cache_key(game_id))

# END Playcall Report

