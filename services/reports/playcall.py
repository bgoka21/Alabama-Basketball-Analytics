"""Playcall report computations and caching."""

# BEGIN Playcall Report
from __future__ import annotations

import logging
import math
import os
import re
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, Iterable, Mapping, MutableMapping, Optional, Tuple

import pandas as pd
from flask import current_app

from models.database import Game
_LOGGER = logging.getLogger(__name__)
_CACHE_TTL_SECONDS = 60 * 60  # 1 hour
_IN_MEMORY_CACHE: Dict[int, Dict[str, object]] = {}


_FLOW_PREFIX_PATTERN = re.compile(r"^\s*flow\s*[–-]\s*", flags=re.IGNORECASE)


def _normalize_flow_playcall_label(label: object) -> str:
    """Normalize a FLOW playcall label for consistent aggregation."""

    if not isinstance(label, str):
        if label is None:
            return ""
        label = str(label)
    trimmed = label.strip()
    normalized = _FLOW_PREFIX_PATTERN.sub("", trimmed)
    return normalized.strip()


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

    base_label: Optional[str] = None
    in_flow = False
    for tok in tokens:
        upper_tok = tok.upper()
        if upper_tok == "FLOW":
            in_flow = True
        elif base_label is None:
            base_label = tok

    if base_label is None:
        if in_flow:
            base_label = next((tok for tok in tokens if tok.upper() == "FLOW"), "FLOW")
        else:
            base_label = tokens[0]

    base_label = base_label or "UNKNOWN"
    flow_only = in_flow and len(tokens) == 1 and base_label.upper() == "FLOW"
    return base_label, in_flow, flow_only, tokens


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
        playcall_tokens = list(_extract_tokens(playcall_raw)) if playcall_raw else []
        primary_playcall = playcall_tokens[0] if playcall_tokens else ""
        playcall_label = primary_playcall or (playcall_raw or "UNKNOWN")
        has_playcall = bool(playcall_raw)
        team_val = _normalize_string(row_mapping.get("TEAM"))
        team_lower = team_val.lower()
        is_neutral = "neutral" in team_lower
        is_off_reb = "off reb" in team_lower

        points = _row_points(row_mapping, player_cols)

        family_label = series_value or "UNKNOWN"
        normalized_family_upper = family_label.upper() if isinstance(family_label, str) else ""
        if normalized_family_upper == "UKNOWN":
            if not playcall_label or playcall_label.upper() == "UNKNOWN":
                continue
            family_label = "MISC"

        chance_increment = 0
        if has_playcall and not is_neutral:
            # Treat Off Reb rows as valid chances whenever a playcall exists.
            chance_increment = 1

        family_key = None if flow_only else family_label or "UNKNOWN"
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
            flow_playcall_label = _normalize_flow_playcall_label(playcall_label) or (
                playcall_label or "UNKNOWN"
            )
            if flow_playcall_label not in flow_map:
                flow_map[flow_playcall_label] = {
                    "playcall": flow_playcall_label,
                    "ran_in_flow": 0,
                    "in_flow": {"pts": 0, "chances": 0},
                }
            flow_entry = flow_map[flow_playcall_label]
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


def aggregate_playcall_reports(game_ids: Iterable[int]):
    """Aggregate multiple game-level playcall payloads into a combined view."""

    ordered_ids = []
    seen: set[int] = set()
    for raw_id in game_ids:
        try:
            game_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if game_id in seen:
            continue
        seen.add(game_id)
        ordered_ids.append(game_id)

    processed_ids: list[int] = []
    families_agg: "OrderedDict[str, Dict[str, object]]" = OrderedDict()
    flow_map: "OrderedDict[str, Dict[str, object]]" = OrderedDict()
    flow_only_chances = 0
    updated_at_values: list[str] = []

    for game_id in ordered_ids:
        try:
            raw_data, meta = cache_get_or_compute_playcall_report(game_id)
        except Exception:  # pragma: no cover - errors are logged and skipped
            _LOGGER.exception("Failed to load playcall report for aggregation (game_id=%s)", game_id)
            continue

        if not isinstance(raw_data, Mapping):
            continue

        processed_ids.append(game_id)

        if isinstance(meta, Mapping):
            updated_val = meta.get("updated_at")
            if isinstance(updated_val, str):
                updated_at_values.append(updated_val)

        series_payload = raw_data.get("series") if isinstance(raw_data, Mapping) else {}
        if not isinstance(series_payload, Mapping):
            series_payload = {}

        data_meta = raw_data.get("meta") if isinstance(raw_data, Mapping) else {}
        total_in_flow_meta = 0
        if isinstance(data_meta, Mapping):
            try:
                total_in_flow_meta = int(data_meta.get("total_chances_in_flow", 0) or 0)
            except (TypeError, ValueError):
                total_in_flow_meta = 0

        family_in_flow_sum = 0

        for family_name, payload in series_payload.items():
            if not isinstance(family_name, str):
                continue
            if not isinstance(payload, Mapping):
                continue
            if family_name == "FLOW":
                plays_payload = payload.get("plays")
                if isinstance(plays_payload, Iterable):
                    for entry in plays_payload:
                        if not isinstance(entry, Mapping):
                            continue
                        raw_playcall = entry.get("playcall", "")
                        playcall = _normalize_flow_playcall_label(raw_playcall)
                        if not playcall:
                            playcall = _normalize_string(raw_playcall) or "UNKNOWN"
                        if not playcall or playcall.upper() == "UNKNOWN":
                            continue
                        if playcall not in flow_map:
                            flow_map[playcall] = {
                                "playcall": playcall,
                                "ran_in_flow": 0,
                                "in_flow": {"pts": 0, "chances": 0},
                            }
                        flow_entry = flow_map[playcall]
                        flow_entry["ran_in_flow"] += int(entry.get("ran_in_flow", 0) or 0)
                        in_flow_bucket = entry.get("in_flow") if isinstance(entry.get("in_flow"), Mapping) else {}
                        if isinstance(in_flow_bucket, Mapping):
                            flow_entry["in_flow"]["pts"] += int(in_flow_bucket.get("pts", 0) or 0)
                            flow_entry["in_flow"]["chances"] += int(in_flow_bucket.get("chances", 0) or 0)
                continue

            family_upper = family_name.upper()
            treat_as_misc = family_upper in ("UKNOWN", "MISC")
            target_family_name = "MISC" if family_upper == "UKNOWN" else ("MISC" if family_upper == "MISC" else family_name)

            removed_totals = {
                "off_set": {"pts": 0, "chances": 0},
                "in_flow": {"pts": 0, "chances": 0},
            }

            filtered_entries: list[tuple[str, Mapping[str, object]]] = []
            source_plays = payload.get("plays") if isinstance(payload.get("plays"), Mapping) else {}
            if isinstance(source_plays, Mapping):
                for playcall, entry in source_plays.items():
                    if not isinstance(playcall, str) or not isinstance(entry, Mapping):
                        continue
                    trimmed_playcall = playcall.strip()
                    if treat_as_misc and (not trimmed_playcall or trimmed_playcall.upper() == "UNKNOWN"):
                        for bucket in ("off_set", "in_flow"):
                            bucket_payload = entry.get(bucket)
                            if isinstance(bucket_payload, Mapping):
                                removed_totals[bucket]["pts"] += int(bucket_payload.get("pts", 0) or 0)
                                removed_totals[bucket]["chances"] += int(bucket_payload.get("chances", 0) or 0)
                        continue
                    filtered_entries.append((playcall, entry))

            totals_payload = payload.get("totals") if isinstance(payload.get("totals"), Mapping) else {}
            normalized_totals = {
                "off_set": {"pts": 0, "chances": 0},
                "in_flow": {"pts": 0, "chances": 0},
            }
            if isinstance(totals_payload, Mapping):
                for bucket in ("off_set", "in_flow"):
                    bucket_payload = totals_payload.get(bucket)
                    pts_val = 0
                    chances_val = 0
                    if isinstance(bucket_payload, Mapping):
                        pts_val = int(bucket_payload.get("pts", 0) or 0)
                        chances_val = int(bucket_payload.get("chances", 0) or 0)
                    if treat_as_misc:
                        pts_val = max(0, pts_val - removed_totals[bucket]["pts"])
                        chances_val = max(0, chances_val - removed_totals[bucket]["chances"])
                    normalized_totals[bucket] = {"pts": pts_val, "chances": chances_val}

            has_family_data = bool(filtered_entries)
            if not has_family_data:
                for bucket in ("off_set", "in_flow"):
                    if normalized_totals[bucket]["pts"] or normalized_totals[bucket]["chances"]:
                        has_family_data = True
                        break
            if not has_family_data:
                continue

            if target_family_name not in families_agg:
                families_agg[target_family_name] = {
                    "plays": OrderedDict(),
                    "totals": {
                        "off_set": {"pts": 0, "chances": 0},
                        "in_flow": {"pts": 0, "chances": 0},
                    },
                }

            family_payload = families_agg[target_family_name]
            plays_map: MutableMapping[str, Dict[str, object]] = family_payload["plays"]  # type: ignore[assignment]

            for playcall, entry in filtered_entries:
                if playcall not in plays_map:
                    plays_map[playcall] = {
                        "ran": 0,
                        "off_set": {"pts": 0, "chances": 0},
                        "in_flow": {"pts": 0, "chances": 0},
                    }
                dest_entry = plays_map[playcall]
                dest_entry["ran"] += int(entry.get("ran", 0) or 0)
                for bucket in ("off_set", "in_flow"):
                    bucket_payload = entry.get(bucket)
                    if isinstance(bucket_payload, Mapping):
                        dest = dest_entry[bucket]
                        dest["pts"] += int(bucket_payload.get("pts", 0) or 0)
                        dest["chances"] += int(bucket_payload.get("chances", 0) or 0)

            for bucket in ("off_set", "in_flow"):
                bucket_totals = normalized_totals[bucket]
                family_bucket = family_payload["totals"][bucket]
                family_bucket["pts"] += bucket_totals["pts"]
                family_bucket["chances"] += bucket_totals["chances"]
                if bucket == "in_flow":
                    family_in_flow_sum += bucket_totals["chances"]

        if total_in_flow_meta and family_in_flow_sum <= total_in_flow_meta:
            flow_only_chances += total_in_flow_meta - family_in_flow_sum

    if not processed_ids:
        empty = _empty_payload()
        return empty, {
            "source": "aggregate",
            "scope": "season",
            "game_ids": [],
            "game_count": 0,
        }

    series_payload = OrderedDict()
    total_off_set_chances = 0
    total_in_flow_chances = 0

    for family_name, payload in families_agg.items():
        plays_map = OrderedDict()
        for playcall, entry in payload["plays"].items():
            ran_val = int(entry.get("ran", 0) or 0)
            formatted_entry = {
                "ran": ran_val,
                "off_set": {
                    "pts": int(entry["off_set"]["pts"]),
                    "chances": int(entry["off_set"]["chances"]),
                },
                "in_flow": {
                    "pts": int(entry["in_flow"]["pts"]),
                    "chances": int(entry["in_flow"]["chances"]),
                },
            }
            for bucket in ("off_set", "in_flow"):
                bucket_payload = formatted_entry[bucket]
                chances = bucket_payload["chances"]
                pts = bucket_payload["pts"]
                bucket_payload["ppc"] = round(pts / chances, 2) if chances else 0.0
            plays_map[playcall] = formatted_entry

        totals_payload = payload["totals"]
        formatted_totals = {"off_set": {}, "in_flow": {}}
        for bucket in ("off_set", "in_flow"):
            pts = int(totals_payload[bucket]["pts"])
            chances = int(totals_payload[bucket]["chances"])
            formatted_totals[bucket] = {
                "pts": pts,
                "chances": chances,
                "ppc": round(pts / chances, 2) if chances else 0.0,
            }
            if bucket == "off_set":
                total_off_set_chances += chances
            else:
                total_in_flow_chances += chances

        series_payload[family_name] = {
            "plays": dict(plays_map),
            "totals": formatted_totals,
        }

    flow_entries = []
    flow_total_pts = 0
    flow_total_chances = 0
    for playcall, entry in flow_map.items():
        pts = int(entry["in_flow"]["pts"])
        chances = int(entry["in_flow"]["chances"])
        flow_total_pts += pts
        flow_total_chances += chances
        flow_entries.append(
            {
                "playcall": playcall,
                "ran_in_flow": int(entry.get("ran_in_flow", 0) or 0),
                "in_flow": {
                    "pts": pts,
                    "chances": chances,
                    "ppc": round(pts / chances, 2) if chances else 0.0,
                },
            }
        )

    flow_totals = {
        "in_flow": {
            "pts": flow_total_pts,
            "chances": flow_total_chances,
            "ppc": round(flow_total_pts / flow_total_chances, 2) if flow_total_chances else 0.0,
        }
    }

    total_in_flow_chances += flow_only_chances

    series_payload["FLOW"] = {"plays": flow_entries, "totals": flow_totals}

    data = {
        "series": dict(series_payload),
        "meta": {
            "total_chances_off_set": int(total_off_set_chances),
            "total_chances_in_flow": int(total_in_flow_chances),
        },
    }

    display_meta: Dict[str, object] = {
        "source": "aggregate",
        "scope": "season",
        "game_ids": processed_ids,
        "game_count": len(processed_ids),
    }
    if updated_at_values:
        display_meta["updated_at"] = max(updated_at_values)

    return data, display_meta


def invalidate_playcall_report(game_id: int) -> None:
    _IN_MEMORY_CACHE.pop(game_id, None)
    _delete_cache_value(_cache_key(game_id))

# END Playcall Report

