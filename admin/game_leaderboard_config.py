"""Shared configuration for game leaderboard table rendering."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

ConfigMap = Dict[str, Any]


def _ensure_tuple(value: Any) -> Tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        return (value,)
    return tuple(str(part) for part in value)


def _clone_sequence(value: Optional[Iterable[str]]) -> Optional[List[str]]:
    if value is None:
        return None
    return [str(item) for item in value]


_GAME_CONFIG: Dict[str, ConfigMap] = {
    "shrinks_offense": {
        "columns": [
            "GP",
            "3FG Att",
            "3FG Makes",
            "3FG %",
            "Shrink 3FG Att",
            "Shrink 3FG Makes",
            "Shrink 3FG %",
            "Shrink 3FG Freq",
            "Non-Shrink 3FG Att",
            "Non-Shrink 3FG Makes",
            "Non-Shrink 3FG %",
            "Non-Shrink 3FG Freq",
        ],
        "column_map": {
            "GP": ("games",),
            "3FG Att": ("fg3_att",),
            "3FG Makes": ("fg3_make",),
            "3FG %": ("fg3_pct",),
            "Shrink 3FG Att": ("fg3_shrink_att",),
            "Shrink 3FG Makes": ("fg3_shrink_make",),
            "Shrink 3FG %": ("fg3_shrink_pct",),
            "Shrink 3FG Freq": ("fg3_shrink_freq_pct",),
            "Non-Shrink 3FG Att": ("fg3_nonshrink_att",),
            "Non-Shrink 3FG Makes": ("fg3_nonshrink_make",),
            "Non-Shrink 3FG %": ("fg3_nonshrink_pct",),
            "Non-Shrink 3FG Freq": ("fg3_nonshrink_freq_pct",),
        },
        "pct_columns": [
            "3FG %",
            "Shrink 3FG %",
            "Shrink 3FG Freq",
            "Non-Shrink 3FG %",
            "Non-Shrink 3FG Freq",
        ],
        "default_sort": [
            "fg3_shrink_pct",
            "fg3_shrink_att",
            "fg3_shrink_make",
            "player",
        ],
        "table_id": "game-leaderboard-offense-3fg-shrinks",
        "percent_specs": [
            {
                "slug": "3fg_pct",
                "metric": "fg3_pct",
                "attempt_slugs": [
                    "fg3_att",
                    "fg3_attempts",
                    "three_att",
                    "three_attempts",
                ],
                "minimum_slugs": [
                    "fg3_min",
                    "fg3_min_att",
                    "fg3_min_attempts",
                    "three_min_att",
                    "three_min_attempts",
                ],
            },
            {
                "slug": "shrink_3fg_pct",
                "metric": "fg3_pct",
                "attempt_slugs": [
                    "fg3_shrink_att",
                    "shrink_att",
                    "shrink_attempts",
                ],
                "minimum_slugs": [
                    "fg3_shrink_min",
                    "fg3_shrink_min_att",
                    "fg3_shrink_min_attempts",
                    "shrink_min",
                    "shrink_min_att",
                    "shrink_min_attempts",
                ],
                "shrink_alias": True,
            },
            {
                "slug": "non_shrink_3fg_pct",
                "metric": "fg3_pct",
                "attempt_slugs": [
                    "fg3_nonshrink_att",
                    "non_shrink_att",
                    "non_shrink_attempts",
                ],
                "minimum_slugs": [
                    "fg3_nonshrink_min",
                    "fg3_nonshrink_min_att",
                    "fg3_nonshrink_min_attempts",
                    "non_shrink_min",
                    "non_shrink_min_att",
                    "non_shrink_min_attempts",
                ],
                "shrink_alias": True,
            },
        ],
    },
    "atr_finishing": {
        "columns": ["GP", "ATR Att", "ATR Makes", "ATR %", "And-1"],
        "column_map": {
            "GP": ("games",),
            "ATR Att": ("atr_att",),
            "ATR Makes": ("atr_make",),
            "ATR %": ("atr_pct",),
            "And-1": ("atr_and1",),
        },
        "pct_columns": ["ATR %"],
        "default_sort": ["atr_pct", "atr_att", "atr_make", "player"],
        "table_id": "game-leaderboard-offense-atr-finishing",
        "percent_specs": [
            {
                "slug": "atr_pct",
                "metric": "atr2fg_pct",
                "attempt_slugs": ["atr_att", "atr_attempts"],
                "minimum_slugs": [
                    "atr_min",
                    "atr_min_att",
                    "atr_min_attempts",
                ],
            }
        ],
    },
    "rebounding_offense": {
        "columns": [
            "GP",
            "Crash +",
            "Crash Opps",
            "Crash %",
            "Back Man +",
            "Back Man Opps",
            "Back Man %",
        ],
        "column_map": {
            "GP": ("games",),
            "Crash +": ("crash_plus",),
            "Crash Opps": ("crash_opps",),
            "Crash %": ("crash_pct",),
            "Back Man +": ("back_plus",),
            "Back Man Opps": ("back_opps",),
            "Back Man %": ("back_pct",),
        },
        "pct_columns": ["Crash %", "Back Man %"],
        "default_sort": ["crash_pct", "crash_opps", "crash_plus", "player"],
        "table_id": "game-leaderboard-rebounding-offensive",
        "percent_specs": [
            {
                "slug": "crash_pct",
                "metric": "crash_pct",
                "attempt_slugs": [
                    "crash_opps",
                    "crash_opp",
                    "crash_att",
                    "crash_attempts",
                ],
                "minimum_slugs": [
                    "crash_min",
                    "crash_min_att",
                    "crash_min_attempts",
                    "crash_att_min",
                    "min_crash_att",
                ],
            },
            {
                "slug": "back_man_pct",
                "metric": "back_man_pct",
                "attempt_slugs": [
                    "back_opps",
                    "back_opp",
                    "back_att",
                    "back_attempts",
                ],
                "minimum_slugs": [
                    "back_min",
                    "back_min_att",
                    "back_min_attempts",
                    "back_att_min",
                    "min_back_att",
                ],
            },
        ],
    },
    "rebounding_defense": {
        "columns": [
            "GP",
            "Box Out +",
            "Box Out Opps",
            "Box Out %",
            "Off Reb's Given Up",
        ],
        "column_map": {
            "GP": ("games",),
            "Box Out +": ("box_plus",),
            "Box Out Opps": ("box_opps",),
            "Box Out %": ("box_pct",),
            "Off Reb's Given Up": ("off_reb_given_up",),
        },
        "pct_columns": ["Box Out %"],
        "default_sort": ["box_pct", "box_opps", "box_plus", "player"],
        "table_id": "game-leaderboard-rebounding-defensive",
        "percent_specs": [
            {
                "slug": "box_out_pct",
                "metric": "box_out_pct",
                "attempt_slugs": [
                    "box_opps",
                    "box_opp",
                    "box_att",
                    "box_attempts",
                ],
                "minimum_slugs": [
                    "box_min",
                    "box_min_opps",
                    "box_min_opp",
                    "box_min_att",
                    "box_min_attempts",
                ],
            }
        ],
    },
    "collisions": {
        "columns": ["GP", "Gap +", "Gap Opps", "Gap %"],
        "column_map": {
            "GP": ("games",),
            "Gap +": ("gap_plus",),
            "Gap Opps": ("gap_opps",),
            "Gap %": ("gap_pct",),
        },
        "pct_columns": ["Gap %"],
        "default_sort": ["gap_pct", "gap_opps", "gap_plus", "player"],
        "table_id": "game-leaderboard-defense-collisions",
        "percent_specs": [
            {
                "slug": "gap_pct",
                "metric": "gap_pct",
                "attempt_slugs": ["gap_opps", "gap_opp"],
                "minimum_slugs": [
                    "gap_min",
                    "gap_min_opp",
                    "gap_min_opps",
                    "gap_opp_min",
                    "gap_opps_min",
                ],
            }
        ],
    },
    "pass_contest": {
        "columns": ["GP", "Contest +", "Contest Opps", "Contest %"],
        "column_map": {
            "GP": ("games",),
            "Contest +": ("contest_plus",),
            "Contest Opps": ("contest_opps",),
            "Contest %": ("contest_pct",),
        },
        "pct_columns": ["Contest %"],
        "default_sort": ["contest_pct", "contest_opps", "contest_plus", "player"],
        "table_id": "game-leaderboard-defense-pass-contest",
        "percent_specs": [
            {
                "slug": "contest_pct",
                "metric": "contest_pct",
                "attempt_slugs": [
                    "contest_opps",
                    "contest_opp",
                    "contest_att",
                    "contest_attempts",
                ],
                "minimum_slugs": [
                    "contest_min",
                    "contest_min_att",
                    "contest_min_attempts",
                    "contest_att_min",
                    "min_contest_att",
                ],
            }
        ],
    },
    "overall_gap_help": {
        "columns": ["GP", "Gap +", "Gap Opps", "Gap %"],
        "column_map": {
            "GP": ("games",),
            "Gap +": ("gap_plus",),
            "Gap Opps": ("gap_opps",),
            "Gap %": ("gap_pct",),
        },
        "pct_columns": ["Gap %"],
        "default_sort": ["gap_pct", "gap_opps", "gap_plus", "player"],
        "table_id": "game-leaderboard-defense-gap",
        "percent_specs": [
            {
                "slug": "gap_pct",
                "metric": "gap_pct",
                "attempt_slugs": ["gap_opps", "gap_opp"],
                "minimum_slugs": [
                    "gap_min",
                    "gap_min_opp",
                    "gap_min_opps",
                    "gap_opp_min",
                    "gap_opps_min",
                ],
            }
        ],
    },
    "overall_low_man": {
        "columns": ["GP", "Low Man +", "Low Man Opps", "Low Man %"],
        "column_map": {
            "GP": ("games",),
            "Low Man +": ("low_plus",),
            "Low Man Opps": ("low_opps",),
            "Low Man %": ("low_pct",),
        },
        "pct_columns": ["Low Man %"],
        "default_sort": ["low_pct", "low_opps", "low_plus", "player"],
        "table_id": "game-leaderboard-defense-low-man",
        "percent_specs": [
            {
                "slug": "low_pct",
                "metric": "low_pct",
                "attempt_slugs": ["low_opps", "low_opp"],
                "minimum_slugs": [
                    "low_min",
                    "low_min_opp",
                    "low_min_opps",
                    "low_opp_min",
                    "low_opps_min",
                ],
            }
        ],
    },
    "pnr_grade": {
        "columns": [
            "GP",
            "Close Window +",
            "Close Window Opps",
            "Close Window %",
            "Shut Door +",
            "Shut Door Opps",
            "Shut Door %",
        ],
        "column_map": {
            "GP": ("games",),
            "Close Window +": ("close_plus",),
            "Close Window Opps": ("close_opps",),
            "Close Window %": ("close_pct",),
            "Shut Door +": ("shut_plus",),
            "Shut Door Opps": ("shut_opps",),
            "Shut Door %": ("shut_pct",),
        },
        "pct_columns": ["Close Window %", "Shut Door %"],
        "default_sort": ["close_pct", "close_opps", "close_plus", "player"],
        "table_id": "game-leaderboard-pnr-grade",
        "percent_specs": [
            {
                "slug": "close_window_pct",
                "metric": "close_window_pct",
                "attempt_slugs": ["close_opps", "close_opp"],
                "minimum_slugs": [
                    "close_min",
                    "close_min_opps",
                    "close_min_opp",
                    "close_opp_min",
                    "close_opps_min",
                ],
            },
            {
                "slug": "shut_door_pct",
                "metric": "shut_door_pct",
                "attempt_slugs": ["shut_opps", "shut_opp"],
                "minimum_slugs": [
                    "shut_min",
                    "shut_min_opps",
                    "shut_min_opp",
                    "shut_opp_min",
                    "shut_opps_min",
                ],
            },
        ],
    },
}


def _config_for(key: str) -> ConfigMap:
    try:
        return _GAME_CONFIG[key]
    except KeyError as exc:  # pragma: no cover - defensive programming
        raise KeyError(f"Unknown game leaderboard section: {key!r}") from exc


def columns_for(key: str) -> List[str]:
    return list(_config_for(key).get("columns", []))


def column_map_for(key: str) -> Dict[str, Tuple[str, ...]]:
    mapping = {}
    for label, value in _config_for(key).get("column_map", {}).items():
        mapping[label] = _ensure_tuple(value)
    return mapping


def pct_columns_for(key: str) -> List[str]:
    return list(_config_for(key).get("pct_columns", []))


def table_id_for(key: str) -> Optional[str]:
    table_id = _config_for(key).get("table_id")
    return str(table_id) if table_id else None


def sort_default_for(key: str) -> List[Any]:
    return list(_config_for(key).get("default_sort", []))


def percent_specs_for(key: str) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    for spec in _config_for(key).get("percent_specs", []) or []:
        cloned = dict(spec)
        cloned["attempt_slugs"] = _clone_sequence(spec.get("attempt_slugs"))
        cloned["minimum_slugs"] = _clone_sequence(spec.get("minimum_slugs"))
        specs.append(cloned)
    return specs


def helptext_for(key: str) -> Optional[str]:
    help_text = _config_for(key).get("help")
    return str(help_text) if help_text else None


def guards_for(key: str) -> Optional[Mapping[str, Any]]:
    guards = _config_for(key).get("guards")
    if guards is None:
        return None
    if isinstance(guards, Mapping):
        return dict(guards)
    return None


__all__ = [
    "columns_for",
    "column_map_for",
    "pct_columns_for",
    "table_id_for",
    "sort_default_for",
    "percent_specs_for",
    "helptext_for",
    "guards_for",
]
