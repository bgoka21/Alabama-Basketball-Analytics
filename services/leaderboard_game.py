"""Games-only leaderboard helpers.

These helpers aggregate ``PlayerStats`` rows scoped to real games so the
admin Game Leaderboard can render season totals vs. each player's most
recent appearance.  The returned data mirrors the practice leaderboard's
shape so it can be fed directly into ``build_dual_table``.
"""

from __future__ import annotations

import inspect
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Set

from sqlalchemy import and_, func
from sqlalchemy.orm import Query

from models.database import Game, GameTypeTag, PlayerStats, Season, Roster, db
from utils.shottype import compute_3fg_breakdown_from_shots


# --- Public data containers -------------------------------------------------


@dataclass
class LeaderboardSlice:
    """Container for a single leaderboard slice (season or last game)."""

    rows: List[Dict[str, Any]]
    totals: Optional[Dict[str, Any]]
    note_date: Optional[date] = None


# --- Shared helpers ---------------------------------------------------------


_AGGREGATE_FIELDS: Tuple[str, ...] = (
    "fg3_makes",
    "fg3_attempts",
    "fg2_attempts",
    "atr_makes",
    "atr_attempts",
    "atr_fouled",
    "crash_positive",
    "crash_missed",
    "back_man_positive",
    "back_man_missed",
    "box_out_positive",
    "box_out_missed",
    "off_reb_given_up",
    "collision_gap_positive",
    "collision_gap_missed",
    "pass_contest_positive",
    "pass_contest_missed",
    "pnr_gap_positive",
    "pnr_gap_missed",
    "low_help_positive",
    "low_help_missed",
    "close_window_positive",
    "close_window_missed",
    "shut_door_positive",
    "shut_door_missed",
)


def _roster_names(season_id: int) -> Set[str]:
    """Return the set of rostered player names for ``season_id``."""

    query = (
        db.session.query(Roster.player_name)
        .filter(Roster.season_id == season_id)
        .filter(Roster.player_name.isnot(None))
    )
    names: Set[str] = set()
    for (name,) in query.all():
        if not name:
            continue
        text = str(name).strip()
        if text:
            names.add(text)
    return names


def _safe_pct(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    try:
        if not numer or not denom:
            return None
        return (float(numer) / float(denom)) * 100.0
    except (TypeError, ZeroDivisionError, ValueError):
        return None


def _base_game_query(season_id: int, game_types: Optional[Sequence[str]] = None) -> Query:
    query = (
        PlayerStats.query.join(Game, PlayerStats.game_id == Game.id)
        .filter(PlayerStats.season_id == season_id)
        .filter(PlayerStats.game_id.isnot(None))
    )
    if game_types:
        query = query.filter(Game.type_tags.any(GameTypeTag.tag.in_(game_types)))
    return query


def _apply_date_window(query: Query, start_date: Optional[date], end_date: Optional[date]) -> Query:
    if start_date:
        query = query.filter(Game.game_date >= start_date)
    if end_date:
        query = query.filter(Game.game_date <= end_date)
    return query


def _parse_shot_details(blob: Optional[str]) -> List[Dict[str, Any]]:
    if not blob:
        return []
    shots: List[Dict[str, Any]] = []
    try:
        parsed = json.loads(blob)
    except (TypeError, ValueError):
        return shots

    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict):
                shots.append(item)
    elif isinstance(parsed, dict):
        shots.append(parsed)
    return shots


def _aggregate_rows(
    rows: Iterable[PlayerStats],
    roster_names: Optional[Set[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    players: Dict[str, Dict[str, Any]] = {}
    shot_events: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in rows:
        raw_player = row.player_name
        if not raw_player:
            continue
        player = str(raw_player).strip()
        if not player:
            continue
        if roster_names is not None and player not in roster_names:
            continue

        entry = players.setdefault(
            player,
            {
                "player": player,
                "jersey": row.jersey_number,
            },
        )
        entry.setdefault("jersey", row.jersey_number or entry.get("jersey"))

        for field in _AGGREGATE_FIELDS:
            entry[field] = (entry.get(field) or 0) + (getattr(row, field) or 0)

        shot_events[player].extend(_parse_shot_details(row.shot_type_details))

    # Attach shrink/non-shrink breakdowns once per player.
    for player, entry in players.items():
        breakdown = compute_3fg_breakdown_from_shots(shot_events.get(player, []))
        entry["fg3_shrink_makes"] = breakdown.get("fg3_shrink_makes", 0)
        entry["fg3_shrink_att"] = breakdown.get("fg3_shrink_att", 0)
        entry["fg3_shrink_pct"] = breakdown.get("fg3_shrink_pct")
        entry["fg3_shrink_freq_pct"] = breakdown.get("fg3_shrink_freq_pct")
        entry["fg3_nonshrink_makes"] = breakdown.get("fg3_nonshrink_makes", 0)
        entry["fg3_nonshrink_att"] = breakdown.get("fg3_nonshrink_att", 0)
        entry["fg3_nonshrink_pct"] = breakdown.get("fg3_nonshrink_pct")
        entry["fg3_nonshrink_freq_pct"] = breakdown.get("fg3_nonshrink_freq_pct")

    return players


def _rows_for_players(
    players: Dict[str, Dict[str, Any]],
    row_builder: Callable[[str, Dict[str, Any]], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    built: List[Dict[str, Any]] = []
    for player, data in players.items():
        row = row_builder(player, data)
        if row:
            built.append(row)
    built.sort(key=lambda r: (r.get("jersey") or 999, r.get("player")))
    return built


def _simple_totals(rows: Sequence[Dict[str, Any]], keys: Sequence[str]) -> Dict[str, Any]:
    totals: Dict[str, Any] = {key: 0 for key in keys}
    for row in rows:
        for key in keys:
            totals[key] += row.get(key, 0) or 0
    for key in keys:
        if key.endswith("_pct"):
            source = key[:-4] + "_opps"
            plus_key = key[:-4] + "_plus"
            denom = totals.get(source)
            numer = totals.get(plus_key)
            totals[key] = _safe_pct(numer, denom)
    return totals


def _season_rows(
    season_id: int,
    start_date: Optional[date],
    end_date: Optional[date],
    game_types: Optional[Sequence[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    query = _apply_date_window(
        _base_game_query(season_id, game_types), start_date, end_date
    )
    return _aggregate_rows(query.all(), _roster_names(season_id))


def _last_game_rows(
    season_id: int,
    start_date: Optional[date],
    end_date: Optional[date],
    game_types: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[date]]:
    base = _apply_date_window(
        _base_game_query(season_id, game_types), start_date, end_date
    )

    last_seen = (
        base.with_entities(
            PlayerStats.player_name.label("player"),
            func.max(Game.game_date).label("last_date"),
        )
        .group_by(PlayerStats.player_name)
        .subquery()
    )

    rows = (
        base.join(
            last_seen,
            and_(
                last_seen.c.player == PlayerStats.player_name,
                last_seen.c.last_date == Game.game_date,
            ),
        )
        .all()
    )

    roster = _roster_names(season_id)
    players = _aggregate_rows(rows, roster)
    latest_date = None
    if rows:
        latest_date = max(getattr(r.game, "game_date", None) for r in rows if getattr(r, "game", None))
    return players, latest_date


def _season_rows_with_types(
    season_id: int,
    start_date: Optional[date],
    end_date: Optional[date],
    game_types: Optional[Sequence[str]],
) -> Dict[str, Dict[str, Any]]:
    params = inspect.signature(_season_rows).parameters
    if len(params) <= 3:
        return _season_rows(season_id, start_date, end_date)  # type: ignore[misc]
    return _season_rows(season_id, start_date, end_date, game_types)


def _last_game_rows_with_types(
    season_id: int,
    start_date: Optional[date],
    end_date: Optional[date],
    game_types: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[date]]:
    params = inspect.signature(_last_game_rows).parameters
    if len(params) <= 3:
        return _last_game_rows(season_id, start_date, end_date)  # type: ignore[misc]
    return _last_game_rows(season_id, start_date, end_date, game_types)


def get_season_window(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Tuple[Optional[date], Optional[date]]:
    season = db.session.get(Season, season_id)
    if not season:
        return start_date, end_date
    resolved_start = start_date or season.start_date
    resolved_end = end_date or season.end_date
    return resolved_start, resolved_end


# --- Row builders -----------------------------------------------------------


def _build_common(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "player": player,
        "jersey": data.get("jersey"),
    }


def _build_shrink_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    att = data.get("fg3_attempts", 0)
    fg2_att = data.get("fg2_attempts", 0)
    makes = data.get("fg3_makes", 0)
    total_fga = (fg2_att or 0) + (att or 0)
    row.update(
        {
            "fg3_att": att,
            "fg3_make": makes,
            "fg3_pct": _safe_pct(makes, att),
            "fg3_freq_pct": _safe_pct(att, total_fga),
            "fg3_shrink_att": data.get("fg3_shrink_att", 0),
            "fg3_shrink_make": data.get("fg3_shrink_makes", 0),
            "fg3_shrink_pct": data.get("fg3_shrink_pct"),
            "fg3_shrink_freq_pct": data.get("fg3_shrink_freq_pct"),
            "fg3_nonshrink_att": data.get("fg3_nonshrink_att", 0),
            "fg3_nonshrink_make": data.get("fg3_nonshrink_makes", 0),
            "fg3_nonshrink_pct": data.get("fg3_nonshrink_pct"),
            "fg3_nonshrink_freq_pct": data.get("fg3_nonshrink_freq_pct"),
            "fg2_att": fg2_att,
        }
    )
    return row


def _build_atr_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    att = data.get("atr_attempts", 0)
    makes = data.get("atr_makes", 0)
    row.update(
        {
            "atr_att": att,
            "atr_make": makes,
            "atr_pct": _safe_pct(makes, att),
            "atr_and1": data.get("atr_fouled", 0),
        }
    )
    return row


def _build_off_reb_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    crash_plus = data.get("crash_positive", 0)
    crash_opp = crash_plus + data.get("crash_missed", 0)
    back_plus = data.get("back_man_positive", 0)
    back_opp = back_plus + data.get("back_man_missed", 0)
    row.update(
        {
            "crash_plus": crash_plus,
            "crash_opps": crash_opp,
            "crash_pct": _safe_pct(crash_plus, crash_opp),
            "back_plus": back_plus,
            "back_opps": back_opp,
            "back_pct": _safe_pct(back_plus, back_opp),
        }
    )
    return row


def _build_def_reb_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    plus = data.get("box_out_positive", 0)
    opps = plus + data.get("box_out_missed", 0)
    row.update(
        {
            "box_plus": plus,
            "box_opps": opps,
            "box_pct": _safe_pct(plus, opps),
            "off_reb_given_up": data.get("off_reb_given_up", 0),
        }
    )
    return row


def _build_collision_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    plus = data.get("collision_gap_positive", 0)
    opps = plus + data.get("collision_gap_missed", 0)
    row.update(
        {
            "gap_plus": plus,
            "gap_opps": opps,
            "gap_pct": _safe_pct(plus, opps),
        }
    )
    return row


def _build_pass_contest_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    plus = data.get("pass_contest_positive", 0)
    opps = plus + data.get("pass_contest_missed", 0)
    row.update(
        {
            "contest_plus": plus,
            "contest_opps": opps,
            "contest_pct": _safe_pct(plus, opps),
        }
    )
    return row


def _build_gap_help_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    plus = data.get("pnr_gap_positive") or 0
    opps = plus + (data.get("pnr_gap_missed") or 0)
    row.update(
        {
            "gap_plus": plus,
            "gap_opps": opps,
            "gap_pct": _safe_pct(plus, opps),
        }
    )
    return row


def _build_low_man_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    plus = data.get("low_help_positive", 0)
    opps = plus + data.get("low_help_missed", 0)
    row.update(
        {
            "low_plus": plus,
            "low_opps": opps,
            "low_pct": _safe_pct(plus, opps),
        }
    )
    return row


def _build_pnr_grade_row(player: str, data: Dict[str, Any]) -> Dict[str, Any]:
    row = _build_common(player, data)
    close_plus = data.get("close_window_positive", 0)
    close_opps = close_plus + data.get("close_window_missed", 0)
    shut_plus = data.get("shut_door_positive", 0)
    shut_opps = shut_plus + data.get("shut_door_missed", 0)
    row.update(
        {
            "close_plus": close_plus,
            "close_opps": close_opps,
            "close_pct": _safe_pct(close_plus, close_opps),
            "shut_plus": shut_plus,
            "shut_opps": shut_opps,
            "shut_pct": _safe_pct(shut_plus, shut_opps),
        }
    )
    return row


def _build_totals(rows: Sequence[Dict[str, Any]], mappings: Sequence[Tuple[str, str]]) -> Optional[Dict[str, Any]]:
    if not rows:
        return None
    totals: Dict[str, Any] = {"player": "Team Totals"}
    for key, target in mappings:
        totals[target] = sum(row.get(target, 0) or 0 for row in rows)
    for pct_source, pct_key in (
        (("fg3_make", "fg3_att"), "fg3_pct"),
        (("fg3_shrink_make", "fg3_shrink_att"), "fg3_shrink_pct"),
        (("fg3_nonshrink_make", "fg3_nonshrink_att"), "fg3_nonshrink_pct"),
        (("atr_make", "atr_att"), "atr_pct"),
        (("crash_plus", "crash_opps"), "crash_pct"),
        (("back_plus", "back_opps"), "back_pct"),
        (("box_plus", "box_opps"), "box_pct"),
        (("gap_plus", "gap_opps"), "gap_pct"),
        (("contest_plus", "contest_opps"), "contest_pct"),
        (("low_plus", "low_opps"), "low_pct"),
        (("close_plus", "close_opps"), "close_pct"),
        (("shut_plus", "shut_opps"), "shut_pct"),
    ):
        numer_key, denom_key = pct_source
        if numer_key in totals and denom_key in totals:
            totals[pct_key] = _safe_pct(totals[numer_key], totals[denom_key])
    if "fg3_shrink_att" in totals and "fg3_att" in totals:
        totals["fg3_shrink_freq_pct"] = _safe_pct(totals["fg3_shrink_att"], totals["fg3_att"])
    if "fg3_nonshrink_att" in totals and "fg3_att" in totals:
        totals["fg3_nonshrink_freq_pct"] = _safe_pct(totals["fg3_nonshrink_att"], totals["fg3_att"])
    if "fg3_att" in totals:
        fg2_total = totals.get("fg2_att") or 0
        totals["fg3_freq_pct"] = _safe_pct(totals["fg3_att"], fg2_total + (totals["fg3_att"] or 0))
    return totals


# --- Fetch helpers ----------------------------------------------------------


def _build_slice(
    players: Dict[str, Dict[str, Any]],
    row_builder: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    total_map: Sequence[Tuple[str, str]],
    note_date: Optional[date] = None,
) -> LeaderboardSlice:
    rows = _rows_for_players(players, row_builder)
    totals = _build_totals(rows, total_map)
    return LeaderboardSlice(rows=rows, totals=totals, note_date=note_date)


def fetch_offense_shrinks(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_shrink_row,
        (
            ("fg3_make", "fg3_make"),
            ("fg3_att", "fg3_att"),
            ("fg2_att", "fg2_att"),
            ("fg3_shrink_make", "fg3_shrink_make"),
            ("fg3_shrink_att", "fg3_shrink_att"),
            ("fg3_nonshrink_make", "fg3_nonshrink_make"),
            ("fg3_nonshrink_att", "fg3_nonshrink_att"),
        ),
    )


def fetch_offense_shrinks_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_shrink_row,
        (
            ("fg3_make", "fg3_make"),
            ("fg3_att", "fg3_att"),
            ("fg2_att", "fg2_att"),
            ("fg3_shrink_make", "fg3_shrink_make"),
            ("fg3_shrink_att", "fg3_shrink_att"),
            ("fg3_nonshrink_make", "fg3_nonshrink_make"),
            ("fg3_nonshrink_att", "fg3_nonshrink_att"),
        ),
        note_date=note_date,
    )


def fetch_atr_finishing(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_atr_row,
        (
            ("atr_make", "atr_make"),
            ("atr_att", "atr_att"),
            ("atr_and1", "atr_and1"),
        ),
    )


def fetch_atr_finishing_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_atr_row,
        (
            ("atr_make", "atr_make"),
            ("atr_att", "atr_att"),
            ("atr_and1", "atr_and1"),
        ),
        note_date=note_date,
    )


def fetch_oreb(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_off_reb_row,
        (
            ("crash_plus", "crash_plus"),
            ("crash_opps", "crash_opps"),
            ("back_plus", "back_plus"),
            ("back_opps", "back_opps"),
        ),
    )


def fetch_oreb_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_off_reb_row,
        (
            ("crash_plus", "crash_plus"),
            ("crash_opps", "crash_opps"),
            ("back_plus", "back_plus"),
            ("back_opps", "back_opps"),
        ),
        note_date=note_date,
    )


def fetch_dreb(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_def_reb_row,
        (
            ("box_plus", "box_plus"),
            ("box_opps", "box_opps"),
            ("off_reb_given_up", "off_reb_given_up"),
        ),
    )


def fetch_dreb_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_def_reb_row,
        (
            ("box_plus", "box_plus"),
            ("box_opps", "box_opps"),
            ("off_reb_given_up", "off_reb_given_up"),
        ),
        note_date=note_date,
    )


def fetch_collisions(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_collision_row,
        (
            ("gap_plus", "gap_plus"),
            ("gap_opps", "gap_opps"),
        ),
    )


def fetch_collisions_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_collision_row,
        (
            ("gap_plus", "gap_plus"),
            ("gap_opps", "gap_opps"),
        ),
        note_date=note_date,
    )


def fetch_pass_contest(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_pass_contest_row,
        (
            ("contest_plus", "contest_plus"),
            ("contest_opps", "contest_opps"),
        ),
    )


def fetch_pass_contest_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_pass_contest_row,
        (
            ("contest_plus", "contest_plus"),
            ("contest_opps", "contest_opps"),
        ),
        note_date=note_date,
    )


def fetch_gap_help(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_gap_help_row,
        (
            ("gap_plus", "gap_plus"),
            ("gap_opps", "gap_opps"),
        ),
    )


def fetch_gap_help_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_gap_help_row,
        (
            ("gap_plus", "gap_plus"),
            ("gap_opps", "gap_opps"),
        ),
        note_date=note_date,
    )


def fetch_low_man(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_low_man_row,
        (
            ("low_plus", "low_plus"),
            ("low_opps", "low_opps"),
        ),
    )


def fetch_low_man_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_low_man_row,
        (
            ("low_plus", "low_plus"),
            ("low_opps", "low_opps"),
        ),
        note_date=note_date,
    )


def fetch_pnr_grade(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players = _season_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_pnr_grade_row,
        (
            ("close_plus", "close_plus"),
            ("close_opps", "close_opps"),
            ("shut_plus", "shut_plus"),
            ("shut_opps", "shut_opps"),
        ),
    )


def fetch_pnr_grade_last_game(
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    game_types: Optional[Sequence[str]] = None,
) -> LeaderboardSlice:
    players, note_date = _last_game_rows_with_types(season_id, start_date, end_date, game_types)
    return _build_slice(
        players,
        _build_pnr_grade_row,
        (
            ("close_plus", "close_plus"),
            ("close_opps", "close_opps"),
            ("shut_plus", "shut_plus"),
            ("shut_opps", "shut_opps"),
        ),
        note_date=note_date,
    )

