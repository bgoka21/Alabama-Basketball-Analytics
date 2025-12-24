"""Build record candidates from per-game stored stats."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import or_

from models.database import (
    BlueCollarStats,
    Game,
    OpponentBlueCollarStats,
    PlayerStats,
    RecordDefinition,
    Roster,
    TeamStats,
)
from utils.records.stat_keys import canonicalize_stat_key


logger = logging.getLogger(__name__)
_WARNED_STAT_KEYS: set[tuple[str, str]] = set()

RowFilter = Callable[[Any], Any]
StatMapping = Tuple[type, str, Optional[RowFilter]]


def _team_stats_filter(query):
    return query.filter(or_(TeamStats.is_opponent.is_(False), TeamStats.is_opponent.is_(None)))


def _opponent_stats_filter(query):
    return query.filter(TeamStats.is_opponent.is_(True))


TEAM_STAT_MAP: Dict[str, StatMapping] = {
    "team.total_points": (TeamStats, "total_points", _team_stats_filter),
    "team.total_possessions": (TeamStats, "total_possessions", _team_stats_filter),
    "team.total_assists": (TeamStats, "total_assists", _team_stats_filter),
    "team.total_turnovers": (TeamStats, "total_turnovers", _team_stats_filter),
    "team.total_atr_makes": (TeamStats, "total_atr_makes", _team_stats_filter),
    "team.total_atr_attempts": (TeamStats, "total_atr_attempts", _team_stats_filter),
    "team.total_fg2_makes": (TeamStats, "total_fg2_makes", _team_stats_filter),
    "team.total_fg2_attempts": (TeamStats, "total_fg2_attempts", _team_stats_filter),
    "team.total_fg3_makes": (TeamStats, "total_fg3_makes", _team_stats_filter),
    "team.total_fg3_attempts": (TeamStats, "total_fg3_attempts", _team_stats_filter),
    "team.total_ftm": (TeamStats, "total_ftm", _team_stats_filter),
    "team.total_fta": (TeamStats, "total_fta", _team_stats_filter),
}

OPPONENT_STAT_MAP: Dict[str, StatMapping] = {
    "opp.total_points": (TeamStats, "total_points", _opponent_stats_filter),
    "opp.total_possessions": (TeamStats, "total_possessions", _opponent_stats_filter),
    "opp.total_assists": (TeamStats, "total_assists", _opponent_stats_filter),
    "opp.total_turnovers": (TeamStats, "total_turnovers", _opponent_stats_filter),
    "opp.total_atr_makes": (TeamStats, "total_atr_makes", _opponent_stats_filter),
    "opp.total_atr_attempts": (TeamStats, "total_atr_attempts", _opponent_stats_filter),
    "opp.total_fg2_makes": (TeamStats, "total_fg2_makes", _opponent_stats_filter),
    "opp.total_fg2_attempts": (TeamStats, "total_fg2_attempts", _opponent_stats_filter),
    "opp.total_fg3_makes": (TeamStats, "total_fg3_makes", _opponent_stats_filter),
    "opp.total_fg3_attempts": (TeamStats, "total_fg3_attempts", _opponent_stats_filter),
    "opp.total_ftm": (TeamStats, "total_ftm", _opponent_stats_filter),
    "opp.total_fta": (TeamStats, "total_fta", _opponent_stats_filter),
}

PLAYER_STAT_MAP: Dict[str, Tuple[type, str]] = {
    "player.points": (PlayerStats, "points"),
    "player.assists": (PlayerStats, "assists"),
    "player.turnovers": (PlayerStats, "turnovers"),
    "player.fg2_makes": (PlayerStats, "fg2_makes"),
    "player.fg2_attempts": (PlayerStats, "fg2_attempts"),
    "player.fg3_makes": (PlayerStats, "fg3_makes"),
    "player.fg3_attempts": (PlayerStats, "fg3_attempts"),
    "player.ftm": (PlayerStats, "ftm"),
    "player.fta": (PlayerStats, "fta"),
}

BLUE_COLLAR_MAP: Dict[str, Tuple[type, str]] = {
    "bc.team.total_blue_collar": (BlueCollarStats, "total_blue_collar"),
    "bc.player.total_blue_collar": (BlueCollarStats, "total_blue_collar"),
}


def get_supported_stat_keys() -> set[str]:
    return {
        *TEAM_STAT_MAP.keys(),
        *OPPONENT_STAT_MAP.keys(),
        *PLAYER_STAT_MAP.keys(),
        *BLUE_COLLAR_MAP.keys(),
    }


def get_missing_stat_keys(registry_keys: Iterable[str]) -> List[str]:
    return sorted(set(registry_keys) - get_supported_stat_keys())


def _resolve_stat_attr(stat_key: str, mapping: Dict[str, StatMapping]) -> Optional[str]:
    entry = mapping.get(stat_key)
    if not entry:
        return None
    _, attr, _ = entry
    if not attr:
        return None
    return attr


def _extract_value(stat_key: str, row: Any, mapping: Dict[str, StatMapping]) -> Optional[float]:
    attr = _resolve_stat_attr(stat_key, mapping)
    if not attr:
        return None
    if not hasattr(row, attr):
        logger.warning("Stat attribute '%s' missing on row for stat_key '%s'", attr, stat_key)
        return None
    raw_value = getattr(row, attr)
    if raw_value is None:
        return None
    return float(raw_value)


def _is_blue_collar(stat_key: str) -> bool:
    return stat_key.startswith("bc.")


def _select_mapping(entity_type: str, stat_key: str) -> Optional[Dict[str, StatMapping]]:
    if _is_blue_collar(stat_key):
        return {key: (model, attr, None) for key, (model, attr) in BLUE_COLLAR_MAP.items()}
    if entity_type == "TEAM":
        return TEAM_STAT_MAP
    if entity_type == "OPPONENT":
        return OPPONENT_STAT_MAP
    if entity_type == "PLAYER":
        return {key: (model, attr, None) for key, (model, attr) in PLAYER_STAT_MAP.items()}
    return None


def _qualifier_value(
    definition: RecordDefinition,
    row: Any,
    mapping: Dict[str, StatMapping],
    qualifier_key: str,
) -> Optional[float]:
    if not qualifier_key:
        return None
    value = _extract_value(qualifier_key, row, mapping)
    return value


def _build_candidate(
    *,
    definition: RecordDefinition,
    row: Any,
    mapping: Dict[str, StatMapping],
    holder_entity_type: str,
    holder_player_id: Optional[int],
    holder_opponent_name: Optional[str],
    game_id: int,
    occurred_on: date,
    stat_key: str,
    qualifier_stat_key: str,
) -> Optional[Dict[str, Any]]:
    value = _extract_value(stat_key, row, mapping)
    if value is None:
        return None

    qualifier_value = None
    if qualifier_stat_key:
        qualifier_value = _qualifier_value(definition, row, mapping, qualifier_stat_key)
        if qualifier_value is None:
            logger.debug(
                "Skipping candidate for definition %s: qualifier stat '%s' missing",
                definition.id,
                qualifier_stat_key,
            )
            return None

    return {
        "definition_id": definition.id,
        "definition_stat_key": stat_key,
        "holder_entity_type": holder_entity_type,
        "holder_player_id": holder_player_id,
        "holder_opponent_name": holder_opponent_name,
        "value": value,
        "game_id": game_id,
        "occurred_on": occurred_on,
        "qualifier_value": qualifier_value,
    }


def _roster_name_lookup(game: Game) -> Dict[str, int]:
    roster_entries = Roster.query.filter_by(season_id=game.season_id).all()
    return {player.player_name.strip().lower(): player.id for player in roster_entries}


def build_game_candidates(
    game_id: int,
    *,
    include_inactive: bool = False,
    scope: str = "GAME",
    definitions: Optional[Iterable[RecordDefinition]] = None,
) -> List[Dict[str, Any]]:
    """Build record candidates for a game using stored aggregate rows."""
    game = Game.query.get(game_id)
    if not game:
        logger.warning("Game %s not found; skipping candidate build", game_id)
        return []

    if definitions is None:
        definition_query = RecordDefinition.query.filter_by(scope=scope)
        if not include_inactive:
            definition_query = definition_query.filter_by(is_active=True)
        definitions = definition_query.all()
    definitions = list(definitions)
    occurred_on = game.game_date or date.today()

    team_stats = (
        TeamStats.query.filter(
            TeamStats.game_id == game_id,
            or_(TeamStats.is_opponent.is_(False), TeamStats.is_opponent.is_(None)),
        ).first()
    )
    opponent_stats = TeamStats.query.filter_by(game_id=game_id, is_opponent=True).first()
    player_stats_rows = PlayerStats.query.filter_by(game_id=game_id).all()

    blue_collar_team = BlueCollarStats.query.filter_by(
        game_id=game_id,
        player_id=None,
    ).first()
    blue_collar_players = BlueCollarStats.query.filter(
        BlueCollarStats.game_id == game_id,
        BlueCollarStats.player_id.isnot(None),
    ).all()

    opp_blue_collar_team = OpponentBlueCollarStats.query.filter_by(
        game_id=game_id,
        player_id=None,
    ).first()
    roster_lookup = _roster_name_lookup(game)
    candidates: List[Dict[str, Any]] = []

    for definition in definitions:
        original_stat_key = definition.stat_key or ""
        canonical_stat_key = canonicalize_stat_key(original_stat_key)
        original_qualifier_key = definition.qualifier_stat_key or ""
        canonical_qualifier_key = canonicalize_stat_key(original_qualifier_key)

        mapping = _select_mapping(definition.entity_type, canonical_stat_key)
        if not mapping:
            logger.warning(
                "No mapping for definition %s (entity_type=%s stat_key=%s canonical=%s)",
                definition.id,
                definition.entity_type,
                original_stat_key,
                canonical_stat_key,
            )
            continue
        if canonical_stat_key not in mapping:
            warning_key = (original_stat_key, canonical_stat_key)
            if warning_key not in _WARNED_STAT_KEYS:
                logger.warning(
                    "No stat mapping found for stat_key '%s' (canonical '%s')",
                    original_stat_key,
                    canonical_stat_key,
                )
                _WARNED_STAT_KEYS.add(warning_key)
            continue

        if definition.entity_type == "TEAM":
            row = blue_collar_team if _is_blue_collar(canonical_stat_key) else team_stats
            if not row:
                logger.warning("Missing team stats row for game %s", game_id)
                continue
            candidate = _build_candidate(
                definition=definition,
                row=row,
                mapping=mapping,
                holder_entity_type="TEAM",
                holder_player_id=None,
                holder_opponent_name=None,
                game_id=game_id,
                occurred_on=occurred_on,
                stat_key=canonical_stat_key,
                qualifier_stat_key=canonical_qualifier_key,
            )
            if candidate:
                candidates.append(candidate)

        elif definition.entity_type == "OPPONENT":
            row = (
                opp_blue_collar_team
                if _is_blue_collar(canonical_stat_key)
                else opponent_stats
            )
            if not row:
                logger.warning("Missing opponent stats row for game %s", game_id)
                continue
            candidate = _build_candidate(
                definition=definition,
                row=row,
                mapping=mapping,
                holder_entity_type="OPPONENT",
                holder_player_id=None,
                holder_opponent_name=game.opponent_name,
                game_id=game_id,
                occurred_on=occurred_on,
                stat_key=canonical_stat_key,
                qualifier_stat_key=canonical_qualifier_key,
            )
            if candidate:
                candidates.append(candidate)

        elif definition.entity_type == "PLAYER":
            if _is_blue_collar(canonical_stat_key):
                for row in blue_collar_players:
                    if not row.player_id:
                        continue
                    candidate = _build_candidate(
                        definition=definition,
                        row=row,
                        mapping=mapping,
                        holder_entity_type="PLAYER",
                        holder_player_id=row.player_id,
                        holder_opponent_name=None,
                        game_id=game_id,
                        occurred_on=occurred_on,
                        stat_key=canonical_stat_key,
                        qualifier_stat_key=canonical_qualifier_key,
                    )
                    if candidate:
                        candidates.append(candidate)
            else:
                for row in player_stats_rows:
                    player_name = (row.player_name or "").strip().lower()
                    player_id = roster_lookup.get(player_name)
                    if not player_id:
                        logger.warning(
                            "Unable to resolve roster id for player '%s' in game %s",
                            row.player_name,
                            game_id,
                        )
                        continue
                    candidate = _build_candidate(
                        definition=definition,
                        row=row,
                        mapping=mapping,
                        holder_entity_type="PLAYER",
                        holder_player_id=player_id,
                        holder_opponent_name=None,
                        game_id=game_id,
                        occurred_on=occurred_on,
                        stat_key=canonical_stat_key,
                        qualifier_stat_key=canonical_qualifier_key,
                    )
                    if candidate:
                        candidates.append(candidate)

    logger.info(
        "Built %s game record candidates from %s definitions for game %s",
        len(candidates),
        len(definitions),
        game_id,
    )
    return candidates
