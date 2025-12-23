"""Build record candidates from per-game stored stats."""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

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


logger = logging.getLogger(__name__)
_WARNED_STAT_KEYS: set[str] = set()

TEAM_STAT_ATTRS = {
    "team.points": "total_points",
    "team.total_points": "total_points",
    "team.assists": "total_assists",
    "team.total_assists": "total_assists",
    "team.turnovers": "total_turnovers",
    "team.total_turnovers": "total_turnovers",
    "team.fg2_makes": "total_fg2_makes",
    "team.fg2_attempts": "total_fg2_attempts",
    "team.fg3_makes": "total_fg3_makes",
    "team.fg3_attempts": "total_fg3_attempts",
    "team.ftm": "total_ftm",
    "team.fta": "total_fta",
    "team.possessions": "total_possessions",
    "team.total_possessions": "total_possessions",
    "team.blue_collar": "total_blue_collar",
}

OPPONENT_STAT_ATTRS = {
    "opponent.points": "total_points",
    "opponent.assists": "total_assists",
    "opponent.turnovers": "total_turnovers",
    "opponent.fg2_makes": "total_fg2_makes",
    "opponent.fg2_attempts": "total_fg2_attempts",
    "opponent.fg3_makes": "total_fg3_makes",
    "opponent.fg3_attempts": "total_fg3_attempts",
    "opponent.ftm": "total_ftm",
    "opponent.fta": "total_fta",
    "opponent.possessions": "total_possessions",
    "opp.points": "total_points",
    "opp.assists": "total_assists",
    "opp.turnovers": "total_turnovers",
    "opp.fg2_makes": "total_fg2_makes",
    "opp.fg2_attempts": "total_fg2_attempts",
    "opp.fg3_makes": "total_fg3_makes",
    "opp.fg3_attempts": "total_fg3_attempts",
    "opp.ftm": "total_ftm",
    "opp.fta": "total_fta",
    "opp.possessions": "total_possessions",
}

PLAYER_STAT_ATTRS = {
    "player.points": "points",
    "player.assists": "assists",
    "player.turnovers": "turnovers",
    "player.fg2_makes": "fg2_makes",
    "player.fg2_attempts": "fg2_attempts",
    "player.fg3_makes": "fg3_makes",
    "player.fg3_attempts": "fg3_attempts",
    "player.ftm": "ftm",
    "player.fta": "fta",
}

BLUE_COLLAR_STAT_ATTRS = {
    "blue_collar.total": "total_blue_collar",
    "blue_collar.rebounds": None,
    "blue_collar.deflections": "deflection",
    "blue_collar.loose_balls": "floor_dive",
    "blue_collar.charges": "charge_taken",
    "bc.total": "total_blue_collar",
    "bc.rebounds": None,
    "bc.deflections": "deflection",
    "bc.loose_balls": "floor_dive",
    "bc.charges": "charge_taken",
}


def _resolve_stat_attr(stat_key: str, mapping: Dict[str, Optional[str]]) -> Optional[str]:
    attr = mapping.get(stat_key)
    if not attr:
        if stat_key not in _WARNED_STAT_KEYS:
            logger.warning("No stat mapping found for stat_key '%s'", stat_key)
            _WARNED_STAT_KEYS.add(stat_key)
        return None
    return attr


def _extract_value(stat_key: str, row: Any, mapping: Dict[str, Optional[str]]) -> Optional[float]:
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
    return stat_key.startswith("blue_collar.") or stat_key.startswith("bc.")


def _select_mapping(entity_type: str, stat_key: str) -> Optional[Dict[str, Optional[str]]]:
    if _is_blue_collar(stat_key):
        return BLUE_COLLAR_STAT_ATTRS
    if entity_type == "TEAM":
        return TEAM_STAT_ATTRS
    if entity_type == "OPPONENT":
        return OPPONENT_STAT_ATTRS
    if entity_type == "PLAYER":
        return PLAYER_STAT_ATTRS
    return None


def _qualifier_value(
    definition: RecordDefinition,
    row: Any,
    mapping: Dict[str, Optional[str]],
) -> Optional[float]:
    if not definition.qualifier_stat_key:
        return None
    value = _extract_value(definition.qualifier_stat_key, row, mapping)
    if value is None:
        logger.warning(
            "Qualifier stat_key '%s' unresolved for definition %s",
            definition.qualifier_stat_key,
            definition.id,
        )
    return value


def _build_candidate(
    *,
    definition: RecordDefinition,
    row: Any,
    mapping: Dict[str, Optional[str]],
    holder_entity_type: str,
    holder_player_id: Optional[int],
    holder_opponent_name: Optional[str],
    game_id: int,
    occurred_on: date,
) -> Optional[Dict[str, Any]]:
    value = _extract_value(definition.stat_key, row, mapping)
    if value is None:
        return None

    qualifier_value = None
    if definition.qualifier_stat_key:
        qualifier_value = _qualifier_value(definition, row, mapping)
        if qualifier_value is None:
            return None

    return {
        "definition_id": definition.id,
        "definition_stat_key": definition.stat_key,
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


def build_game_candidates(game_id: int) -> List[Dict[str, Any]]:
    """Build record candidates for a game using stored aggregate rows."""
    game = Game.query.get(game_id)
    if not game:
        logger.warning("Game %s not found; skipping candidate build", game_id)
        return []

    definitions = RecordDefinition.query.filter_by(scope="GAME", is_active=True).all()
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
        mapping = _select_mapping(definition.entity_type, definition.stat_key)
        if not mapping:
            logger.warning(
                "No mapping for definition %s (entity_type=%s stat_key=%s)",
                definition.id,
                definition.entity_type,
                definition.stat_key,
            )
            continue

        if definition.entity_type == "TEAM":
            row = blue_collar_team if _is_blue_collar(definition.stat_key) else team_stats
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
            )
            if candidate:
                candidates.append(candidate)

        elif definition.entity_type == "OPPONENT":
            row = (
                opp_blue_collar_team
                if _is_blue_collar(definition.stat_key)
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
            )
            if candidate:
                candidates.append(candidate)

        elif definition.entity_type == "PLAYER":
            if _is_blue_collar(definition.stat_key):
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
