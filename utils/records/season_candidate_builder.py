"""Build record candidates from season aggregate stats."""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import func, or_

from models.database import Game, RecordDefinition, Season, TeamStats, db
from utils.records.stat_keys import canonicalize_stat_key

logger = logging.getLogger(__name__)

TEAM_STAT_COLUMNS: Dict[str, str] = {
    "team.total_points": "total_points",
    "team.total_assists": "total_assists",
    "team.total_second_assists": "total_second_assists",
    "team.total_pot_assists": "total_pot_assists",
    "team.total_turnovers": "total_turnovers",
    "team.total_atr_makes": "total_atr_makes",
    "team.total_atr_attempts": "total_atr_attempts",
    "team.total_fg2_makes": "total_fg2_makes",
    "team.total_fg2_attempts": "total_fg2_attempts",
    "team.total_fg3_makes": "total_fg3_makes",
    "team.total_fg3_attempts": "total_fg3_attempts",
    "team.total_ftm": "total_ftm",
    "team.total_fta": "total_fta",
    "team.total_possessions": "total_possessions",
    "team.total_blue_collar": "total_blue_collar",
    "team.total_fouls_drawn": "total_fouls_drawn",
}

OPPONENT_STAT_COLUMNS: Dict[str, str] = {
    "opp.total_points": "total_points",
    "opp.total_assists": "total_assists",
    "opp.total_turnovers": "total_turnovers",
    "opp.total_atr_makes": "total_atr_makes",
    "opp.total_atr_attempts": "total_atr_attempts",
    "opp.total_fg2_makes": "total_fg2_makes",
    "opp.total_fg2_attempts": "total_fg2_attempts",
    "opp.total_fg3_makes": "total_fg3_makes",
    "opp.total_fg3_attempts": "total_fg3_attempts",
    "opp.total_ftm": "total_ftm",
    "opp.total_fta": "total_fta",
    "opp.total_possessions": "total_possessions",
    "opp.total_blue_collar": "total_blue_collar",
    "opp.total_fouls_drawn": "total_fouls_drawn",
}

_AGG_COLUMNS = sorted({*TEAM_STAT_COLUMNS.values(), *OPPONENT_STAT_COLUMNS.values()})


def _season_year_from_name(season_name: str | None) -> Optional[int]:
    if not season_name:
        return None
    match = re.search(r"(19|20)\d{2}", season_name)
    if not match:
        return None
    return int(match.group(0))


def _aggregate_totals(*, season_id: int, is_opponent: bool) -> Dict[str, Optional[float]]:
    columns = [getattr(TeamStats, column) for column in _AGG_COLUMNS]
    query = db.session.query(*[func.sum(column) for column in columns])
    filters = [
        TeamStats.season_id == season_id,
        TeamStats.game_id.isnot(None),
    ]
    if is_opponent:
        filters.append(TeamStats.is_opponent.is_(True))
    else:
        filters.append(or_(TeamStats.is_opponent.is_(False), TeamStats.is_opponent.is_(None)))
    row = query.filter(*filters).one()
    return {column: row[index] for index, column in enumerate(_AGG_COLUMNS)}


def _get_last_game_date(season_id: int) -> Optional[date]:
    return db.session.query(func.max(Game.game_date)).filter(Game.season_id == season_id).scalar()


def build_season_candidates(
    season_id: int,
    *,
    include_inactive_definitions: bool = False,
) -> List[Dict[str, Any]]:
    season = Season.query.get(season_id)
    if not season:
        logger.warning("Season %s not found; skipping season candidate build", season_id)
        return []

    definition_query = RecordDefinition.query.filter_by(scope="SEASON").filter(
        RecordDefinition.entity_type.in_(["TEAM", "OPPONENT"])
    )
    if not include_inactive_definitions:
        definition_query = definition_query.filter_by(is_active=True)
    definitions = definition_query.all()

    if not definitions:
        return []

    team_totals = _aggregate_totals(season_id=season_id, is_opponent=False)
    opponent_totals = _aggregate_totals(season_id=season_id, is_opponent=True)

    season_year = _season_year_from_name(season.season_name)
    occurred_on = _get_last_game_date(season_id)

    candidates: List[Dict[str, Any]] = []

    for definition in definitions:
        original_stat_key = definition.stat_key or ""
        canonical_stat_key = canonicalize_stat_key(original_stat_key)
        original_qualifier_key = definition.qualifier_stat_key or ""
        canonical_qualifier_key = canonicalize_stat_key(original_qualifier_key)

        if definition.entity_type == "TEAM":
            mapping = TEAM_STAT_COLUMNS
            totals = team_totals
        else:
            mapping = OPPONENT_STAT_COLUMNS
            totals = opponent_totals

        if canonical_stat_key not in mapping:
            logger.warning(
                "No season stat mapping found for stat_key '%s' (canonical '%s')",
                original_stat_key,
                canonical_stat_key,
            )
            continue

        column = mapping[canonical_stat_key]
        value = totals.get(column)
        if value is None:
            continue

        qualifier_value = None
        if canonical_qualifier_key:
            qualifier_column = mapping.get(canonical_qualifier_key)
            if not qualifier_column:
                logger.warning(
                    "Missing qualifier mapping for stat_key '%s' (canonical '%s')",
                    original_qualifier_key,
                    canonical_qualifier_key,
                )
                continue
            qualifier_value = totals.get(qualifier_column)
            if qualifier_value is None:
                continue

        candidates.append(
            {
                "definition_id": definition.id,
                "definition_stat_key": canonical_stat_key,
                "holder_entity_type": definition.entity_type,
                "holder_player_id": None,
                "holder_opponent_name": None,
                "value": float(value),
                "game_id": None,
                "occurred_on": occurred_on,
                "season_year": season_year,
                "qualifier_value": qualifier_value,
            }
        )

    logger.info(
        "Built %s season record candidates from %s definitions for season %s",
        len(candidates),
        len(definitions),
        season_id,
    )
    return candidates
