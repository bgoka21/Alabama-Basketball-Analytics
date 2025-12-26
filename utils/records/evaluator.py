"""Scaffolding for record evaluation and auto entry upserts."""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from models.database import RecordDefinition, RecordEntry, db
from utils.records.qualifications import qualifies
from utils.records.stat_keys import canonicalize_stat_key

logger = logging.getLogger(__name__)


def _build_auto_key(definition: RecordDefinition, candidate: Dict[str, Any]) -> str:
    holder_player_id = candidate.get("holder_player_id")
    holder_key = holder_player_id if holder_player_id is not None else "NONE"
    return (
        f"{definition.id}:{candidate['game_id']}:{candidate['holder_entity_type']}:{holder_key}"
    )


def _build_season_auto_key(definition: RecordDefinition, candidate: Dict[str, Any]) -> str:
    holder_player_id = candidate.get("holder_player_id")
    holder_key = holder_player_id if holder_player_id is not None else "NONE"
    season_year = candidate.get("season_year")
    season_key = season_year if season_year is not None else "NONE"
    return f"{definition.id}:SEASON:{season_key}:{candidate['holder_entity_type']}:{holder_key}"


def upsert_auto_entry(
    definition: RecordDefinition,
    candidate: Dict[str, Any],
) -> tuple[RecordEntry, bool]:
    """Create or update an AUTO record entry for a game/holder combo.

    Example candidate:
        {
          "definition_stat_key": "team.points",
          "holder_entity_type": "TEAM",
          "value": 100,
          "game_id": 123,
          "occurred_on": date(2024, 11, 1),
          "qualifier_value": 65,
        }
    """
    auto_key = _build_auto_key(definition, candidate)
    entry = RecordEntry.query.filter_by(auto_key=auto_key).one_or_none()

    if entry is None:
        entry = RecordEntry(
            record_definition_id=definition.id,
            holder_entity_type=candidate["holder_entity_type"],
            holder_player_id=candidate.get("holder_player_id"),
            holder_opponent_name=candidate.get("holder_opponent_name"),
            value=float(candidate["value"]),
            scope=definition.scope,
            season_year=candidate.get("season_year"),
            game_id=candidate.get("game_id"),
            occurred_on=candidate.get("occurred_on"),
            source_type="AUTO",
            notes=candidate.get("notes"),
            auto_key=auto_key,
        )
        db.session.add(entry)
        was_created = True
    else:
        entry.value = float(candidate["value"])
        entry.scope = definition.scope
        entry.game_id = candidate.get("game_id")
        entry.occurred_on = candidate.get("occurred_on")
        entry.holder_entity_type = candidate["holder_entity_type"]
        entry.holder_player_id = candidate.get("holder_player_id")
        entry.holder_opponent_name = candidate.get("holder_opponent_name")
        entry.season_year = candidate.get("season_year")
        was_created = False

    return entry, was_created


def upsert_season_auto_entry(
    definition: RecordDefinition,
    candidate: Dict[str, Any],
) -> tuple[RecordEntry, bool]:
    """Create or update an AUTO record entry for a season/holder combo."""
    auto_key = _build_season_auto_key(definition, candidate)
    entry = RecordEntry.query.filter_by(auto_key=auto_key).one_or_none()

    if entry is None:
        entry = RecordEntry(
            record_definition_id=definition.id,
            holder_entity_type=candidate["holder_entity_type"],
            holder_player_id=candidate.get("holder_player_id"),
            holder_opponent_name=candidate.get("holder_opponent_name"),
            value=float(candidate["value"]),
            scope=definition.scope,
            season_year=candidate.get("season_year"),
            game_id=None,
            occurred_on=candidate.get("occurred_on"),
            source_type="AUTO",
            notes=candidate.get("notes"),
            auto_key=auto_key,
        )
        db.session.add(entry)
        was_created = True
    else:
        entry.value = float(candidate["value"])
        entry.scope = definition.scope
        entry.game_id = None
        entry.occurred_on = candidate.get("occurred_on")
        entry.holder_entity_type = candidate["holder_entity_type"]
        entry.holder_player_id = candidate.get("holder_player_id")
        entry.holder_opponent_name = candidate.get("holder_opponent_name")
        entry.season_year = candidate.get("season_year")
        was_created = False

    return entry, was_created


def evaluate_candidates(
    game_id: int,
    candidates: Iterable[Dict[str, Any]],
    *,
    scope: str = "GAME",
    include_inactive: bool = False,
    definitions: Optional[Iterable[RecordDefinition]] = None,
    stats: Optional[Dict[str, int]] = None,
) -> List[RecordEntry]:
    """Evaluate candidates, upsert AUTO entries, and update current flags.

    Forced current entries are respected and never demoted automatically.
    """
    candidate_list = list(candidates)
    if definitions is None:
        definition_query = RecordDefinition.query.filter_by(scope=scope)
        if not include_inactive:
            definition_query = definition_query.filter_by(is_active=True)
        definitions = definition_query.all()
    definitions = list(definitions)
    definitions_by_id = {definition.id: definition for definition in definitions}
    definitions_by_stat = {
        canonicalize_stat_key(definition.stat_key): definition for definition in definitions
    }
    candidates_by_definition: Dict[int, List[Dict[str, Any]]] = {definition.id: [] for definition in definitions}
    touched_definition_ids = set()
    updated_entries: List[RecordEntry] = []
    auto_created = 0
    auto_updated = 0

    logger.info(
        "Evaluating %s definitions against %s candidates for game %s",
        len(definitions),
        len(candidate_list),
        game_id,
    )

    for candidate in candidate_list:
        definition = None
        definition_id = candidate.get("definition_id")
        if definition_id is not None:
            definition = definitions_by_id.get(definition_id)
        if definition is None:
            definition = definitions_by_stat.get(
                canonicalize_stat_key(candidate.get("definition_stat_key", ""))
            )
        if not definition:
            continue
        candidates_by_definition.setdefault(definition.id, []).append(candidate)

    for definition in definitions:
        definition_candidates = candidates_by_definition.get(definition.id, [])
        if not definition_candidates:
            continue
        filtered_out = 0
        qualified_candidates: List[Dict[str, Any]] = []
        if definition.qualifier_stat_key:
            for candidate in definition_candidates:
                if qualifies(definition, candidate.get("qualifier_value")):
                    qualified_candidates.append(candidate)
                else:
                    filtered_out += 1
            if filtered_out:
                logger.debug(
                    "Filtered %s candidates for definition %s (%s) due to qualification",
                    filtered_out,
                    definition.id,
                    definition.name,
                )
        else:
            qualified_candidates = definition_candidates

        if not qualified_candidates:
            continue

        for candidate in qualified_candidates:
            entry, was_created = upsert_auto_entry(definition, candidate)
            touched_definition_ids.add(definition.id)
            updated_entries.append(entry)
            if was_created:
                auto_created += 1
            else:
                auto_updated += 1

    logger.info(
        "Auto record entries created=%s updated=%s for game %s",
        auto_created,
        auto_updated,
        game_id,
    )

    current_changed = 0
    for definition_id in touched_definition_ids:
        forced_entries = RecordEntry.query.filter_by(
            record_definition_id=definition_id,
            is_forced_current=True,
            is_active=True,
        ).all()
        if forced_entries:
            for entry in forced_entries:
                entry.is_current = True
            continue

        entries = RecordEntry.query.filter_by(
            record_definition_id=definition_id,
            is_active=True,
        ).all()
        if not entries:
            continue
        previous_current = {entry.id for entry in entries if entry.is_current}
        max_value = max(entry.value for entry in entries)
        for entry in entries:
            entry.is_current = entry.value == max_value
        current_entries = {entry.id for entry in entries if entry.is_current}
        if previous_current != current_entries:
            current_changed += 1

    logger.info(
        "Record definitions with current holder changes=%s for game %s",
        current_changed,
        game_id,
    )

    if stats is not None:
        stats["definitions_evaluated"] = len(definitions)
        stats["candidates_evaluated"] = len(candidate_list)
        stats["auto_created"] = auto_created
        stats["auto_updated"] = auto_updated
        stats["definitions_with_current_changes"] = current_changed

    return updated_entries


def evaluate_season_candidates(
    season_id: int,
    candidates: Iterable[Dict[str, Any]],
    *,
    scope: str = "SEASON",
    include_inactive: bool = False,
    definitions: Optional[Iterable[RecordDefinition]] = None,
    stats: Optional[Dict[str, int]] = None,
) -> List[RecordEntry]:
    """Evaluate season candidates, upsert AUTO entries, and update current flags."""
    candidate_list = list(candidates)
    if definitions is None:
        definition_query = RecordDefinition.query.filter_by(scope=scope)
        if not include_inactive:
            definition_query = definition_query.filter_by(is_active=True)
        definitions = definition_query.all()
    definitions = list(definitions)
    definitions_by_id = {definition.id: definition for definition in definitions}
    definitions_by_stat = {
        canonicalize_stat_key(definition.stat_key): definition for definition in definitions
    }
    candidates_by_definition: Dict[int, List[Dict[str, Any]]] = {definition.id: [] for definition in definitions}
    touched_definition_ids = set()
    updated_entries: List[RecordEntry] = []
    auto_created = 0
    auto_updated = 0

    logger.info(
        "Evaluating %s definitions against %s candidates for season %s",
        len(definitions),
        len(candidate_list),
        season_id,
    )

    for candidate in candidate_list:
        definition = None
        definition_id = candidate.get("definition_id")
        if definition_id is not None:
            definition = definitions_by_id.get(definition_id)
        if definition is None:
            definition = definitions_by_stat.get(
                canonicalize_stat_key(candidate.get("definition_stat_key", ""))
            )
        if not definition:
            continue
        candidates_by_definition.setdefault(definition.id, []).append(candidate)

    for definition in definitions:
        definition_candidates = candidates_by_definition.get(definition.id, [])
        if not definition_candidates:
            continue
        filtered_out = 0
        qualified_candidates: List[Dict[str, Any]] = []
        if definition.qualifier_stat_key:
            for candidate in definition_candidates:
                if qualifies(definition, candidate.get("qualifier_value")):
                    qualified_candidates.append(candidate)
                else:
                    filtered_out += 1
            if filtered_out:
                logger.debug(
                    "Filtered %s candidates for definition %s (%s) due to qualification",
                    filtered_out,
                    definition.id,
                    definition.name,
                )
        else:
            qualified_candidates = definition_candidates

        if not qualified_candidates:
            continue

        for candidate in qualified_candidates:
            entry, was_created = upsert_season_auto_entry(definition, candidate)
            touched_definition_ids.add(definition.id)
            updated_entries.append(entry)
            if was_created:
                auto_created += 1
            else:
                auto_updated += 1

    logger.info(
        "Auto season record entries created=%s updated=%s for season %s",
        auto_created,
        auto_updated,
        season_id,
    )

    current_changed = 0
    for definition_id in touched_definition_ids:
        forced_entries = RecordEntry.query.filter_by(
            record_definition_id=definition_id,
            is_forced_current=True,
            is_active=True,
        ).all()
        if forced_entries:
            for entry in forced_entries:
                entry.is_current = True
            continue

        entries = RecordEntry.query.filter_by(
            record_definition_id=definition_id,
            is_active=True,
        ).all()
        if not entries:
            continue
        previous_current = {entry.id for entry in entries if entry.is_current}
        max_value = max(entry.value for entry in entries)
        for entry in entries:
            entry.is_current = entry.value == max_value
        current_entries = {entry.id for entry in entries if entry.is_current}
        if previous_current != current_entries:
            current_changed += 1

    logger.info(
        "Season record definitions with current holder changes=%s for season %s",
        current_changed,
        season_id,
    )

    if stats is not None:
        stats["definitions_evaluated"] = len(definitions)
        stats["candidates_evaluated"] = len(candidate_list)
        stats["auto_created"] = auto_created
        stats["auto_updated"] = auto_updated
        stats["definitions_with_current_changes"] = current_changed

    return updated_entries
