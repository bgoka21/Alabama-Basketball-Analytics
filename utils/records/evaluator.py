"""Scaffolding for record evaluation and auto entry upserts."""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from models.database import RecordDefinition, RecordEntry, db
from utils.records.stat_keys import DEFAULT_QUALIFIER_THRESHOLDS, canonicalize_stat_key

logger = logging.getLogger(__name__)

def get_threshold(definition: RecordDefinition) -> Optional[float]:
    """Return the qualifier threshold for a definition (override > default > None)."""
    if definition.qualifier_threshold_override is not None:
        return float(definition.qualifier_threshold_override)
    if definition.qualifier_stat_key:
        canonical_key = canonicalize_stat_key(definition.qualifier_stat_key)
        default = DEFAULT_QUALIFIER_THRESHOLDS.get(canonical_key)
        return float(default) if default is not None else None
    return None


def qualifies(
    definition: RecordDefinition,
    candidate_value: float,
    candidate_context: Dict[str, Any],
) -> bool:
    """Return True if a candidate meets the qualifier rules for a definition."""
    if not definition.qualifier_stat_key:
        return True

    qualifier_value = candidate_context.get("qualifier_value")
    threshold = get_threshold(definition)

    if qualifier_value is None:
        return False
    if threshold is None:
        return True
    return qualifier_value >= threshold


def _build_auto_key(definition: RecordDefinition, candidate: Dict[str, Any]) -> str:
    holder_player_id = candidate.get("holder_player_id")
    holder_key = holder_player_id if holder_player_id is not None else "NONE"
    return (
        f"{definition.id}:{candidate['game_id']}:{candidate['holder_entity_type']}:{holder_key}"
    )


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
        if not qualifies(definition, candidate["value"], candidate):
            continue
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
