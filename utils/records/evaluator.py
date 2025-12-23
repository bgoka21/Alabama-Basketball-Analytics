"""Scaffolding for record evaluation and auto entry upserts."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from models.database import RecordDefinition, RecordEntry, db
from utils.records.stat_keys import DEFAULT_QUALIFIER_THRESHOLDS


def get_threshold(definition: RecordDefinition) -> Optional[float]:
    """Return the qualifier threshold for a definition (override > default > None)."""
    if definition.qualifier_threshold_override is not None:
        return float(definition.qualifier_threshold_override)
    if definition.qualifier_stat_key:
        default = DEFAULT_QUALIFIER_THRESHOLDS.get(definition.qualifier_stat_key)
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


def upsert_auto_entry(definition: RecordDefinition, candidate: Dict[str, Any]) -> RecordEntry:
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
    else:
        entry.value = float(candidate["value"])
        entry.scope = definition.scope
        entry.game_id = candidate.get("game_id")
        entry.occurred_on = candidate.get("occurred_on")
        entry.holder_entity_type = candidate["holder_entity_type"]
        entry.holder_player_id = candidate.get("holder_player_id")
        entry.holder_opponent_name = candidate.get("holder_opponent_name")
        entry.season_year = candidate.get("season_year")

    return entry


def evaluate_candidates(game_id: int, candidates: Iterable[Dict[str, Any]]) -> List[RecordEntry]:
    """Evaluate candidates, upsert AUTO entries, and update current flags.

    Forced current entries are respected and never demoted automatically.
    """
    definitions = {
        definition.stat_key: definition
        for definition in RecordDefinition.query.filter_by(scope="GAME", is_active=True)
    }
    touched_definition_ids = set()
    updated_entries: List[RecordEntry] = []

    for candidate in candidates:
        definition = definitions.get(candidate.get("definition_stat_key"))
        if not definition:
            continue
        if not qualifies(definition, candidate["value"], candidate):
            continue
        entry = upsert_auto_entry(definition, candidate)
        touched_definition_ids.add(definition.id)
        updated_entries.append(entry)

    for definition_id in touched_definition_ids:
        forced_entries = RecordEntry.query.filter_by(
            record_definition_id=definition_id,
            is_forced_current=True,
        ).all()
        if forced_entries:
            for entry in forced_entries:
                entry.is_current = True
            continue

        entries = RecordEntry.query.filter_by(record_definition_id=definition_id).all()
        if not entries:
            continue
        max_value = max(entry.value for entry in entries)
        for entry in entries:
            entry.is_current = entry.value == max_value

    return updated_entries
