"""Qualification rules and default thresholds for record definitions."""
from __future__ import annotations

import logging
from typing import Optional

from models.database import RecordDefinition
from utils.records.stat_keys import canonicalize_stat_key

logger = logging.getLogger(__name__)

DEFAULT_QUALIFIER_THRESHOLDS = {
    # Possessions
    "team.total_possessions": 40,
    "opp.total_possessions": 40,
    "player.total_possessions": 10,
    # Shot attempts (team)
    "team.total_fg3_attempts": 10,
    "team.total_fg2_attempts": 10,
    "team.total_atr_attempts": 8,
    "team.total_fta": 10,
    # Shot attempts (player)
    "player.total_fg3_attempts": 3,
    "player.total_fg2_attempts": 3,
    "player.total_atr_attempts": 2,
    "player.total_fta": 4,
}

_WARNED_QUALIFIER_KEYS: set[str] = set()


def get_threshold(definition: RecordDefinition) -> Optional[float]:
    """Return the qualifier threshold for a definition (override > default > None)."""
    if not definition.qualifier_stat_key:
        return None
    if definition.qualifier_threshold_override is not None:
        return float(definition.qualifier_threshold_override)

    canonical_key = canonicalize_stat_key(definition.qualifier_stat_key)
    default = DEFAULT_QUALIFIER_THRESHOLDS.get(canonical_key)
    if default is None and canonical_key not in _WARNED_QUALIFIER_KEYS:
        logger.warning(
            "No default qualifier threshold for stat_key '%s' (definition %s)",
            canonical_key,
            definition.id,
        )
        _WARNED_QUALIFIER_KEYS.add(canonical_key)
    return float(default) if default is not None else None


def qualifies(definition: RecordDefinition, qualifier_value: Optional[float]) -> bool:
    """Return True if a candidate meets the qualifier rules for a definition."""
    threshold = get_threshold(definition)
    if threshold is None:
        return True
    if qualifier_value is None:
        return False
    return qualifier_value >= threshold
