"""Helpers for applying normalized label filters to query objects."""
from __future__ import annotations

from typing import Iterable, Optional, Set

from sqlalchemy.orm import Query

from models.database import PlayerStatLabel, PlayerStats, Possession, PossessionLabel


def _coerce_labels(label_set: Optional[Iterable[str]]) -> Set[str]:
    if not label_set:
        return set()
    return {str(lbl).strip().upper() for lbl in label_set if str(lbl).strip()}


def apply_player_label_filter(query: Query, label_set: Optional[Iterable[str]]) -> Query:
    """Restrict ``query`` to PlayerStats rows that match any label in ``label_set``."""
    labels = tuple(_coerce_labels(label_set))
    if not labels:
        return query
    return query.filter(
        PlayerStats.label_entries.any(PlayerStatLabel.label.in_(labels))
    )


def apply_possession_label_filter(query: Query, label_set: Optional[Iterable[str]]) -> Query:
    """Restrict ``query`` to Possession rows that match any label in ``label_set``."""
    labels = tuple(_coerce_labels(label_set))
    if not labels:
        return query
    return query.filter(
        Possession.label_entries.any(PossessionLabel.label.in_(labels))
    )
