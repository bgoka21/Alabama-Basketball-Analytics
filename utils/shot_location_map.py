"""Helpers for normalizing raw shot location strings."""
from __future__ import annotations

import logging
from typing import Dict, Set

logger = logging.getLogger(__name__)

SHOT_LOCATION_MAP: Dict[str, str] = {
    "Rim": "rim",
    "Paint": "paint",
    "Corner": "corner",
    "Corner 3": "corner",
    "Left Corner": "corner_left",
    "Right Corner": "corner_right",
    "Wing": "wing",
    "Right Wing": "wing_right",
    "Left Slot": "slot_left",
    "Right Slot": "slot_right",
    "Left Houston": "short_wing_left",
    "Right Houston": "short_wing_right",
    "Left SC": "short_corner_left",
    "Right SC": "short_corner_right",
    "Left SW": "short_wing_left",
    "Nail": "nail",
    "Top": "top",
    "Logo": "logo",
    "Right SW": "short_wing_right",
}

UNKNOWN_SHOT_LOCATIONS: Set[str] = set()


def normalize_shot_location(raw_value: str | None) -> str:
    """Normalize a raw shot location to a canonical zone."""
    if raw_value is None:
        return _record_unknown("<missing>")

    key = raw_value.strip()
    if key in SHOT_LOCATION_MAP:
        return SHOT_LOCATION_MAP[key]

    if not key:
        return _record_unknown("<empty>")

    return _record_unknown(key)


def _record_unknown(raw_value: str) -> str:
    UNKNOWN_SHOT_LOCATIONS.add(raw_value)
    logger.warning("Unknown shot location encountered: %s", raw_value)
    return "unknown"


def get_unknown_shot_locations() -> Set[str]:
    """Return a copy of the unknown shot locations collected so far."""
    return set(UNKNOWN_SHOT_LOCATIONS)


def clear_unknown_shot_locations() -> None:
    """Clear any collected unknown shot locations."""
    UNKNOWN_SHOT_LOCATIONS.clear()
