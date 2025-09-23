from typing import Dict

SESSION_CATEGORY_CANONICAL = {
    # canonical : accepted aliases
    "Summer Workouts": {"Summer Workouts"},
    "Fall Workouts": {"Fall Workouts"},
    "Official Practice": {"Official Practice", "Official Practices"},
}

# Fast reverse lookup for normalization
_REVERSE_MAP: Dict[str, str] = {}
for canonical, aliases in SESSION_CATEGORY_CANONICAL.items():
    for alias in aliases:
        _REVERSE_MAP[alias.lower()] = canonical


def normalize_category(name: str) -> str:
    """Return the canonical category name for ``name`` if known."""
    if not name:
        return name
    return _REVERSE_MAP.get(name.strip().lower(), name.strip())
