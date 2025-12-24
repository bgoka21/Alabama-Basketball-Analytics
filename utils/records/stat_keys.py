"""Registry of record stat keys."""

from __future__ import annotations

STAT_KEY_GROUPS = {
    "Team — Totals": [
        {"key": "team.total_points", "label": "Total Points"},
        {"key": "team.total_possessions", "label": "Total Possessions"},
        {"key": "team.total_assists", "label": "Total Assists"},
        {"key": "team.total_turnovers", "label": "Total Turnovers"},
        {"key": "team.total_atr_makes", "label": "ATR Makes"},
        {"key": "team.total_atr_attempts", "label": "ATR Attempts"},
        {"key": "team.total_fg2_makes", "label": "2FG Makes"},
        {"key": "team.total_fg2_attempts", "label": "2FG Attempts"},
        {"key": "team.total_fg3_makes", "label": "3FG Makes"},
        {"key": "team.total_fg3_attempts", "label": "3FG Attempts"},
        {"key": "team.total_ftm", "label": "FT Made"},
        {"key": "team.total_fta", "label": "FT Attempted"},
    ],
    "Opponent — Totals": [
        {"key": "opp.total_points", "label": "Opponent Points"},
        {"key": "opp.total_possessions", "label": "Opponent Possessions"},
        {"key": "opp.total_turnovers", "label": "Opponent Turnovers"},
        {"key": "opp.total_assists", "label": "Opponent Assists"},
        {"key": "opp.total_atr_makes", "label": "Opponent ATR Makes"},
        {"key": "opp.total_atr_attempts", "label": "Opponent ATR Attempts"},
        {"key": "opp.total_fg2_makes", "label": "Opponent 2FG Makes"},
        {"key": "opp.total_fg2_attempts", "label": "Opponent 2FG Attempts"},
        {"key": "opp.total_fg3_makes", "label": "Opponent 3FG Makes"},
        {"key": "opp.total_fg3_attempts", "label": "Opponent 3FG Attempts"},
        {"key": "opp.total_ftm", "label": "Opponent FT Made"},
        {"key": "opp.total_fta", "label": "Opponent FT Attempted"},
    ],
    "Player — Totals": [
        {"key": "player.points", "label": "Points"},
        {"key": "player.assists", "label": "Assists"},
        {"key": "player.turnovers", "label": "Turnovers"},
        {"key": "player.fg2_makes", "label": "2FG Makes"},
        {"key": "player.fg2_attempts", "label": "2FG Attempts"},
        {"key": "player.fg3_makes", "label": "3FG Makes"},
        {"key": "player.fg3_attempts", "label": "3FG Attempts"},
        {"key": "player.ftm", "label": "FT Made"},
        {"key": "player.fta", "label": "FT Attempted"},
    ],
    "Blue Collar — Team": [
        {"key": "bc.team.total_blue_collar", "label": "Team Blue Collar Total"},
    ],
    "Blue Collar — Player": [
        {"key": "bc.player.total_blue_collar", "label": "Player Blue Collar Total"},
    ],
}

STAT_KEY_ALIASES = {
    "team.points": "team.total_points",
    "team.assists": "team.total_assists",
    "team.turnovers": "team.total_turnovers",
    "team.possessions": "team.total_possessions",
    "team.fg2_makes": "team.total_fg2_makes",
    "team.fg2_attempts": "team.total_fg2_attempts",
    "team.fg3_makes": "team.total_fg3_makes",
    "team.fg3_attempts": "team.total_fg3_attempts",
    "team.ftm": "team.total_ftm",
    "team.fta": "team.total_fta",
    "team.blue_collar": "bc.team.total_blue_collar",
    "opp.points": "opp.total_points",
    "opp.assists": "opp.total_assists",
    "opp.turnovers": "opp.total_turnovers",
    "opp.possessions": "opp.total_possessions",
    "opp.fg2_makes": "opp.total_fg2_makes",
    "opp.fg2_attempts": "opp.total_fg2_attempts",
    "opp.fg3_makes": "opp.total_fg3_makes",
    "opp.fg3_attempts": "opp.total_fg3_attempts",
    "opp.ftm": "opp.total_ftm",
    "opp.fta": "opp.total_fta",
    "opponent.points": "opp.total_points",
    "opponent.assists": "opp.total_assists",
    "opponent.turnovers": "opp.total_turnovers",
    "opponent.possessions": "opp.total_possessions",
    "opponent.fg2_makes": "opp.total_fg2_makes",
    "opponent.fg2_attempts": "opp.total_fg2_attempts",
    "opponent.fg3_makes": "opp.total_fg3_makes",
    "opponent.fg3_attempts": "opp.total_fg3_attempts",
    "opponent.ftm": "opp.total_ftm",
    "opponent.fta": "opp.total_fta",
    "blue_collar.total": "bc.team.total_blue_collar",
    "bc.total": "bc.team.total_blue_collar",
}


def canonicalize_stat_key(key: str) -> str:
    """Return the canonical stat key for a given alias."""
    return STAT_KEY_ALIASES.get(key, key)


def get_all_stat_keys() -> set[str]:
    """Return all canonical stat keys in the registry."""
    keys = set()
    for group in STAT_KEY_GROUPS.values():
        for entry in group:
            keys.add(entry["key"])
    return keys


def get_grouped_options() -> list[tuple[str, list[tuple[str, str]]]]:
    """Return grouped (key, label) options for templates."""
    grouped: list[tuple[str, list[tuple[str, str]]]] = []
    for group_name, entries in STAT_KEY_GROUPS.items():
        grouped.append(
            (
                group_name,
                [(entry["key"], entry["label"]) for entry in entries],
            )
        )
    return grouped


def get_label_for_key(key: str) -> str:
    """Return a friendly label for a stat key (falls back to the key)."""
    canonical_key = canonicalize_stat_key(key)
    for entries in STAT_KEY_GROUPS.values():
        for entry in entries:
            if entry["key"] == canonical_key:
                return entry["label"]
    return key
