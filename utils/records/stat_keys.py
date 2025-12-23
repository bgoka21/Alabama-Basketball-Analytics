"""Registry of record stat keys and qualification defaults."""

STAT_KEYS = {
    "team": {
        "TEAM": [
            "team.points",
            "team.assists",
            "team.turnovers",
            "team.fg2_makes",
            "team.fg2_attempts",
            "team.fg3_makes",
            "team.fg3_attempts",
            "team.ftm",
            "team.fta",
            "team.possessions",
            "team.blue_collar",
        ],
    },
    "player": {
        "PLAYER": [
            "player.points",
            "player.assists",
            "player.turnovers",
            "player.fg2_makes",
            "player.fg2_attempts",
            "player.fg3_makes",
            "player.fg3_attempts",
            "player.ftm",
            "player.fta",
        ],
    },
    "opponent": {
        "OPPONENT": [
            "opponent.points",
            "opponent.assists",
            "opponent.turnovers",
            "opponent.fg2_makes",
            "opponent.fg2_attempts",
            "opponent.fg3_makes",
            "opponent.fg3_attempts",
            "opponent.ftm",
            "opponent.fta",
            "opponent.possessions",
        ],
    },
    "blue_collar": {
        "TEAM": [
            "blue_collar.total",
            "blue_collar.rebounds",
            "blue_collar.deflections",
            "blue_collar.loose_balls",
            "blue_collar.charges",
        ],
    },
}

DEFAULT_QUALIFIER_THRESHOLDS = {
    "team.possessions": 50,
    "opponent.possessions": 50,
    "team.fga": 30,
    "team.3pa": 10,
    "team.fta": 10,
    "player.fga": 10,
    "player.3pa": 5,
    "player.fta": 5,
}
