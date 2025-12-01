"""Player on/off possession efficiency calculations.

This helper mirrors team possession/point logic for player-level splits.
"""
from typing import Any, Dict

import pandas as pd


def _get_off_points(value: str) -> int:
    """Return points scored for an offensive possession cell."""
    if not isinstance(value, str):
        return 0
    if "ATR+" in value or "2FG+" in value:
        return 2
    if "3FG+" in value:
        return 3
    if "FT+" in value:
        return value.count("FT+")
    return 0


def _get_def_points(value: str) -> int:
    """Return opponent points scored for a defensive possession cell."""
    if not isinstance(value, str):
        return 0
    if "ATR+" in value or "2FG+" in value:
        return 2
    if "3FG+" in value:
        return 3
    if "FT+" in value:
        return value.count("FT+")
    return 0


def _calc_ppp(points: float, possessions: int) -> float:
    return points / possessions if possessions else 0


def get_on_off_summary(df: pd.DataFrame, player_name: str) -> Dict[str, Any]:
    """Compute offensive/defensive PPP and leverage for a single player.

    The calculations align with team-level possession and point parsing rules so
    that player on/off splits use the same counting logic.
    """

    # Offensive team totals
    team_off_rows = df[df["Row"] == "Offense"]
    team_points = team_off_rows["STATS"].apply(_get_off_points).sum()
    team_possessions = len(team_off_rows)

    # Player offensive possessions/points
    player_off_rows = team_off_rows[
        team_off_rows["PLAYER POSSESSIONS"].str.contains(player_name, na=False)
    ]
    off_points = player_off_rows["STATS"].apply(_get_off_points).sum()
    off_possessions = len(player_off_rows)

    ppp_on = _calc_ppp(off_points, off_possessions)
    ppp_off = _calc_ppp(team_points - off_points, team_possessions - off_possessions)
    leverage_off = ppp_on - ppp_off

    # Defensive team totals (exclude neutral/offensive rebounds like team logic)
    def_rows = df[df["Row"] == "Defense"]
    valid_def_rows = def_rows[~def_rows["STATS"].str.contains("Neutral|Off Reb", na=False)]

    team_points_allowed = valid_def_rows["OPP STATS"].apply(_get_def_points).sum()
    team_def_possessions = len(valid_def_rows)
    total_possessions = team_def_possessions

    player_def_rows = valid_def_rows[
        valid_def_rows["PLAYER POSSESSIONS"].str.contains(player_name, na=False)
    ]
    def_points_allowed = player_def_rows["OPP STATS"].apply(_get_def_points).sum()
    def_possessions = len(player_def_rows)

    ppp_def_on = _calc_ppp(def_points_allowed, def_possessions)
    ppp_def_off = _calc_ppp(
        team_points_allowed - def_points_allowed,
        team_def_possessions - def_possessions,
    )
    leverage_def = ppp_def_off - ppp_def_on

    percent_off = off_possessions / team_possessions if team_possessions else 0
    percent_def = def_possessions / total_possessions if total_possessions else 0

    return {
        "offense": {
            "possessions_on": off_possessions,
            "team_possessions": team_possessions,
            "percent_of_team_possessions": percent_off,
            "ppp_on": ppp_on,
            "ppp_off": ppp_off,
            "leverage": leverage_off,
        },
        "defense": {
            "possessions_on": def_possessions,
            "team_possessions": team_def_possessions,
            "percent_of_team_possessions": percent_def,
            "ppp_on": ppp_def_on,
            "ppp_off": ppp_def_off,
            "leverage": leverage_def,
        },
    }
