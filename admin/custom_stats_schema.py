from __future__ import annotations

from typing import Optional, TypedDict


class PlayerGameStatsRow(TypedDict, total=False):
    """Shape of a single row in the game custom stats table."""

    player: str
    adv_ppp_on_offense: Optional[float]
    adv_ppp_off_offense: Optional[float]
    adv_offensive_leverage: Optional[float]
    adv_ppp_on_defense: Optional[float]
    adv_ppp_off_defense: Optional[float]
    adv_defensive_leverage: Optional[float]
    adv_off_possession_pct: Optional[float]
    adv_def_possession_pct: Optional[float]
    summary: dict

