"""Compile player shot data for PDF generation.
These helpers reuse the Shot Type tab data pipeline without recomputing stats.
"""
from __future__ import annotations

import re

from admin.routes import compute_team_shot_details
from models.database import PlayerStats, Season


def compile_player_shot_data(player, db_session):
    """Return full player shot report payload based on Shot Type tab data."""
    player_name = getattr(player, "player_name", None) or "Unknown"
    season_name = None
    if getattr(player, "season_id", None):
        season_name = (
            db_session.query(Season.season_name)
            .filter(Season.id == player.season_id)
            .scalar()
        )
    stats_rows = (
        db_session.query(PlayerStats)
        .filter(PlayerStats.player_name == player_name)
        .all()
    )
    shot_type_totals, shot_summaries = compute_team_shot_details(stats_rows, label_set=None)

    # Strip leading #<number> from the raw DB name so the renderer can
    # safely reconstruct "#{number} {name}" without doubling.
    clean_name = re.sub(r"^#\d+\s*", "", player_name)

    return {
        "name": clean_name,
        "number": _extract_jersey_number(player_name),
        "season": season_name or "",
        "shot_type_totals": shot_type_totals,
        "shot_summaries": shot_summaries,
    }


def _extract_jersey_number(player_name: str | None) -> str:
    if not player_name:
        return ""
    text = player_name.strip()
    if text.startswith("#"):
        text = text[1:]
    number = ""
    for ch in text:
        if ch.isdigit():
            number += ch
        else:
            break
    return number
