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
    resolved_season_id = getattr(player, "season_id", None)
    if not resolved_season_id:
        latest_season = db_session.query(Season).order_by(Season.start_date.desc()).first()
        if latest_season:
            resolved_season_id = latest_season.id
    season_name = None
    if resolved_season_id:
        season_name = (
            db_session.query(Season.season_name)
            .filter(Season.id == resolved_season_id)
            .scalar()
        )
    stats_query = db_session.query(PlayerStats).filter(PlayerStats.player_name == player_name)
    if resolved_season_id:
        stats_query = stats_query.filter(PlayerStats.season_id == resolved_season_id)
    stats_rows = stats_query.all()

    # Mirror the website's game-type filtering:
    # 1) keep only game records (not practice)
    # 2) exclude Exhibition by default (same as DEFAULT_GAME_TYPE_SELECTION)
    default_game_types = ["Non-Conference", "Conference", "Postseason"]
    stats_rows = [
        r for r in stats_rows
        if r.game_id and r.game and any(tag in default_game_types for tag in r.game.game_types)
    ]

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
