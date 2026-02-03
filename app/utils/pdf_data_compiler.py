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
    season_stats = _build_season_stats(stats_rows)
    # Strip leading #<number> from the raw DB name so the renderer can
    # safely reconstruct "#{number} {name}" without doubling.
    clean_name = re.sub(r"^#\d+\s*", "", player_name)
    return {
        "name": clean_name,
        "number": _extract_jersey_number(player_name),
        "season": season_name or "",
        "shot_type_totals": shot_type_totals,
        "shot_summaries": shot_summaries,
        "season_stats": season_stats,
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


def _build_season_stats(stats_rows):
    atr_makes = sum(row.atr_makes or 0 for row in stats_rows)
    atr_attempts = sum(row.atr_attempts or 0 for row in stats_rows)
    fg2_makes = sum(row.fg2_makes or 0 for row in stats_rows)
    fg2_attempts = sum(row.fg2_attempts or 0 for row in stats_rows)
    fg3_makes = sum(row.fg3_makes or 0 for row in stats_rows)
    fg3_attempts = sum(row.fg3_attempts or 0 for row in stats_rows)
    ftm = sum(row.ftm or 0 for row in stats_rows)
    fta = sum(row.fta or 0 for row in stats_rows)
    points = sum(row.points or 0 for row in stats_rows)

    total_fga = atr_attempts + fg2_attempts + fg3_attempts
    total_makes = atr_makes + fg2_makes + fg3_makes
    efg_pct = ((total_makes + 0.5 * fg3_makes) / total_fga * 100) if total_fga else 0.0
    pps = (efg_pct / 100) * 2 if total_fga else 0.0
    ft_pct = (ftm / fta * 100) if fta else 0.0
    ts_denom = 2 * (total_fga + 0.44 * fta)
    ts_pct = (points / ts_denom * 100) if ts_denom else 0.0

    return {
        "ft_pct": ft_pct,
        "ts_pct": ts_pct,
        "pps": pps,
        "efg_pct": efg_pct,
    }
