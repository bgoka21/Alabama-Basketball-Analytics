from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import func, or_

from models.database import (
    db, PlayerStats, BlueCollarStats, Roster,
    Possession, PlayerPossession, ShotDetail
)


def get_player_overall_stats(player_id: int, labels=None):
    """Return season totals and on-court metrics for one player."""
    # Import heavy helpers lazily to avoid circular deps
    from admin.routes import aggregate_stats, compute_filtered_totals, compute_filtered_blue

    roster = db.session.get(Roster, player_id)
    if not roster:
        return SimpleNamespace()

    label_set = {lbl.strip().upper() for lbl in labels or [] if lbl.strip()}

    # --- PlayerStats/BlueCollar aggregates ---
    stats_query = PlayerStats.query.filter(
        PlayerStats.player_name == roster.player_name,
        PlayerStats.season_id == roster.season_id,
    )
    records = stats_query.all()

    if label_set:
        totals = compute_filtered_totals(records, label_set)
        blue = compute_filtered_blue(records, label_set)
    else:
        totals = aggregate_stats(records)
        blue = compute_filtered_blue(records, None)

    stats_map = totals.__dict__.copy()
    total_shots = (
        stats_map.get("atr_attempts", 0)
        + stats_map.get("fg2_attempts", 0)
        + stats_map.get("fg3_attempts", 0)
    )
    stats_map["two_fg_pct"] = (
        round(stats_map.get("fg2_makes", 0) / stats_map.get("fg2_attempts", 0) * 100, 1)
        if stats_map.get("fg2_attempts", 0)
        else 0.0
    )
    stats_map["two_fg_freq_pct"] = (
        round(stats_map.get("fg2_attempts", 0) / total_shots * 100, 1) if total_shots else 0.0
    )
    # rename fg3_pct/freq to three_fg_*
    stats_map["three_fg_pct"] = stats_map.pop("fg3_pct", 0)
    stats_map["three_fg_freq_pct"] = stats_map.pop("fg3_freq_pct", 0)

    stats_map.update(blue.__dict__)

    # --- On-court metrics ---
    offense_sides = ("Offense", "Crimson", "White")
    poss_q = (
        db.session.query(func.count(PlayerPossession.id), func.coalesce(func.sum(Possession.points_scored), 0))
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
            Possession.possession_side.in_(offense_sides),
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        poss_q = poss_q.filter(or_(*clauses))
    on_poss, on_pts = poss_q.one()

    team_q = (
        db.session.query(func.count(Possession.id), func.coalesce(func.sum(Possession.points_scored), 0))
        .filter(
            Possession.season_id == roster.season_id,
            Possession.possession_side.in_(offense_sides),
        )
    )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        team_q = team_q.filter(or_(*clauses))
    team_poss, team_pts = team_q.one()

    def ev_count(ev_type: str) -> int:
        q = (
            db.session.query(func.count(ShotDetail.id))
            .join(Possession, ShotDetail.possession_id == Possession.id)
            .join(PlayerPossession, Possession.id == PlayerPossession.possession_id)
            .filter(
                PlayerPossession.player_id == player_id,
                Possession.season_id == roster.season_id,
                Possession.possession_side.in_(offense_sides),
                ShotDetail.event_type == ev_type,
            )
        )
        if label_set:
            q = q.filter(or_(*clauses))
        return q.scalar() or 0

    turnovers_on = ev_count("Turnover")
    off_reb_events = ev_count("Off Rebound")
    fouled_events = ev_count("Fouled")
    team_misses = sum(ev_count(ev) for ev in ["ATR-", "2FG-", "3FG-"])

    ppp_on = on_pts / on_poss if on_poss else 0
    team_to_rate = turnovers_on / on_poss if on_poss else 0
    indiv_to_rate = stats_map.get("turnovers", 0) / on_poss if on_poss else 0
    ind_oreb_pct = off_reb_events / team_misses if team_misses else 0
    ind_fd_pct = fouled_events / on_poss if on_poss else 0

    stats_map.update(
        offensive_poss_on=on_poss,
        ppp_on=round(ppp_on, 2),
        team_turnover_rate_on=round(team_to_rate * 100, 1),
        indiv_turnover_rate=round(indiv_to_rate * 100, 1),
        ind_off_reb_pct=round(ind_oreb_pct * 100, 1),
        ind_fouls_drawn_pct=round(ind_fd_pct * 100, 1),
    )

    return SimpleNamespace(**stats_map)
