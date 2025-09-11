from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import func, or_, and_

from models.database import (
    db, PlayerStats, BlueCollarStats, Roster,
    Possession, PlayerPossession, ShotDetail,
    Game, Practice
)


def get_on_court_metrics(player_id, start_date=None, end_date=None, labels=None):
    """Return on-court offensive metrics for a player."""
    roster = db.session.get(Roster, player_id)
    if not roster:
        return {}

    label_set = {lbl.strip().upper() for lbl in labels or [] if lbl.strip()}
    offense_sides = ("Offense", "Crimson", "White")

    # Determine the player's squad based on practice scrimmage data. If the
    # player appears in possessions tagged with a specific squad name (e.g.
    # "Crimson" or "White"), treat the most common value as the player's squad
    # and restrict team calculations to that side.  This allows us to exclude
    # possessions from the opposing squad when computing PPP_on/PPP_off.
    squad_row = (
        db.session.query(Possession.possession_side, func.count(Possession.id))
        .join(PlayerPossession, Possession.id == PlayerPossession.possession_id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
            Possession.possession_side.in_(('Crimson', 'White')),
        )
        .group_by(Possession.possession_side)
        .order_by(func.count(Possession.id).desc())
        .first()
    )
    player_squad = squad_row[0] if squad_row else None

    poss_q = (
        db.session.query(
            func.count(PlayerPossession.id),
            func.coalesce(func.sum(Possession.points_scored), 0),
        )
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
        )
    )
    if player_squad:
        poss_q = poss_q.filter(Possession.possession_side == player_squad)
    else:
        poss_q = poss_q.filter(Possession.possession_side.in_(offense_sides))
    if start_date or end_date:
        poss_q = (
            poss_q.outerjoin(Game, Possession.game_id == Game.id)
                   .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_date:
            poss_q = poss_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_date),
                    and_(Possession.practice_id != None, Practice.date >= start_date),
                )
            )
        if end_date:
            poss_q = poss_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_date),
                    and_(Possession.practice_id != None, Practice.date <= end_date),
                )
            )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        poss_q = poss_q.filter(or_(*clauses))
    ON_poss, ON_pts = poss_q.one()

    team_q = (
        db.session.query(
            func.count(Possession.id),
            func.coalesce(func.sum(Possession.points_scored), 0),
        )
        .filter(Possession.season_id == roster.season_id)
    )
    if player_squad:
        team_q = team_q.filter(Possession.possession_side == player_squad)
    else:
        team_q = team_q.filter(Possession.possession_side.in_(offense_sides))
    if start_date or end_date:
        team_q = (
            team_q.outerjoin(Game, Possession.game_id == Game.id)
                    .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_date:
            team_q = team_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_date),
                    and_(Possession.practice_id != None, Practice.date >= start_date),
                )
            )
        if end_date:
            team_q = team_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_date),
                    and_(Possession.practice_id != None, Practice.date <= end_date),
                )
            )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        team_q = team_q.filter(or_(*clauses))
    TEAM_poss, TEAM_pts = team_q.one()

    def ev_count(ev_type: str) -> int:
        q = (
            db.session.query(func.count(ShotDetail.id))
            .join(Possession, ShotDetail.possession_id == Possession.id)
            .join(PlayerPossession, Possession.id == PlayerPossession.possession_id)
            .filter(
                PlayerPossession.player_id == player_id,
                Possession.season_id == roster.season_id,
                ShotDetail.event_type == ev_type,
            )
        )
        if player_squad:
            q = q.filter(Possession.possession_side == player_squad)
        else:
            q = q.filter(Possession.possession_side.in_(offense_sides))
        if start_date or end_date:
            q = q.outerjoin(Game, Possession.game_id == Game.id).outerjoin(Practice, Possession.practice_id == Practice.id)
            if start_date:
                q = q.filter(
                    or_(
                        and_(Possession.game_id != None, Game.game_date >= start_date),
                        and_(Possession.practice_id != None, Practice.date >= start_date),
                    )
                )
            if end_date:
                q = q.filter(
                    or_(
                        and_(Possession.game_id != None, Game.game_date <= end_date),
                        and_(Possession.practice_id != None, Practice.date <= end_date),
                    )
                )
        if label_set:
            q = q.filter(or_(*[Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]))
        return q.scalar() or 0

    turnovers_on = ev_count("Turnover")
    # Personal off rebounds and fouls are tracked in BlueCollarStats and
    # PlayerStats respectively. Pull those aggregates using the same
    # date/label filters applied above so values mirror the leaderboard.
    bc_q = BlueCollarStats.query.filter(
        BlueCollarStats.player_id == player_id,
        BlueCollarStats.season_id == roster.season_id,
    )
    ps_filter_q = PlayerStats.query.filter(
        PlayerStats.player_name == roster.player_name,
        PlayerStats.season_id == roster.season_id,
    )
    if start_date or end_date:
        bc_q = bc_q.outerjoin(Game, BlueCollarStats.game_id == Game.id)
        bc_q = bc_q.outerjoin(Practice, BlueCollarStats.practice_id == Practice.id)
        ps_filter_q = ps_filter_q.outerjoin(Game, PlayerStats.game_id == Game.id)
        ps_filter_q = ps_filter_q.outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        if start_date:
            bc_q = bc_q.filter(
                or_(
                    and_(BlueCollarStats.game_id != None, Game.game_date >= start_date),
                    and_(BlueCollarStats.practice_id != None, Practice.date >= start_date),
                )
            )
            ps_filter_q = ps_filter_q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_date),
                    and_(PlayerStats.practice_id != None, Practice.date >= start_date),
                )
            )
        if end_date:
            bc_q = bc_q.filter(
                or_(
                    and_(BlueCollarStats.game_id != None, Game.game_date <= end_date),
                    and_(BlueCollarStats.practice_id != None, Practice.date <= end_date),
                )
            )
            ps_filter_q = ps_filter_q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_date),
                    and_(PlayerStats.practice_id != None, Practice.date <= end_date),
                )
            )
    if label_set:
        bc_q = bc_q.join(
            PlayerStats,
            and_(
                PlayerStats.season_id == BlueCollarStats.season_id,
                PlayerStats.player_name == roster.player_name,
                PlayerStats.practice_id == BlueCollarStats.practice_id,
                PlayerStats.game_id == BlueCollarStats.game_id,
            ),
        )
        clauses = [
            PlayerStats.shot_type_details.ilike(f"%{lbl}%") | PlayerStats.stat_details.ilike(f"%{lbl}%")
            for lbl in label_set
        ]
        bc_q = bc_q.filter(or_(*clauses))
        ps_clauses = [
            PlayerStats.shot_type_details.ilike(f"%{lbl}%") | PlayerStats.stat_details.ilike(f"%{lbl}%")
            for lbl in label_set
        ]
        ps_filter_q = ps_filter_q.filter(or_(*ps_clauses))
    off_reb_on = bc_q.with_entities(func.coalesce(func.sum(BlueCollarStats.off_reb), 0)).scalar() or 0
    records = ps_filter_q.all()
    if label_set:
        from admin.routes import compute_filtered_totals
        totals = compute_filtered_totals(records, label_set)
    else:
        from admin.routes import aggregate_stats
        totals = aggregate_stats(records)
    fouls_drawn_on = totals.foul_by
    player_turnovers = totals.turnovers
    team_missed_on = sum(ev_count(ev) for ev in ["ATR-", "2FG-", "3FG-"])
    total_fga = totals.atr_attempts + totals.fg2_attempts + totals.fg3_attempts
    denominator = (
        player_turnovers
        + total_fga
        + totals.pot_assists
        + totals.assists
    )
    indiv_team_to_pct = (
        round(player_turnovers / turnovers_on * 100, 1)
        if turnovers_on
        else None
    )


    return {
        'offensive_poss_on': round(ON_poss, 0),
        'ppp_on': round(ON_pts / ON_poss, 2) if ON_poss else None,
        'team_turnover_rate_on': round(turnovers_on / ON_poss * 100, 1) if ON_poss else None,
        'indiv_turnover_rate': round(player_turnovers / ON_poss * 100, 1) if ON_poss else None,
        'bamalytics_turnover_rate': round(player_turnovers / denominator * 100, 1) if denominator else None,
        'individual_team_turnover_pct': indiv_team_to_pct,
        'ind_off_reb_pct': round(off_reb_on / team_missed_on * 100, 1) if team_missed_on else None,
        'ind_fouls_drawn_pct': round(fouls_drawn_on / ON_poss * 100, 1) if ON_poss else None,
    }


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
    on_court = get_on_court_metrics(
        player_id,
        labels=labels,
    )
    stats_map.update(on_court)

    return SimpleNamespace(**stats_map)
