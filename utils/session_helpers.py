from sqlalchemy import and_, or_
from types import SimpleNamespace
from models.database import PlayerStats, Game, Practice, Roster, db


def get_player_stats_for_date_range(player_id, start_date, end_date, *, labels=None, **kwargs):
    """Return aggregated stats for a player within a date range.

    Optionally filter by a set of ``labels`` representing drill labels.
    ``player_id`` should be a ``Roster`` id.
    """
    # Import here to avoid circular imports when admin.routes imports this module
    from admin.routes import (
        aggregate_stats,
        compute_filtered_totals,
        compute_filtered_blue,
    )

    roster = db.session.get(Roster, player_id)
    if not roster:
        return SimpleNamespace()

    player_name = roster.player_name

    records = (
        PlayerStats.query
        .filter(PlayerStats.player_name == player_name)
        .outerjoin(Game, PlayerStats.game_id == Game.id)
        .outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        .filter(
            or_(
                and_(
                    PlayerStats.game_id != None,
                    Game.game_date >= start_date,
                    Game.game_date <= end_date,
                ),
                and_(
                    PlayerStats.practice_id != None,
                    Practice.date >= start_date,
                    Practice.date <= end_date,
                ),
            )
        )
        .all()
    )

    if labels is None and 'drill_labels' in kwargs:
        labels = kwargs['drill_labels']

    if labels:
        label_set = {lbl.strip().upper() for lbl in labels if lbl.strip()}
        totals = compute_filtered_totals(records, label_set)
        blue_totals = compute_filtered_blue(records, label_set)
    else:
        totals = aggregate_stats(records)
        # Unfiltered blue-collar counts from stat details
        blue_totals = compute_filtered_blue(records, None)

    combined = {**totals.__dict__, **blue_totals.__dict__}
    return SimpleNamespace(**combined)

