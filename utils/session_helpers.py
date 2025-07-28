from sqlalchemy import and_, or_
from models.database import PlayerStats, Game, Practice


def get_player_stats_for_date_range(player_name, start_date, end_date):
    """Return aggregated stats for a player within a date range."""
    # Import here to avoid circular imports when admin.routes imports this module
    from admin.routes import aggregate_stats

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
    return aggregate_stats(records)

