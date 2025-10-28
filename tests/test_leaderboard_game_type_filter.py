from datetime import date

from models.database import db, Game, PlayerStats, Roster, Season


def _add_player_with_game(
    season: Season,
    *,
    name: str,
    jersey: int,
    game_date: date,
    opponent: str,
    game_types: list[str],
    fg3_makes: int = 0,
    fg3_attempts: int = 0,
    atr_makes: int = 0,
    atr_attempts: int = 0,
) -> None:
    roster = Roster(season_id=season.id, player_name=name)
    db.session.add(roster)

    game = Game(
        season_id=season.id,
        game_date=game_date,
        opponent_name=opponent,
        home_or_away="Home",
        result="W",
        csv_filename=f"{opponent.lower()}-{game_date.isoformat()}.csv",
    )
    game.game_types = game_types
    db.session.add(game)
    db.session.flush()

    stats = PlayerStats(
        season_id=season.id,
        game_id=game.id,
        player_name=name,
        jersey_number=jersey,
        fg3_makes=fg3_makes,
        fg3_attempts=fg3_attempts,
        atr_makes=atr_makes,
        atr_attempts=atr_attempts,
    )
    db.session.add(stats)


def test_leaderboard_game_type_filter(client, app):
    season_id = None
    with app.app_context():
        season = Season(
            season_name="2024-25",
            start_date=date(2024, 6, 1),
            end_date=date(2025, 4, 1),
        )
        db.session.add(season)
        db.session.flush()

        _add_player_with_game(
            season,
            name="#1 Alpha",
            jersey=1,
            game_date=date(2024, 11, 5),
            opponent="State",
            game_types=["Conference"],
            fg3_makes=2,
            fg3_attempts=3,
        )
        _add_player_with_game(
            season,
            name="#2 Beta",
            jersey=2,
            game_date=date(2024, 11, 12),
            opponent="Tech",
            game_types=["Exhibition"],
            atr_makes=4,
            atr_attempts=6,
        )

        db.session.commit()
        season_id = season.id

    response = client.get("/admin/leaderboard/game", query_string={"season": season_id})
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "#1 Alpha" in html
    assert "#2 Beta" in html

    response = client.get(
        "/admin/leaderboard/game",
        query_string=[("season", season_id), ("game_type", "Conference")],
    )
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "#1 Alpha" in html
    assert "#2 Beta" not in html
