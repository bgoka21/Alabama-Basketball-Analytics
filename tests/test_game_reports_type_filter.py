from datetime import date

import pytest

from models.database import db, Season, Game
from werkzeug.datastructures import MultiDict


@pytest.fixture
def season(app):
    with app.app_context():
        season = Season(
            season_name="2024-25",
            start_date=date(2024, 6, 1),
            end_date=date(2025, 4, 1),
        )
        db.session.add(season)
        db.session.commit()
        yield season


def _create_game(season_id: int, **kwargs) -> Game:
    game = Game(
        season_id=season_id,
        game_date=kwargs.get("game_date", date(2024, 10, 1)),
        opponent_name=kwargs.get("opponent_name", "Opponent"),
        home_or_away=kwargs.get("home_or_away", "Home"),
        result=kwargs.get("result", "W"),
        csv_filename=kwargs.get("csv_filename", f"{kwargs.get('opponent_name', 'opp')}.csv"),
    )
    if "game_types" in kwargs and kwargs["game_types"] is not None:
        game.game_types = kwargs["game_types"]
    return game


def test_game_reports_type_filter(client, app, season):
    with app.app_context():
        exhibition = _create_game(
            season_id=season.id,
            opponent_name="State",
            csv_filename="state.csv",
            game_types=["Exhibition"],
        )
        conference = _create_game(
            season_id=season.id,
            opponent_name="Tech",
            game_date=date(2024, 11, 15),
            csv_filename="tech.csv",
            game_types=["Conference"],
        )
        db.session.add_all([exhibition, conference])
        db.session.commit()

    response = client.get(f"/admin/game-reports?season_id={season.id}&game_type=Exhibition")
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "State" in html
    assert "Tech" not in html
    assert "Type" in html
    assert ">Exhibition</span>" in html


def test_edit_game_updates_game_type(client, app, season):
    with app.app_context():
        game = _create_game(
            season_id=season.id,
            opponent_name="Rivals",
            game_date=date(2024, 12, 1),
            csv_filename="rivals.csv",
        )
        db.session.add(game)
        db.session.commit()
        game_id = game.id
        game_date_value = game.game_date.isoformat()

    form_data = MultiDict(
        [
            ("game_date", game_date_value),
            ("opponent_name", "Rivals"),
            ("result", "L"),
            ("game_type", "Conference"),
            ("game_type", "Postseason"),
        ]
    )
    response = client.post(
        f"/admin/game/{game_id}/edit",
        data=form_data,
        follow_redirects=True,
    )
    assert response.status_code == 200

    with app.app_context():
        updated = db.session.get(Game, game_id)
        assert updated.game_types == ["Conference", "Postseason"]
        assert updated.result == "L"


def test_game_reports_multiple_type_filter(client, app, season):
    with app.app_context():
        exhibition = _create_game(
            season_id=season.id,
            opponent_name="Bears",
            csv_filename="bears.csv",
            game_types=["Exhibition"],
        )
        conference = _create_game(
            season_id=season.id,
            opponent_name="Aggies",
            game_date=date(2024, 11, 20),
            csv_filename="aggies.csv",
            game_types=["Conference"],
        )
        postseason = _create_game(
            season_id=season.id,
            opponent_name="Hawks",
            game_date=date(2025, 3, 10),
            csv_filename="hawks.csv",
            game_types=["Postseason"],
        )
        db.session.add_all([exhibition, conference, postseason])
        db.session.commit()

    response = client.get(
        "/admin/game-reports",
        query_string=[
            ("season_id", season.id),
            ("game_type", "Conference"),
            ("game_type", "Postseason"),
        ],
    )
    assert response.status_code == 200
    html = response.data.decode("utf-8")
    assert "Aggies" in html
    assert "Hawks" in html
    assert "Bears" not in html
    assert 'value="Conference"' in html and 'checked' in html.split('value="Conference"', 1)[1]
    assert 'value="Postseason"' in html and 'checked' in html.split('value="Postseason"', 1)[1]
