import json

import json

import pytest
from flask import Flask

from admin.routes import compute_leaderboard
from models.database import db, PlayerStats, Roster, Season
from utils.shottype import persist_player_shot_details


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["TESTING"] = True

    db.init_app(app)

    with app.app_context():
        db.create_all()

        db.session.add(Season(id=1, season_name="2024", start_date=None))
        db.session.add(Roster(id=1, season_id=1, player_name="Contest Player"))

        player_stat = PlayerStats(
            season_id=1,
            player_name="Contest Player",
            atr_contest_attempts=4,
            atr_contest_makes=3,
            atr_late_attempts=2,
            atr_late_makes=1,
            atr_no_contest_attempts=1,
            atr_no_contest_makes=1,
            fg2_contest_attempts=5,
            fg2_contest_makes=2,
            fg2_late_attempts=1,
            fg2_late_makes=1,
            fg2_no_contest_attempts=2,
            fg2_no_contest_makes=1,
            fg3_contest_attempts=3,
            fg3_contest_makes=1,
            fg3_late_attempts=2,
            fg3_late_makes=1,
            fg3_no_contest_attempts=1,
            fg3_no_contest_makes=1,
            shot_type_details=json.dumps([]),
        )
        db.session.add(player_stat)
        persist_player_shot_details(player_stat, [], replace=True)

        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


def _rows_to_map(rows):
    return {player: value for player, value in rows}


def test_leaderboard_includes_contest_totals(app):
    with app.app_context():
        _, rows, _ = compute_leaderboard("fg3_no_contest_attempts", season_id=1)

    row_map = _rows_to_map(rows)
    assert row_map["Contest Player"] == 1


def test_leaderboard_contest_percentages(app):
    with app.app_context():
        _, atr_rows, _ = compute_leaderboard("atr_contest_pct", season_id=1)
        _, fg2_rows, _ = compute_leaderboard("fg2_no_contest_pct", season_id=1)
        _, fg3_rows, _ = compute_leaderboard("fg3_late_pct", season_id=1)

    atr_map = _rows_to_map(atr_rows)
    fg2_map = _rows_to_map(fg2_rows)
    fg3_map = _rows_to_map(fg3_rows)

    assert atr_map["Contest Player"] == pytest.approx(75.0)
    assert fg2_map["Contest Player"] == pytest.approx(50.0)
    assert fg3_map["Contest Player"] == pytest.approx(50.0)
