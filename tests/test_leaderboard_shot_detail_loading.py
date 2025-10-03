import json

import pytest
from flask import Flask

from admin.routes import compute_leaderboard
from models.database import db, Season, Roster, PlayerStats


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["TESTING"] = True

    db.init_app(app)

    with app.app_context():
        db.create_all()

        db.session.add(Season(id=1, season_name="2024-25"))
        db.session.add(Roster(id=1, season_id=1, player_name="Test Player"))

        shots = [
            {
                "shot_class": "3fg",
                "result": "made",
                "possession_type": "Halfcourt",
                "Assisted": True,
                "3fg_shrink": "Shrink",
                "drill_labels": ["Sample"],
            },
            {
                "shot_class": "3fg",
                "result": "made",
                "possession_type": "Transition",
                "Assisted": False,
                "3fg_shrink": "Non-Shrink",
                "drill_labels": ["Sample"],
            },
            {
                "shot_class": "3fg",
                "result": "miss",
                "possession_type": "Halfcourt",
                "Assisted": True,
                "3fg_shrink": "Shrink",
                "drill_labels": ["Sample"],
            },
        ]

        db.session.add(
            PlayerStats(
                player_name="Test Player",
                season_id=1,
                points=12,
                fg3_attempts=3,
                fg3_makes=2,
                atr_attempts=0,
                atr_makes=0,
                fg2_attempts=0,
                fg2_makes=0,
                shot_type_details=json.dumps(shots),
            )
        )

        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def track_shot_detail_calls(monkeypatch):
    from admin import routes as admin_routes

    calls = []
    original = admin_routes.array_agg_or_group_concat

    def wrapper(column):
        calls.append(column)
        return original(column)

    monkeypatch.setattr(admin_routes, "array_agg_or_group_concat", wrapper)
    return calls


def test_fg3_fg_pct_uses_shot_details(app, track_shot_detail_calls):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1)

    assert track_shot_detail_calls, "shot detail aggregation should run for FG% leaderboards"

    row = next(r for r in rows if r[0] == "Test Player")
    assert row[1] == 2
    assert row[2] == 3
    assert row[3] == pytest.approx(66.6666, rel=1e-3)


def test_points_leaderboard_skips_shot_details(app, track_shot_detail_calls):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("points", season_id=1)

    assert track_shot_detail_calls == []

    row = next(r for r in rows if r[0] == "Test Player")
    assert row[1] == 12
