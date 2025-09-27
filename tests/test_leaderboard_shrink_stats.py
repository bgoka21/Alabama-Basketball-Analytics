import json

import pytest
from flask import Flask

from admin.routes import compute_leaderboard
from models.database import db, Season, PlayerStats, Roster


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
        db.session.add(Roster(id=1, season_id=1, player_name="Test Player"))

        shots = [
            {
                "shot_class": "3fg",
                "result": "made",
                "possession_type": "halfcourt",
                "3fg_shrink": "Shrink",
                "Assisted": True,
                "drill_labels": ["Special"],
            },
            {
                "shot_class": "3fg",
                "result": "miss",
                "possession_type": "halfcourt",
                "3fg_shrink": "Shrink",
                "Assisted": False,
                "drill_labels": ["Special"],
            },
            {
                "shot_class": "3fg",
                "result": "made",
                "possession_type": "halfcourt",
                "3fg_shrink": "Non-Shrink",
                "Assisted": False,
                "drill_labels": ["Special"],
            },
            {
                "shot_class": "3fg",
                "result": "miss",
                "possession_type": "halfcourt",
                "3fg_shrink": "Non-Shrink",
                "Assisted": False,
                "drill_labels": ["Other"],
            },
        ]

        db.session.add(
            PlayerStats(
                player_name="Test Player",
                season_id=1,
                practice_id=None,
                game_id=None,
                fg3_attempts=4,
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


def test_leaderboard_shrink_percentages_use_fg_pct(app):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1)

    row = next(r for r in rows if r[0] == "Test Player")

    assert row[1] == 2
    assert row[2] == 4
    assert row[3] == pytest.approx(50.0)
    assert row[5] == 1  # shrink makes
    assert row[6] == 2  # shrink attempts
    assert row[7] == pytest.approx(50.0)
    assert row[8] == 1  # non-shrink makes
    assert row[9] == 2  # non-shrink attempts
    assert row[10] == pytest.approx(50.0)


def test_leaderboard_label_filtered_shrink_stats(app):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1, label_set={"SPECIAL"})

    row = next(r for r in rows if r[0] == "Test Player")

    assert row[1] == 2
    assert row[2] == 3
    assert row[3] == pytest.approx(66.666, rel=1e-3)
    assert row[5] == 1
    assert row[6] == 2
    assert row[7] == pytest.approx(50.0)
    assert row[8] == 1
    assert row[9] == 1
    assert row[10] == pytest.approx(100.0)
