import json

import pytest
from flask import Flask

from admin.routes import compute_leaderboard
from admin._leaderboard_helpers import build_leaderboard_table
from markupsafe import Markup
from models.database import db, Season, PlayerStats, Roster
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
            {
                "shot_class": "3fg",
                "result": "miss",
                "possession_type": "halfcourt",
                "Assisted": True,
                "drill_labels": ["Special"],
            },
        ]

        player_stat = PlayerStats(
            player_name="Test Player",
            season_id=1,
            practice_id=None,
            game_id=None,
            fg3_attempts=5,
            fg3_makes=2,
            atr_attempts=0,
            atr_makes=0,
            fg2_attempts=0,
            fg2_makes=0,
            shot_type_details=json.dumps(shots),
        )
        db.session.add(player_stat)
        persist_player_shot_details(player_stat, shots, replace=True)

        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


def test_leaderboard_shrink_percentages_use_fg_pct(app):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1)

    row = next(r for r in rows if r[0] == "Test Player")

    assert row[1] == 2
    assert row[2] == 5
    assert row[3] == pytest.approx(40.0)
    assert row[5] == 1  # shrink makes
    assert row[6] == 2  # shrink attempts
    assert row[7] == pytest.approx(50.0)
    assert row[8] == pytest.approx(40.0)  # shrink freq of total 3FG
    assert row[9] == 1  # non-shrink makes
    assert row[10] == 2  # non-shrink attempts
    assert row[11] == pytest.approx(50.0)
    assert row[12] == pytest.approx(40.0)


def test_leaderboard_label_filtered_shrink_stats(app):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1, label_set={"SPECIAL"})

    row = next(r for r in rows if r[0] == "Test Player")

    assert row[1] == 2
    assert row[2] == 4
    assert row[3] == pytest.approx(50.0)
    assert row[5] == 1
    assert row[6] == 2
    assert row[7] == pytest.approx(50.0)
    assert row[8] == pytest.approx(50.0)
    assert row[9] == 1
    assert row[10] == 1
    assert row[11] == pytest.approx(100.0)
    assert row[12] == pytest.approx(25.0)


def test_unlabeled_three_is_excluded_from_non_shrink_totals(app):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1)

    row = next(r for r in rows if r[0] == "Test Player")

    assert row[2] == 5  # total attempts include unlabeled 3FG
    assert row[10] == 2  # only explicitly tagged Non-Shrink attempts are counted


def test_build_leaderboard_table_shrink_cells_are_styled(app):
    with app.app_context():
        cfg, rows, totals = compute_leaderboard("fg3_fg_pct", season_id=1)
        if totals is None:
            total_makes = sum(r[1] for r in rows)
            total_attempts = sum(r[2] for r in rows)
            shrink_makes = sum(r[5] for r in rows)
            shrink_attempts = sum(r[6] for r in rows)
            non_shrink_makes = sum(r[9] for r in rows)
            non_shrink_attempts = sum(r[10] for r in rows)

            def pct(makes: float, attempts: float) -> float:
                return (makes / attempts * 100.0) if attempts else 0.0

            totals = {
                "fg3_fg_pct": pct(total_makes, total_attempts),
                "fg3_makes": total_makes,
                "fg3_attempts": total_attempts,
                "fg3_freq_pct": sum(r[4] for r in rows),
                "fg3_shrink_makes": shrink_makes,
                "fg3_shrink_att": shrink_attempts,
                "fg3_shrink_pct": pct(shrink_makes, shrink_attempts),
                "fg3_shrink_freq_pct": sum(r[8] for r in rows),
                "fg3_nonshrink_makes": non_shrink_makes,
                "fg3_nonshrink_att": non_shrink_attempts,
                "fg3_nonshrink_pct": pct(non_shrink_makes, non_shrink_attempts),
                "fg3_nonshrink_freq_pct": sum(r[12] for r in rows),
            }

        table = build_leaderboard_table(config=cfg, rows=rows, team_totals=totals)

    player_row = next(entry for entry in table["rows"] if entry["player"] == "Test Player")

    for slug in ("shrink_3fg_pct", "non_shrink_3fg_pct"):
        cell = player_row[slug]
        assert isinstance(cell, Markup)
        rendered = str(cell)
        assert "percent-box" in rendered
        assert "grade-token" in rendered
        assert "shrink-3fg--" in rendered

    totals_entry = table["totals"]
    assert totals_entry is not None
    totals_cell = totals_entry["shrink_3fg_pct"]
    assert isinstance(totals_cell, Markup)
    totals_rendered = str(totals_cell)
    assert "percent-box" in totals_rendered
    assert "grade-token" in totals_rendered
    assert "shrink-3fg--" in totals_rendered
