import json
import os
import sys
from datetime import date

import pytest
from flask import Flask

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
from models.database import (
    db,
    Season,
    Practice,
    PlayerStats,
    Roster,
    BlueCollarStats,
)
from parse_practice_csv import parse_practice_csv
from test_parse import parse_csv


@pytest.fixture
def practice_app(tmp_path):
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["TESTING"] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def test_practice_collision_low_man(practice_app, tmp_path):
    csv_path = tmp_path / "practice_collision.csv"
    csv_content = "Row,DRILL TYPE,#1 A\n"
    csv_content += "Crimson,,\"Bump +, Low Man +\"\n"
    csv_content += "Crimson,,\"Bump -, Low Man -\"\n"
    csv_path.write_text(csv_content)

    practice_date = date(2024, 2, 1)

    with practice_app.app_context():
        season = Season(id=1, season_name="2024", start_date=practice_date)
        roster = Roster(season_id=season.id, player_name="#1 A")
        practice = Practice(
            id=1,
            season_id=season.id,
            date=practice_date,
            category="Official Practice",
        )
        db.session.add_all([season, roster, practice])
        db.session.commit()

        parse_practice_csv(
            str(csv_path),
            season_id=season.id,
            category="Official Practice",
            file_date=practice_date,
        )

        row = PlayerStats.query.filter_by(
            player_name="#1 A",
            practice_id=practice.id,
        ).first()
        assert row is not None
        assert row.bump_positive == 1
        assert row.bump_missed == 1
        assert row.low_help_positive == 1
        assert row.low_help_missed == 1
        assert row.collision_gap_positive == 0
        assert row.collision_gap_missed == 0

        details = json.loads(row.stat_details)
        assert details == [
            {"event": "bump_positive", "drill_labels": []},
            {"event": "low_help_positive", "drill_labels": []},
            {"event": "bump_missed", "drill_labels": []},
            {"event": "low_help_missed", "drill_labels": []},
        ]

        blue = BlueCollarStats.query.filter_by(
            practice_id=practice.id,
            player_id=roster.id,
        ).first()
        assert blue is not None
        assert blue.total_blue_collar == 0


def test_game_collision_low_man(tmp_path):
    csv_path = tmp_path / "game_collision.csv"
    csv_content = (
        "Row,PLAYER POSSESSIONS,OPP STATS,POSSESSION START,POSSESSION TYPE,PAINT TOUCHES,SHOT CLOCK,SHOT CLOCK PT,TEAM,#1 A\n"
        "Defense,,,start,Half Court,,24,,Man,\"Bump +, Low Man +\"\n"
        "Defense,,,start2,Half Court,,24,,Man,\"Bump -, Low Man -\"\n"
        "Defense,,,start3,Half Court,,24,,Man,\"Gap +\"\n"
        "Defense,,,start4,Half Court,,24,,Man,\"Gap -\"\n"
        "Offense,\"#1 A\",,start5,Half Court,,24,,Man,2FG+\n"
    )
    csv_path.write_text(csv_content)

    os.makedirs("instance", exist_ok=True)
    db_path = os.path.join("instance", "database.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        db.session.add(Season(id=1, season_name="2024"))
        db.session.commit()

        parse_csv(str(csv_path), game_id=None, season_id=1)

        row = PlayerStats.query.filter_by(player_name="#1 A").first()
        assert row is not None
        # Game collisions only credit "Bump" labels; low-man and gap tags are ignored.
        assert row.bump_positive == 1
        assert row.bump_missed == 1
        assert row.low_help_positive == 0
        assert row.low_help_missed == 0
        assert row.collision_gap_positive == 0
        assert row.collision_gap_missed == 0
