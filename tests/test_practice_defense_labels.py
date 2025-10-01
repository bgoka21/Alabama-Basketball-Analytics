import json
from datetime import date

import pytest
from flask import Flask

from models.database import db, Season, Practice, PlayerStats, Roster
from parse_practice_csv import parse_practice_csv


@pytest.fixture
def app(tmp_path):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def test_defense_labels(app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 3)

    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        db.session.add(Roster(season_id=1, player_name='#1 A'))
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practice')
        db.session.add(practice)
        db.session.commit()

    csv_content = "Row,DRILL TYPE,#1 A\n"
    csv_content += "Crimson,,\"Bump +, Side, Contest\"\n"
    csv_path.write_text(csv_content)

    with app.app_context():
        parse_practice_csv(str(csv_path), season_id=1, category='Official Practice', file_date=practice_date)

        row = PlayerStats.query.filter_by(player_name='#1 A').first()
        assert row.bump_positive == 1
        assert row.contest_side == 1
        assert row.contest_early == 1
        details = json.loads(row.stat_details)
        assert details == [
            {"event": "bump_positive", "drill_labels": []},
            {"event": "contest_side", "drill_labels": []},
            {"event": "contest_early", "drill_labels": []},
        ]


def test_contest_shot_breakdown(app, tmp_path):
    csv_path = tmp_path / "contest_breakdown.csv"
    practice_date = date(2024, 1, 4)

    with app.app_context():
        season = Season(id=2, season_name='2024', start_date=practice_date)
        db.session.add(season)
        for name in ('#1 Off', '#2 Def', '#3 Off', '#4 Def', '#5 Off', '#6 Def'):
            db.session.add(Roster(season_id=2, player_name=name))
        practice = Practice(id=2, season_id=2, date=practice_date, category='Official Practice')
        db.session.add(practice)
        db.session.commit()

    rows = [
        ["Row", "DRILL TYPE", "#1 Off", "#2 Def", "#3 Off", "#4 Def", "#5 Off", "#6 Def"],
        ["Crimson", "", "ATR+", "Contest", "", "", "", ""],
        ["White", "", "", "", "2FG-", "Late", "", ""],
        ["Crimson", "", "", "", "", "", "3FG+", "No Contest"],
    ]
    csv_content = "\n".join(",".join(row) for row in rows) + "\n"
    csv_path.write_text(csv_content)

    with app.app_context():
        parse_practice_csv(
            str(csv_path),
            season_id=2,
            category='Official Practice',
            file_date=practice_date,
        )

        defender_one = PlayerStats.query.filter_by(player_name='#2 Def').first()
        assert defender_one.contest_early == 1
        assert defender_one.atr_contest_attempts == 1
        assert defender_one.atr_contest_makes == 1
        details_one = json.loads(defender_one.stat_details)
        assert details_one == [
            {
                "event": "contest_early",
                "drill_labels": [],
                "contest_level": "contest",
                "shot_class": "atr",
                "shot_result": "made",
            }
        ]

        defender_two = PlayerStats.query.filter_by(player_name='#4 Def').first()
        assert defender_two.contest_late == 1
        assert defender_two.fg2_late_attempts == 1
        assert defender_two.fg2_late_makes == 0
        details_two = json.loads(defender_two.stat_details)
        assert details_two == [
            {
                "event": "contest_late",
                "drill_labels": [],
                "contest_level": "late",
                "shot_class": "2fg",
                "shot_result": "miss",
            }
        ]

        defender_three = PlayerStats.query.filter_by(player_name='#6 Def').first()
        assert defender_three.contest_no == 1
        assert defender_three.fg3_no_contest_attempts == 1
        assert defender_three.fg3_no_contest_makes == 1
        details_three = json.loads(defender_three.stat_details)
        assert details_three == [
            {
                "event": "contest_no",
                "drill_labels": [],
                "contest_level": "no_contest",
                "shot_class": "3fg",
                "shot_result": "made",
            }
        ]
