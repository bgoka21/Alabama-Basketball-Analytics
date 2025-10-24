import os
import json

import pytest
from datetime import date
from flask import Flask

from models.database import (
    db,
    Season,
    Practice,
    Roster,
    Possession,
    PlayerPossession,
    PlayerStats,
    ShotDetail,
    Game,
)
from parse_practice_csv import parse_practice_csv
from test_parse import parse_csv
from app import create_app

@pytest.fixture
def practice_app(tmp_path):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def test_practice_shot_details(practice_app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 10)

    with practice_app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        r1 = Roster(season_id=1, player_name='#1 A')
        r2 = Roster(season_id=1, player_name='#2 B')
        db.session.add_all([r1, r2])
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practice')
        db.session.add(practice)
        db.session.commit()

    csv_content = "Row,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,#1 A,#2 B\n"
    csv_content += "Crimson,\"#1 A\",\"#2 B\",2FG+,Turnover\n"
    csv_path.write_text(csv_content)

    with practice_app.app_context():
        parse_practice_csv(str(csv_path), season_id=1, category='Official Practice', file_date=practice_date)
        off_poss = Possession.query.filter_by(time_segment='Offense').first()
        def_poss = Possession.query.filter_by(time_segment='Defense').first()
        off_events = [d.event_type for d in ShotDetail.query.filter_by(possession_id=off_poss.id)]
        def_events = [d.event_type for d in ShotDetail.query.filter_by(possession_id=def_poss.id)]
        assert '2FG+' in off_events
        assert 'Turnover' in def_events


def test_practice_shot_details_include_balance_column(practice_app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 10)

    with practice_app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        roster = Roster(season_id=1, player_name='#1 A')
        db.session.add(roster)
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practice')
        db.session.add(practice)
        db.session.commit()

    csv_content = (
        "Row,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,POSSESSION TYPE,Shot Location,#1 A,#2 B,3FG (Balance)\n"
        "Crimson,\"#1 A\",,Halfcourt,Arc,3FG+, ,On Balance\n"
    )
    csv_path.write_text(csv_content)

    with practice_app.app_context():
        parse_practice_csv(str(csv_path), season_id=1, category='Official Practice', file_date=practice_date)
        stats = PlayerStats.query.filter_by(player_name='#1 A').first()
        assert stats is not None
        details = json.loads(stats.shot_type_details)
        assert details[0]['3fg_balance'] == 'On Balance'


def test_game_shot_details(tmp_path):
    csv_path = tmp_path / "game.csv"
    csv_content = "Row,PLAYER POSSESSIONS,OPP STATS,POSSESSION START,POSSESSION TYPE,PAINT TOUCHES,SHOT CLOCK,SHOT CLOCK PT,TEAM,#1 A\n"
    csv_content += "Offense,\"#1 A\",,start,HC,,24,, ,2FG+\n"
    csv_content += "Defense,,3FG+,start2,HC,,24,, ,\n"
    csv_path.write_text(csv_content)

    db_path = os.path.join('instance', 'database.db')
    if os.path.exists(db_path):
        os.remove(db_path)

    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        db.session.add(Season(id=1, season_name='2024'))
        db.session.commit()
        parse_csv(str(csv_path), game_id=None, season_id=1)
        details = ShotDetail.query.all()
        events = {d.event_type for d in details}
        assert {'2FG+', '3FG+'} <= events

