import os
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


def test_win_loss_parsing(app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 1)

    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        db.session.add(Roster(season_id=1, player_name='#1 A'))
        db.session.add(Roster(season_id=1, player_name='#2 B'))
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practices')
        db.session.add(practice)
        db.session.commit()

    csv_content = "Row,CRIMSON,WHITE,ALABAMA,BLUE\n"
    csv_content += "Win / Loss,\"Win, #1 A\",\"Loss, #2 B\",,\n"
    csv_path.write_text(csv_content)

    with app.app_context():
        parse_practice_csv(str(csv_path), season_id=1, category='Official Practices', file_date=practice_date)

        win_row = PlayerStats.query.filter_by(player_name='#1 A').first()
        loss_row = PlayerStats.query.filter_by(player_name='#2 B').first()
        assert win_row.practice_wins == 1
        assert loss_row.practice_losses == 1

