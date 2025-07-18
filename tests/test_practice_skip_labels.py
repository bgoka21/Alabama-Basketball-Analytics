import pytest
from datetime import date
from flask import Flask

from models.database import db, Season, Practice, Roster, Possession
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


def test_skip_off_rebound_and_neutral_rows(app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 7)

    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        db.session.add(Roster(season_id=1, player_name='#1 A'))
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practices')
        db.session.add(practice)
        db.session.commit()

    csv_content = "Row,DRILL TYPE,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,#1 A\n"
    csv_content += "Crimson,,\"#1 A\",,2FG+\n"
    csv_content += "Crimson,Off Rebound,\"#1 A\",,3FG+\n"
    csv_content += "Crimson,Neutral,\"#1 A\",,ATR+\n"
    csv_path.write_text(csv_content)

    with app.app_context():
        parse_practice_csv(str(csv_path), season_id=1, category='Official Practices', file_date=practice_date)
        assert Possession.query.count() == 2
