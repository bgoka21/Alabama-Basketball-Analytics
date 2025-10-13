from datetime import date

import pytest
from flask import Flask

from models.database import db, Season, Practice, Roster
from parse_practice_csv import parse_practice_csv
from services.reports.advanced_possession import compute_advanced_possession_practice


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


def test_practice_report_uses_team_possessions(app, tmp_path):
    csv_path = tmp_path / "practice.csv"
    practice_date = date(2024, 1, 15)

    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        db.session.add_all(
            [
                Roster(season_id=1, player_name='#1 A'),
                Roster(season_id=1, player_name='#2 B'),
            ]
        )
        practice = Practice(
            id=1,
            season_id=1,
            date=practice_date,
            category='Official Practice',
        )
        db.session.add(practice)
        db.session.commit()

    csv_content = (
        "Row,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,#1 A,#2 B\n"
        "Crimson,\"#1 A\",,2FG+,\n"
        "White,,\"#2 B\",,3FG+\n"
    )
    csv_path.write_text(csv_content)

    with app.app_context():
        parse_practice_csv(
            str(csv_path),
            season_id=1,
            category='Official Practice',
            file_date=practice_date,
        )

        payload = compute_advanced_possession_practice(practice_id=1)
        crimson_meta = payload['crimson']['meta']
        white_meta = payload['white']['meta']

        assert crimson_meta['total_pts'] == 2
        assert white_meta['total_pts'] == 3
        assert crimson_meta != white_meta
