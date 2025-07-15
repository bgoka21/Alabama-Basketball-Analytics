import logging
from datetime import date
from unittest.mock import patch

import pytest
from flask import Flask
import pandas as pd

from models.database import db, Season, Practice, Roster
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


def test_malformed_csv_logs_error_and_returns_message(app, tmp_path, caplog):
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text('Row\n1')
    practice_date = date(2024, 1, 1)

    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=practice_date)
        db.session.add(season)
        db.session.add(Roster(season_id=1, player_name='#1 A'))
        practice = Practice(id=1, season_id=1, date=practice_date, category='Official Practices')
        db.session.add(practice)
        db.session.commit()

        with patch('pandas.read_csv', side_effect=pd.errors.ParserError('bad')):
            caplog.set_level(logging.ERROR)
            result = parse_practice_csv(
                str(csv_path),
                season_id=1,
                category='Official Practices',
                file_date=practice_date,
            )

        assert 'error' in result
        assert 'Unable to parse practice CSV' in result['error']
        assert any('Failed to read practice CSV' in r.getMessage() for r in caplog.records)
