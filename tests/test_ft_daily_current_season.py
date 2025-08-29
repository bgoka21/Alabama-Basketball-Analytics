import pytest
from datetime import date
from flask import Flask

from models.database import db, Season, Roster, SkillEntry
from admin.routes import _ft_daily_data


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
        # Older season has higher id but earlier start_date to ensure ordering by start_date
        s_old = Season(id=2, season_name='2023', start_date=date(2023, 1, 1))
        s_current = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add_all([s_old, s_current])
        db.session.add_all([
            Roster(id=1, season_id=2, player_name='Old Player'),
            Roster(id=2, season_id=1, player_name='Current Player'),
        ])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


def test_ft_daily_uses_current_season(app):
    selected_date = date(2024, 1, 5)
    with app.app_context():
        # Add free-throw entries for both seasons
        db.session.add_all([
            SkillEntry(player_id=1, date=selected_date, shot_class='ft', makes=5, attempts=5),
            SkillEntry(player_id=2, date=selected_date, shot_class='ft', makes=7, attempts=8),
        ])
        db.session.commit()

        rows, _totals, _has_entries, _sort = _ft_daily_data(
            selected_date,
            include_total=False,
            hide_zeros=False,
            sort='attempts',
            dir_='desc'
        )

        names = [r['player_name'] for r in rows]
        assert 'Current Player' in names
        assert 'Old Player' not in names
