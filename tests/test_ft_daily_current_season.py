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


def test_ft_daily_respects_total_since_sort(app):
    start_date = end_date = date(2024, 1, 5)
    since_date = date(2024, 1, 1)

    with app.app_context():
        db.session.add(
            Roster(id=3, season_id=1, player_name='Current Player 2')
        )
        db.session.add_all([
            # Player 2: fewer total shots since the anchor date
            SkillEntry(player_id=2, date=start_date, shot_class='ft', makes=6, attempts=8),
            SkillEntry(player_id=2, date=since_date, shot_class='3pt', makes=4, attempts=10),
            # Player 3: more total shots since the anchor date
            SkillEntry(player_id=3, date=start_date, shot_class='ft', makes=5, attempts=6),
            SkillEntry(player_id=3, date=since_date, shot_class='mid', makes=7, attempts=20),
        ])
        db.session.commit()

        rows, _totals, _has_entries, sort = _ft_daily_data(
            start_date,
            end_date,
            since_date,
            hide_zeros=False,
            sort='total_since',
            dir_='desc',
        )

        assert sort == 'total_since'
        assert rows[0]['player_name'] == 'Current Player 2'
        assert rows[0]['total_shots_since'] > rows[1]['total_shots_since']
