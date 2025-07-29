import os
import sys
import pytest
from datetime import date
from flask import Flask

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.database import db, Season, Practice, Roster, Possession, PlayerPossession
from models.user import User
from models import recruit, uploaded_file  # ensure related tables exist
from utils.leaderboard_helpers import get_on_court_metrics

@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()

def test_on_court_metrics_filters_by_squad(app):
    with app.app_context():
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        practice = Practice(id=1, season_id=1, date=date(2024,1,2), category='Official')
        db.session.add(practice)
        r1 = Roster(season_id=1, player_name='#1 A')
        r2 = Roster(season_id=1, player_name='#2 B')
        db.session.add_all([r1, r2])
        db.session.commit()

        # Crimson offense with r1 on court
        p1 = Possession(practice_id=1, season_id=1, game_id=0, possession_side='Crimson', points_scored=2)
        db.session.add(p1)
        db.session.flush()
        db.session.add(PlayerPossession(possession_id=p1.id, player_id=r1.id))

        # Additional Crimson possession with r1 on court to establish squad
        p1b = Possession(practice_id=1, season_id=1, game_id=0, possession_side='Crimson', points_scored=2)
        db.session.add(p1b)
        db.session.flush()
        db.session.add(PlayerPossession(possession_id=p1b.id, player_id=r1.id))

        # Crimson offense with r1 off court
        p2 = Possession(practice_id=1, season_id=1, game_id=0, possession_side='Crimson', points_scored=3)
        db.session.add(p2)
        db.session.flush()
        db.session.add(PlayerPossession(possession_id=p2.id, player_id=r2.id))

        # White offense with r1 on court (should be ignored for squad stats)
        p3 = Possession(practice_id=1, season_id=1, game_id=0, possession_side='White', points_scored=1)
        db.session.add(p3)
        db.session.flush()
        db.session.add(PlayerPossession(possession_id=p3.id, player_id=r1.id))

        # White offense with r1 off court (should be ignored)
        p4 = Possession(practice_id=1, season_id=1, game_id=0, possession_side='White', points_scored=2)
        db.session.add(p4)
        db.session.flush()
        db.session.add(PlayerPossession(possession_id=p4.id, player_id=r2.id))
        db.session.commit()

        metrics = get_on_court_metrics(r1.id)
        assert metrics['ppp_on'] == 2.0
