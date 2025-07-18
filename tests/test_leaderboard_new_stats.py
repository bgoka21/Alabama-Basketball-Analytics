import pytest
from datetime import date
from pathlib import Path
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
import json

from models.database import (
    db, Season, Practice, PlayerStats, Roster,
    Possession, PlayerPossession, ShotDetail
)
from models.user import User
from admin.routes import admin_bp


@pytest.fixture
def app():
    template_root = Path(__file__).resolve().parents[1] / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    db.init_app(app)

    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        practice = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official')
        db.session.add(practice)
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        other = Roster(id=2, season_id=1, player_name='#2 Other')
        db.session.add_all([roster, other])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        shots = [
            {"shot_class": "atr", "result": "made", "possession_type": "total"},
            {"shot_class": "atr", "result": "miss", "possession_type": "total"},
            {"shot_class": "2fg", "result": "made", "possession_type": "total"},
            {"shot_class": "2fg", "result": "made", "possession_type": "total"},
            {"shot_class": "3fg", "result": "made", "possession_type": "total"},
        ]
        db.session.add(PlayerStats(
            practice_id=1,
            season_id=1,
            player_name='#1 Test',
            atr_attempts=2,
            atr_makes=1,
            fg2_attempts=2,
            fg2_makes=2,
            fg3_attempts=1,
            fg3_makes=1,
            shot_type_details=json.dumps(shots),
        ))

        poss1 = Possession(id=1, practice_id=1, season_id=1, game_id=0,
                           possession_side='Offense', points_scored=3)
        db.session.add(poss1)
        db.session.add(PlayerPossession(possession_id=1, player_id=1))
        db.session.add(ShotDetail(possession_id=1, event_type='3FG+'))
        db.session.add(ShotDetail(possession_id=1, event_type='Off Rebound'))

        poss2 = Possession(id=2, practice_id=1, season_id=1, game_id=0,
                           possession_side='Offense', points_scored=2)
        db.session.add(poss2)
        db.session.add(PlayerPossession(possession_id=2, player_id=2))
        db.session.add(ShotDetail(possession_id=2, event_type='2FG+'))

        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_leaderboard_shows_frequency_stats(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'atr_freq_pct'})
    html = resp.data.decode('utf-8')
    assert 'ATR Frequency' in html
    assert '40.0%' in html

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'fg3_freq_pct'})
    html = resp.data.decode('utf-8')
    assert '3FG Frequency' in html
    assert '20.0%' in html


def test_fg_pct_table_includes_frequency(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'atr_fg_pct'})
    html = resp.data.decode('utf-8')
    assert 'Freq' in html
    assert '40.0%' in html


def test_assist_summary_table(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'assist_summary'})
    html = resp.data.decode('utf-8')
    assert 'Assist/TO Stats' in html
    assert 'Adj AST/TO' in html


def test_on_court_offensive_metrics(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'ppp_on'})
    html = resp.data.decode('utf-8')
    assert 'PPP On' in html
    assert '3.0' in html

    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'efg_on'})
    html = resp.data.decode('utf-8')
    assert 'EFG%' in html
    assert '150.0%' in html


def test_offense_summary_table(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'offense_summary'})
    html = resp.data.decode('utf-8')
    assert 'Offense Stats' in html
    assert 'PPP On' in html
