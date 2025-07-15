import re
from datetime import date

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from pathlib import Path
from models.database import db, Season, Practice, PlayerStats, Roster
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
        practice1 = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official')
        practice2 = Practice(id=2, season_id=1, date=date(2024, 1, 5), category='Official')
        db.session.add_all([practice1, practice2])
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#1 Test', points=5))
        db.session.add(PlayerStats(practice_id=2, season_id=1, player_name='#1 Test', points=7))
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def _points_from_html(html):
    m = re.search(r'#1 Test</td>\s*<td class="px-4 py-2">\s*(\d+)', html)
    return int(m.group(1)) if m else 0


def test_leaderboard_date_filter(client):
    resp = client.get('/admin/leaderboard', query_string={'season_id': 1, 'stat': 'points'})
    html = resp.data.decode('utf-8')
    assert _points_from_html(html) == 12

    resp = client.get('/admin/leaderboard', query_string={
        'season_id': 1,
        'stat': 'points',
        'start_date': '2024-01-04'
    })
    html = resp.data.decode('utf-8')
    assert _points_from_html(html) == 7

    resp = client.get('/admin/leaderboard', query_string={
        'season_id': 1,
        'stat': 'points',
        'end_date': '2024-01-03'
    })
    html = resp.data.decode('utf-8')
    assert _points_from_html(html) == 5

