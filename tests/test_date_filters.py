import json
from datetime import date
import re
import pytest
from flask import Flask
from flask_login import LoginManager
from pathlib import Path
from werkzeug.security import generate_password_hash

from models.database import db, Season, Practice, PlayerStats, Roster
from models.user import User
from public.routes import public_bp
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
        return User.query.get(int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(public_bp)

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        practice1 = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official Practices')
        practice2 = Practice(id=2, season_id=1, date=date(2024, 1, 5), category='Official Practices')
        db.session.add_all([practice1, practice2])
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        shots = [
            {"shot_class": "2fg", "result": "made", "2fg_type": "Dunk", "drill_labels": []}
        ]
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#1 Test',
                                  atr_makes=1, atr_attempts=1,
                                  shot_type_details=json.dumps(shots)))
        db.session.add(PlayerStats(practice_id=2, season_id=1, player_name='#1 Test',
                                  atr_makes=3, atr_attempts=4,
                                  shot_type_details=json.dumps(shots)))
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def _dunk_count(html):
    start = html.index('Dunks Get You Paid')
    snippet = html[start:]
    row_start = snippet.index('#1 Test')
    row_end = snippet.index('</tr>', row_start)
    row = snippet[row_start:row_end]
    if '>2<' in row:
        return 2
    if '>1<' in row:
        return 1
    return 0


def _atr_ratio(html):
    start = html.index('ATR')
    snippet = html[start:]
    m = re.search(r'(\d+)/(\d+)', snippet)
    return m.group(0) if m else ''


def test_practice_home_start_date_filter(client):
    resp = client.get('/practice_home')
    html = resp.data.decode('utf-8')
    assert _dunk_count(html) == 2

    resp = client.get('/practice_home', query_string={'start_date': '2024-01-04'})
    html = resp.data.decode('utf-8')
    assert _dunk_count(html) == 1


def test_player_detail_start_date_filter(client):
    resp = client.get('/admin/player/%231%20Test', query_string={'mode': 'practice'})
    html = resp.data.decode('utf-8')
    assert '4/5' in _atr_ratio(html)

    resp = client.get('/admin/player/%231%20Test', query_string={'mode': 'practice', 'start_date': '2024-01-04'})
    html = resp.data.decode('utf-8')
    assert '3/4' in _atr_ratio(html)

