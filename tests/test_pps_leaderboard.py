import pytest
from datetime import date
from flask import Flask
from pathlib import Path
from flask_login import LoginManager
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
        roster_a = Roster(id=1, season_id=1, player_name='#1 A')
        roster_b = Roster(id=2, season_id=1, player_name='#2 B')
        db.session.add_all([roster_a, roster_b])
        practice = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official')
        db.session.add(practice)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        # stats for player A (higher PPS)
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#1 A',
                                  atr_makes=2, atr_attempts=4,
                                  fg2_makes=3, fg2_attempts=5,
                                  fg3_makes=1, fg3_attempts=2))
        # stats for player B (lower PPS)
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#2 B',
                                  atr_makes=0, atr_attempts=4,
                                  fg2_makes=1, fg2_attempts=6,
                                  fg3_makes=0, fg3_attempts=2))
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_pps_leaderboard_order(client):
    resp = client.get('/practice_home')
    assert resp.status_code == 200
    html = resp.data.decode()
    assert 'PPS Leaders' in html
    start = html.index('PPS Leaders')
    snippet = html[start:]
    assert snippet.index('#1 A') < snippet.index('#2 B')
