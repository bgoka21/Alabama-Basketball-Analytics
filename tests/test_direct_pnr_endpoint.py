import json
from datetime import date

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster, PnRStats
from models.user import User
from public.routes import public_bp
from admin.routes import admin_bp


@pytest.fixture
def app():
    app = Flask(__name__)
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
    app.register_blueprint(public_bp)

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        roster = Roster(id=1, season_id=1, player_name='#1 A')
        db.session.add(roster)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        db.session.flush()
        # sample PnR stats
        db.session.add_all([
            PnRStats(game_id=None, possession_id=1, player_id=1, role='BH', advantage_created='Adv+',
                     direct=True, points_scored=2, turnover_occurred=False, assist_occurred=True,
                     start_time=0.0, duration=5.0),
            PnRStats(game_id=None, possession_id=2, player_id=1, role='BH', advantage_created='Adv-',
                     direct=False, points_scored=None, turnover_occurred=False, assist_occurred=False,
                     start_time=10.0, duration=4.0),
            PnRStats(game_id=None, possession_id=3, player_id=1, role='Screener', advantage_created='Adv+',
                     direct=True, points_scored=3, turnover_occurred=False, assist_occurred=False,
                     start_time=20.0, duration=6.0)
        ])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_direct_pnr_endpoint(client):
    resp = client.get('/api/direct_pnr_for_player/1')
    assert resp.status_code == 200
    data = json.loads(resp.data.decode())
    assert data['total_pnrs'] == 3
    assert data['pnrs_as_bh'] == 2
    assert data['pnrs_as_screener'] == 1
    assert round(data['pct_adv_plus'], 2) == 0.67
    assert data['direct_pnr_points_per'] == 2.5
    assert data['direct_pnr_turnovers'] == 0
    assert data['direct_pnr_assists'] == 1
