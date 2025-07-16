from pathlib import Path
from datetime import date
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster, PlayerDevelopmentPlan, PlayerStats
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
    lm = LoginManager(); lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
        plan = PlayerDevelopmentPlan(player_name='#1 Test', season_id=1, stat_1_name='points', stat_1_goal=50)
        db.session.add(plan)
        ps = PlayerStats(player_name='#1 Test', season_id=1, points=10)
        db.session.add(ps)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as c:
        c.post('/admin/login', data={'username':'admin','password':'pw'})
        yield c


def test_player_development_view(client):
    resp = client.get('/admin/player/%231%20Test/development')
    assert resp.status_code == 200
    assert 'Development Plan' in resp.data.decode('utf-8')
