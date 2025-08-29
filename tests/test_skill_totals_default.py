import pytest
from datetime import date
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster
from models.user import User
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

    with app.app_context():
        db.create_all()
        # Current season has lower id to ensure ordering by start_date
        season_current = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        season_old = Season(id=2, season_name='2023', start_date=date(2023, 1, 1))
        db.session.add_all([season_current, season_old])
        db.session.add_all([
            Roster(id=1, season_id=1, player_name='#1 Current'),
            Roster(id=2, season_id=2, player_name='#2 Old'),
        ])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_skill_totals_defaults_to_current_season(client):
    resp = client.get('/admin/skill_totals')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert '#1 Current' in html
    assert '#2 Old' not in html
