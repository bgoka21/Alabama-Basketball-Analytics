from datetime import date
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster, SkillEntry
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
        return User.query.get(int(uid))
    app.register_blueprint(admin_bp, url_prefix='/admin')
    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        entry = SkillEntry(id=1, player_id=1, date=date(2024,1,5), skill_name='NBA 100', value=80)
        db.session.add(entry)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_delete_nba100_entry(client, app):
    resp = client.post('/admin/player/%231%20Test/nba100/1/delete')
    assert resp.status_code == 302
    with app.app_context():
        assert SkillEntry.query.get(1) is None
