from datetime import date
from pathlib import Path
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster, SkillEntry
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
        entry = SkillEntry(player_id=1, date=date(2024,1,5), skill_name='Free Throws', value=20)
        db.session.add(entry)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client


def test_player_skill_shows_entries(client):
    resp = client.get('/admin/player/%231%20Test/skill')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'Free Throws' in html
    assert '20' in html


def test_player_skill_add_entry(client, app):
    resp = client.post('/admin/player/%231%20Test/skill', data={
        'date':'2024-01-06',
        'skill_name':'Free Throws',
        'value':'5'
    })
    assert resp.status_code == 302
    with app.app_context():
        assert SkillEntry.query.filter_by(skill_name='Free Throws', value=5).first() is not None
