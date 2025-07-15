import pytest
from datetime import date
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
        return db.session.get(User, int(uid))
    app.register_blueprint(admin_bp, url_prefix='/admin')
    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
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


def _add_entry(app, shot_date, attempts):
    with app.app_context():
        db.session.add(SkillEntry(player_id=1, date=shot_date, skill_name='Free Throws', value=attempts,
                                  shot_class='ft', subcategory='Free Throw', makes=attempts, attempts=attempts))
        db.session.commit()


def test_date_filter_applied(client, app):
    _add_entry(app, date(2024, 1, 1), 10)
    _add_entry(app, date(2024, 1, 5), 5)

    resp = client.get('/admin/skill_totals', query_string={'start_date': '2024-01-04'})
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert '5' in html
    assert '10' not in html
