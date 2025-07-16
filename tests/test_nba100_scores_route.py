import pytest
from pathlib import Path
from datetime import date
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
        r1 = Roster(id=1, season_id=1, player_name='#1 A')
        r2 = Roster(id=2, season_id=1, player_name='#2 B')
        db.session.add_all([r1, r2])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        db.session.add_all([
            SkillEntry(player_id=1, date=date(2024,1,5), skill_name='NBA 100', value=80),
            SkillEntry(player_id=2, date=date(2024,1,5), skill_name='NBA 100', value=75),
            SkillEntry(player_id=1, date=date(2024,1,6), skill_name='NBA 100', value=90),
            SkillEntry(player_id=2, date=date(2024,1,6), skill_name='NBA 100', value=70),
        ])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as c:
        c.post('/admin/login', data={'username':'admin','password':'pw'})
        yield c


def test_nba100_scores_route(client):
    resp = client.get('/admin/nba100_scores', query_string={'date':'2024-01-05'})
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert '#1 A' in html
    assert '80' in html


def test_nba100_best_scores(client):
    resp = client.get('/admin/nba100_scores', query_string={'best':'1'})
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert '#1 A' in html and '90' in html
    assert '#2 B' in html and '75' in html
    assert 'Jan 06' in html and 'Jan 05' in html

