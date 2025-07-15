from datetime import date
from pathlib import Path
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster, SkillEntry
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
    lm = LoginManager(); lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(public_bp)

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        roster = Roster(id=1, season_id=1, player_name='Test Player')
        db.session.add(roster)
        player = User(username='player', password_hash=generate_password_hash('pw'), is_player=True, player_name='Test Player')
        db.session.add(player)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()

@pytest.fixture
def client(app):
    with app.test_client() as c:
        c.post('/admin/login', data={'username':'player','password':'pw'})
        yield c

def test_player_adds_nba100_entry(client, app):
    resp = client.post('/nba100', data={'date':'2024-01-10','makes':'80'})
    assert resp.status_code == 302
    with app.app_context():
        entry = SkillEntry.query.filter_by(skill_name='NBA 100', value=80).first()
        assert entry is None
