from datetime import date
from pathlib import Path
import pytest
from flask import Flask, Blueprint
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
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    public_bp = Blueprint('public', __name__)

    @public_bp.route('/')
    def root():
        return 'ok'

    app.register_blueprint(public_bp)

    @app.route('/player/<player_name>')
    def player_view(player_name):
        return ''

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        roster1 = Roster(id=1, season_id=1, player_name='#1 Test')
        roster2 = Roster(id=2, season_id=1, player_name='#2 Other')
        db.session.add_all([roster1, roster2])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        player1 = User(username='player1', password_hash=generate_password_hash('pw'), is_player=True, player_name='#1 Test')
        player2 = User(username='player2', password_hash=generate_password_hash('pw'), is_player=True, player_name='#2 Other')
        db.session.add_all([admin, player1, player2])
        entry = SkillEntry(id=1, player_id=1, date=date(2024, 1, 5),
                           skill_name='Free Throws', shot_class='ft', subcategory='Free Throws',
                           makes=1, attempts=2)
        db.session.add(entry)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def player1_client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'player1', 'password': 'pw'})
        yield client


@pytest.fixture
def player2_client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'player2', 'password': 'pw'})
        yield client


def test_player_can_edit_own_entry(player1_client, app):
    resp = player1_client.post('/admin/admin/player/%231%20Test/skill-entry/2024-01-05/edit', data={
        'ft_Free_Throws_makes': '4',
        'ft_Free_Throws_attempts': '5'
    })
    assert resp.status_code == 302
    with app.app_context():
        e = db.session.get(SkillEntry, 1)
        assert e.makes == 4
        assert e.attempts == 5


def test_player_can_delete_own_entry(player1_client, app):
    resp = player1_client.post('/admin/admin/player/%231%20Test/skill-entry/2024-01-05/delete')
    assert resp.status_code == 302
    with app.app_context():
        assert SkillEntry.query.filter_by(player_id=1).count() == 0


def test_edit_entry_unauthorized(player2_client):
    resp = player2_client.post('/admin/admin/player/%231%20Test/skill-entry/2024-01-05/edit', data={
        'ft_Free_Throws_makes': '2',
        'ft_Free_Throws_attempts': '3'
    })
    assert resp.status_code == 403


def test_delete_entry_unauthorized(player2_client):
    resp = player2_client.post('/admin/admin/player/%231%20Test/skill-entry/2024-01-05/delete')
    assert resp.status_code == 403
