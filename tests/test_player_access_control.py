import pytest
from datetime import date
from pathlib import Path
from flask import Flask, request, redirect, url_for
from flask_login import LoginManager, current_user
from werkzeug.security import generate_password_hash

from models.database import db, Season, Roster
from models.user import User
from admin.routes import admin_bp
from public.routes import public_bp
from recruits import recruits_bp
from utils.auth import PLAYER_ALLOWED_ENDPOINTS


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
    app.register_blueprint(recruits_bp, url_prefix='/recruits')

    @app.route('/player/<player_name>')
    def player_view(player_name):
        return ''

    @app.before_request
    def restrict_player():
        if request.endpoint in ('static', None):
            return
        if current_user.is_authenticated and current_user.is_player:
            if request.endpoint not in PLAYER_ALLOWED_ENDPOINTS:
                target = (
                    url_for('player_view', player_name=current_user.player_name)
                    if current_user.player_name else url_for('public.homepage')
                )
                return redirect(target)

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        roster = Roster(id=1, season_id=1, player_name='Test Player')
        db.session.add_all([season, roster])
        player = User(username='player', password_hash=generate_password_hash('pw'), is_player=True, player_name='Test Player')
        db.session.add(player)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as c:
        c.post('/admin/login', data={'username': 'player', 'password': 'pw'})
        yield c


def test_recruit_routes_blocked(client):
    resp = client.get('/recruits/')
    assert resp.status_code == 302


def test_other_route_blocked(client):
    resp = client.get('/admin/dashboard')
    assert resp.status_code == 302
