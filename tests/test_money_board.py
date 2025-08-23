import os
from pathlib import Path

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

os.environ.setdefault('SKIP_CREATE_ALL', '1')

from models.database import db
from models.user import User
from admin.routes import admin_bp
from recruits import recruits_bp


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
    app.register_blueprint(recruits_bp, url_prefix='/recruits')
    app.jinja_env.globals['view_exists'] = lambda n: n in app.view_functions

    with app.app_context():
        from app.models.prospect import Prospect  # noqa: F401
        db.create_all()
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


def test_money_board_ok(client):
    rv = client.get('/recruits/money')
    assert rv.status_code == 200
    assert b'Money Board' in rv.data


def test_money_coach_ok(client):
    rv = client.get('/recruits/money/coach/Nate%20Oats')
    assert rv.status_code in (200, 404)


def test_money_compare_interface(client):
    rv = client.get('/recruits/money/compare')
    assert rv.status_code == 200
    assert b'coach-search' in rv.data

    rv2 = client.get('/recruits/coach_list')
    assert rv2.status_code == 200
    assert isinstance(rv2.get_json(), list)

