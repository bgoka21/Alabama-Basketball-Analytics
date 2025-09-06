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
from bs4 import BeautifulSoup


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


def test_money_board_has_compare_link(client, app):
    with app.app_context():
        from app.models.prospect import Prospect
        db.session.add(Prospect(coach='CoachX', player='PX', year=2024))
        db.session.commit()

    rv = client.get('/recruits/money?year_min=2020&conf=SEC')
    soup = BeautifulSoup(rv.data, 'html.parser')

    # no coach selector on Money Board
    assert soup.find('select', id='coach-search') is None

    link = soup.find('a', string=lambda s: s and 'Compare Coaches' in s)
    assert link is not None
    href = link['href']
    assert '/recruits/compare' in href
    assert 'year_min=2020' in href
    assert 'conf=SEC' in href


def test_money_board_no_redirect(client, app):
    """Money Board should not redirect when coaches are provided."""
    from app.models.prospect import Prospect

    with app.app_context():
        db.session.add_all([
            Prospect(coach='CoachA', player='A', year=2024),
            Prospect(coach='CoachB', player='B', year=2024),
        ])
        db.session.commit()

    rv = client.get('/recruits/money?coaches=CoachA&coaches=CoachB')
    assert rv.status_code == 200
    soup = BeautifulSoup(rv.data, 'html.parser')
    link = soup.find('a', string=lambda s: s and 'Compare Coaches' in s)
    assert link is not None

