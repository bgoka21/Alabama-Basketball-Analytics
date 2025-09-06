import os
from pathlib import Path

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from bs4 import BeautifulSoup

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


def test_compare_defaults_to_coach_and_lists_coaches(client, app):
    with app.app_context():
        from app.models.prospect import Prospect
        db.session.add_all([
            Prospect(coach='CoachA', player='A1', year=2024),
            Prospect(coach='CoachB', player='B1', year=2024),
        ])
        db.session.commit()

    rv = client.get('/recruits/compare')
    assert rv.status_code == 200
    soup = BeautifulSoup(rv.data, 'html.parser')
    # default radio checked
    coach_radio = soup.find('input', {'type': 'radio', 'name': 'by', 'value': 'coach'})
    assert coach_radio is not None and coach_radio.has_attr('checked')
    options = [o.get_text(strip=True) for o in soup.select('select[name="entities"] option')]
    assert 'CoachA' in options
    assert 'CoachB' in options


def test_compare_switch_to_team_lists_teams(client, app):
    from app.models.prospect import Prospect
    with app.app_context():
        db.session.add_all([
            Prospect(coach='CoachA', player='P1', team='TeamX', year=2024),
            Prospect(coach='CoachB', player='P2', team='TeamY', year=2024),
            Prospect(coach='CoachC', player='P3', team='TeamX', year=2024),
        ])
        db.session.commit()

    rv = client.get('/recruits/compare?by=team')
    soup = BeautifulSoup(rv.data, 'html.parser')
    options = [o.get_text(strip=True) for o in soup.select('select[name="entities"] option')]
    assert 'TeamX' in options
    assert 'TeamY' in options


def test_compare_aggregates_by_team(client, app):
    from app.models.prospect import Prospect
    with app.app_context():
        db.session.add_all([
            Prospect(coach='CoachA', player='A1', team='TeamX', year=2024,
                     projected_money=100, actual_money=150, net=50),
            Prospect(coach='CoachB', player='B1', team='TeamX', year=2024,
                     projected_money=50, actual_money=70, net=20),
            Prospect(coach='CoachC', player='C1', team='TeamY', year=2024,
                     projected_money=200, actual_money=100, net=-100),
        ])
        db.session.commit()

    rv = client.get('/recruits/compare?by=team&entities=TeamX&entities=TeamY')
    assert rv.status_code == 200
    html = rv.data.decode()
    # TeamX totals: recruits=2, proj=$150, act=$220, net=$70, avg=$35
    assert 'TeamX' in html
    assert '$150' in html
    assert '$220' in html
    assert '$70' in html
    assert '$35' in html
    # TeamY totals: recruits=1, proj=$200, act=$100, net=-$100, avg=-$100
    assert 'TeamY' in html
    assert '$200' in html
    assert '$100' in html
    assert '$-100' in html


def test_compare_accepts_legacy_coaches_param(client, app):
    from app.models.prospect import Prospect
    with app.app_context():
        db.session.add_all([
            Prospect(coach='CoachA', player='A1', team='TeamX', year=2024),
            Prospect(coach='CoachB', player='B1', team='TeamY', year=2024),
        ])
        db.session.commit()

    rv = client.get('/recruits/compare?coaches=CoachA&coaches=CoachB')
    assert rv.status_code == 200
    html = rv.data.decode()
    assert 'CoachA' in html
    assert 'CoachB' in html
