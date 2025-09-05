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
    assert '/recruits/money/compare' in href
    assert 'year_min=2020' in href
    assert 'conf=SEC' in href


def test_money_compare_interface(client):
    rv = client.get('/recruits/money/compare')
    assert rv.status_code == 200
    soup = BeautifulSoup(rv.data, 'html.parser')
    sel = soup.find('select', id='coach-search')
    assert sel is not None
    assert sel.has_attr('multiple')



def test_money_compare_has_search_and_badge_container(client):
    rv = client.get('/recruits/money/compare')
    soup = BeautifulSoup(rv.data, 'html.parser')
    assert soup.find('input', id='coach-filter') is not None
    assert soup.find(id='coach-selected') is not None


def test_money_compare_lists_coaches_from_table(client, app):
    """All coaches in the database appear in the picker."""
    with app.app_context():
        from app.models.coach import Coach
        db.session.add_all([Coach(name='CoachA'), Coach(name='CoachB')])
        db.session.commit()

    rv = client.get('/recruits/money/compare')
    soup = BeautifulSoup(rv.data, 'html.parser')
    options = [o.get_text(strip=True) for o in soup.select('#coach-search option')]
    assert 'CoachA' in options
    assert 'CoachB' in options


def test_money_compare_limit(client, app):
    with app.app_context():
        from app.models.prospect import Prospect
        db.session.add_all([Prospect(coach=f'c{i}', player=f'p{i}', year=2024) for i in range(11)])
        db.session.commit()

    qs = '&'.join(f'coaches=c{i}' for i in range(11))
    rv = client.get(f'/recruits/money/compare?{qs}')
    assert rv.status_code == 200
    html = rv.data.decode()
    soup = BeautifulSoup(html, 'html.parser')
    selected_opts = soup.select('select[name="coaches"] option[selected]')
    count = len(selected_opts)
    assert count == 10
    assert 'up to 10 coaches' in soup.get_text()


def test_money_compare_aggregates(client, app):
    """Aggregated values for each selected coach are rendered."""
    from app.models.prospect import Prospect

    with app.app_context():
        db.session.add_all([
            Prospect(coach='CoachA', player='A1', year=2024,
                     projected_money=100, actual_money=150, net=50),
            Prospect(coach='CoachA', player='A2', year=2024,
                     projected_money=200, actual_money=250, net=50),
            Prospect(coach='CoachB', player='B1', year=2024,
                     projected_money=80, actual_money=50, net=-30),
        ])
        db.session.commit()

    rv = client.get('/recruits/money/compare?coaches=CoachA&coaches=CoachB')
    assert rv.status_code == 200
    html = rv.data.decode()

    # CoachA totals: recruits=2, proj=$300, act=$400, net=$100, avg=$50
    assert 'CoachA' in html
    assert '$300' in html
    assert '$400' in html
    assert '$100' in html

    # CoachB totals: recruits=1, proj=$80, act=$50, net=-$30, avg=-$30
    assert 'CoachB' in html
    assert '$80' in html
    assert '-$30' in html

    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('div.p-4.rounded-xl.border')
    assert len(cards) == 2

    # Coaches should be listed in descending order by net money
    names = [c.find('div', class_='font-semibold').get_text(strip=True)
             for c in cards]
    assert names == ['CoachA', 'CoachB']


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

