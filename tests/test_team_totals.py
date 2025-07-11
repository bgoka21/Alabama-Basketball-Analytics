import pytest
from datetime import date
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

import json
import re

from models.database import db, Season, Practice, PlayerStats, BlueCollarStats, Roster, Possession
from models.user import User
from admin.routes import admin_bp
from public.routes import public_bp


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
        return User.query.get(int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(public_bp)
    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        r1 = Roster(id=1, season_id=1, player_name='#1 A')
        r2 = Roster(id=2, season_id=1, player_name='#2 B')
        db.session.add_all([r1, r2])

        practice1 = Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Official')
        practice2 = Practice(id=2, season_id=1, date=date(2024, 1, 5), category='Pickup')
        db.session.add_all([practice1, practice2])

        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)

        # Stats on 2024-01-02
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#1 A',
                                  atr_makes=2, atr_attempts=4,
                                  fg2_makes=3, fg2_attempts=6,
                                  fg3_makes=1, fg3_attempts=2,
                                  ftm=1, fta=2, points=14,
                                  assists=5, turnovers=3,
                                  second_assists=1, pot_assists=2))
        db.session.add(PlayerStats(practice_id=1, season_id=1, player_name='#2 B',
                                  atr_makes=1, atr_attempts=2,
                                  fg2_makes=2, fg2_attempts=4,
                                  fg3_makes=0, fg3_attempts=1,
                                  ftm=2, fta=2, points=8,
                                  assists=3, turnovers=1,
                                  second_assists=0, pot_assists=1))

        # Stats on 2024-01-05
        db.session.add(PlayerStats(practice_id=2, season_id=1, player_name='#1 A',
                                  atr_makes=1, atr_attempts=2,
                                  fg2_makes=1, fg2_attempts=2,
                                  fg3_makes=1, fg3_attempts=3,
                                  ftm=2, fta=2, points=10,
                                  assists=2, turnovers=1,
                                  second_assists=1, pot_assists=1))
        db.session.add(PlayerStats(practice_id=2, season_id=1, player_name='#2 B',
                                  atr_makes=0, atr_attempts=1,
                                  fg2_makes=2, fg2_attempts=3,
                                  fg3_makes=1, fg3_attempts=2,
                                  ftm=1, fta=1, points=7,
                                  assists=1, turnovers=2,
                                  second_assists=0, pot_assists=1))

        db.session.add(BlueCollarStats(season_id=1, player_id=1,
                                       practice_id=1,
                                       def_reb=1, off_reb=1, misc=0,
                                       deflection=1, steal=1, block=0,
                                       floor_dive=0, charge_taken=1,
                                       reb_tip=0, total_blue_collar=4))
        db.session.add(BlueCollarStats(season_id=1, player_id=2,
                                       practice_id=1,
                                       def_reb=2, off_reb=0, misc=0,
                                       deflection=0, steal=0, block=1,
                                       floor_dive=0, charge_taken=0,
                                       reb_tip=0, total_blue_collar=3))
        db.session.add_all([
            Possession(practice_id=1, season_id=1, game_id=0, paint_touches='0', points_scored=2),
            Possession(practice_id=1, season_id=1, game_id=0, paint_touches='1', points_scored=1),
            Possession(practice_id=1, season_id=1, game_id=0, paint_touches='3', points_scored=3),
        ])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_team_totals_aggregate(client):
    resp = client.get('/admin/team_totals', query_string={'season_id': 1})
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert '39' in html  # points total
    assert '51.6' in html  # efg percent


def test_team_totals_date_filters(client):
    resp = client.get('/admin/team_totals', query_string={
        'season_id': 1,
        'start_date': '2024-01-04'
    })
    html = resp.data.decode('utf-8')
    assert '17' in html
    assert '53.8' in html

    resp = client.get('/admin/team_totals', query_string={
        'season_id': 1,
        'end_date': '2024-01-03'
    })
    html = resp.data.decode('utf-8')
    assert '22' in html
    assert '50.0' in html


def test_team_totals_shot_type_tab(client):
    resp = client.get('/admin/team_totals', query_string={'season_id': 1})
    html = resp.data.decode('utf-8')
    assert 'Shot Type Overview' in html
    assert 'ATR Season' in html
    assert '4/9' in html
    assert '53.3%' in html


def test_team_totals_access_for_players(app):
    with app.test_client() as c:
        user = User(username='player', password_hash=generate_password_hash('pw'), is_admin=False, is_player=True)
        with app.app_context():
            db.session.add(user)
            db.session.commit()
        c.post('/admin/login', data={'username': 'player', 'password': 'pw'})
        resp = c.get('/admin/team_totals', query_string={'season_id': 1})
        assert resp.status_code == 200


def test_team_totals_access_for_staff(app):
    """Non-admin, non-player users should access team totals."""
    with app.test_client() as c:
        user = User(
            username='staff',
            password_hash=generate_password_hash('pw'),
            is_admin=False,
            is_player=False,
        )
        with app.app_context():
            db.session.add(user)
            db.session.commit()
        c.post('/admin/login', data={'username': 'staff', 'password': 'pw'})
        resp = c.get('/admin/team_totals', query_string={'season_id': 1})
        assert resp.status_code == 200


def test_team_totals_trend_multiple_stats(client):
    resp = client.get(
        '/admin/team_totals',
        query_string=[('season_id', 1), ('trend_stat', 'points'), ('trend_stat', 'assists')]
    )
    html = resp.data.decode('utf-8')
    assert 'const trendStats = ["points", "assists"]' in html


def test_team_totals_trend_blue_collar_sum(client):
    resp = client.get(
        '/admin/team_totals',
        query_string={'season_id': 1, 'trend_stat': 'total_blue_collar'}
    )
    html = resp.data.decode('utf-8')
    m = re.search(r"const trendData = (.*?);", html, re.S)
    assert m, 'trendData not found'
    trend_data = json.loads(m.group(1))
    assert trend_data == [
        {'date': '2024-01-02', 'total_blue_collar': 7},
        {'date': '2024-01-05', 'total_blue_collar': 0},
    ]


def test_team_totals_trend_category_filter(client):
    resp = client.get(
        '/admin/team_totals',
        query_string={'season_id': 1, 'trend_stat': 'points', 'trend_category': 'Pickup'}
    )
    html = resp.data.decode('utf-8')
    m = re.search(r"const trendData = (.*?);", html, re.S)
    assert m
    data = json.loads(m.group(1))
    assert data == [
        {'date': '2024-01-05', 'points': 17},
    ]
    assert '<option value="Pickup" selected' in html


def test_team_totals_paint_touch_ppp(client):
    resp = client.get('/admin/team_totals', query_string={'season_id': 1})
    html = resp.data.decode('utf-8')
    assert 'Paint Touch PPP' in html
    assert '>0<' in html  # row label presence
    assert '2.0' in html
    assert '1.0' in html
    assert '3.0' in html
