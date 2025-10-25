import json
import re
from datetime import date
import pytest
from flask import Flask
from flask_login import LoginManager
from pathlib import Path
from werkzeug.security import generate_password_hash

from models.database import (
    db,
    Season,
    Practice,
    PlayerStats,
    Roster,
    Possession,
    PlayerPossession,
)
from models.user import User
from public.routes import public_bp
from admin.routes import admin_bp
from utils.shottype import persist_player_shot_details


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
    app.register_blueprint(public_bp)

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        practice = Practice(id=1, season_id=1, date=date(2024,1,2), category='Official Practice')
        db.session.add(practice)
        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        shots = [
            {"shot_class":"2fg","result":"made","2fg_type":"Dunk","drill_labels":["SCRIMMAGE"]},
            {"shot_class":"2fg","result":"made","2fg_type":"Dunk","drill_labels":["4V4 DRILLS"]},
        ]
        player_stat = PlayerStats(practice_id=1, season_id=1, player_name='#1 Test', shot_type_details=json.dumps(shots))
        player_stat.stat_details = json.dumps([
            {"event": "floor_dive", "drill_labels": ["4V4 DRILLS"]},
        ])
        db.session.add(player_stat)
        persist_player_shot_details(player_stat, shots, replace=True)
        possession1 = Possession(id=1, season_id=1, practice_id=1, drill_labels='4V4 DRILLS')
        possession2 = Possession(id=2, season_id=1, practice_id=1, drill_labels='4V4 DRILLS')
        db.session.add_all([possession1, possession2])
        db.session.add_all([
            PlayerPossession(possession_id=1, player_id=1),
            PlayerPossession(possession_id=2, player_id=1),
        ])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client


def _dunk_count(html):
    start = html.index('Dunks Get You Paid')
    snippet = html[start:]
    row_start = snippet.index('#1 Test')
    row_end = snippet.index('</tr>', row_start)
    row = snippet[row_start:row_end]
    if '>2<' in row:
        return 2
    if '>1<' in row:
        return 1
    return 0


def _poss_per_bcp_values(html):
    pattern = r'<td[^>]*data-key="poss_per_bcp"[^>]*>([^<]*)</td>'
    return [match.strip() for match in re.findall(pattern, html)]


def test_home_label_filter(client):
    resp = client.get('/practice_home')
    html = resp.data.decode('utf-8')
    assert _dunk_count(html) == 2

    resp = client.get('/practice_home', query_string={'label':'4V4 DRILLS'})
    html = resp.data.decode('utf-8')
    assert _dunk_count(html) == 1


def test_poss_per_bcp_column_visibility(client):
    resp = client.get('/practice_home')
    html = resp.data.decode('utf-8')
    assert 'Poss / BCP' not in html

    resp = client.get('/practice_home', query_string={'label': '4V4 DRILLS'})
    html = resp.data.decode('utf-8')
    assert 'Poss / BCP' in html
    assert _poss_per_bcp_values(html) == ['1.00']
