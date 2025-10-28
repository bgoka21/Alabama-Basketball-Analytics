import json
import re
from datetime import date
from pathlib import Path

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from admin.routes import admin_bp
from models.database import db, Season, Game, PlayerStats, Roster
from models.user import User


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

    with app.app_context():
        db.create_all()

        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)

        roster = Roster(id=1, season_id=1, player_name='#1 Test')
        db.session.add(roster)

        game1 = Game(
            id=1,
            season_id=1,
            game_date=date(2024, 1, 3),
            opponent_name='Team A',
            home_or_away='Home',
            result='W',
        )
        game1.game_types = ['Conference']

        game2 = Game(
            id=2,
            season_id=1,
            game_date=date(2024, 1, 10),
            opponent_name='Team B',
            home_or_away='Away',
            result='L',
        )
        game2.game_types = ['Non-Conference']

        db.session.add_all([game1, game2])

        shots_game1 = [
            {"shot_class": "2fg", "result": "made", "possession_type": "Halfcourt", "drill_labels": []}
            for _ in range(5)
        ]
        shots_game1.append({"shot_class": "2fg", "result": "missed", "possession_type": "Halfcourt", "drill_labels": []})

        shots_game2 = [
            {"shot_class": "3fg", "result": "made", "possession_type": "Halfcourt", "drill_labels": []}
            for _ in range(4)
        ]
        shots_game2.append({"shot_class": "3fg", "result": "missed", "possession_type": "Halfcourt", "drill_labels": []})
        shots_game2.extend([
            {"shot_class": "2fg", "result": "made", "possession_type": "Halfcourt", "drill_labels": []},
            {"shot_class": "2fg", "result": "missed", "possession_type": "Halfcourt", "drill_labels": []},
        ])

        ps1 = PlayerStats(
            game_id=1,
            season_id=1,
            player_name='#1 Test',
            points=10,
            fg2_makes=5,
            fg2_attempts=6,
            fg3_makes=0,
            fg3_attempts=0,
            atr_makes=0,
            atr_attempts=0,
            shot_type_details=json.dumps(shots_game1),
        )

        ps2 = PlayerStats(
            game_id=2,
            season_id=1,
            player_name='#1 Test',
            points=18,
            fg2_makes=1,
            fg2_attempts=2,
            fg3_makes=4,
            fg3_attempts=5,
            atr_makes=0,
            atr_attempts=0,
            ftm=2,
            fta=2,
            shot_type_details=json.dumps(shots_game2),
        )

        db.session.add_all([ps1, ps2])

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


def _season_points(html: str) -> int:
    match = re.search(r"Points</div>\s*<div class=\"text-2xl font-bold mt-2\">(\d+)</div>", html)
    assert match is not None
    return int(match.group(1))


def test_player_detail_multi_game_type_filter(client):
    resp = client.get('/admin/player/%231%20Test', query_string={'mode': 'game'})
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'Team A' in html
    assert 'Team B' in html
    assert _season_points(html) == 28

    resp = client.get(
        '/admin/player/%231%20Test',
        query_string=[('mode', 'game'), ('game_type', 'Conference'), ('game_type', 'Postseason')],
    )
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'Team A' in html
    assert 'Team B' not in html
    assert _season_points(html) == 10
