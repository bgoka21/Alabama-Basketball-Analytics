from datetime import date
from pathlib import Path

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, UploadedFile, Roster
from models.user import User
from admin.routes import admin_bp, build_leaderboard_cache_payload


@pytest.fixture
def app(tmp_path, monkeypatch):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    upload_folder = tmp_path / 'uploads'
    upload_folder.mkdir()
    app.config['UPLOAD_FOLDER'] = str(upload_folder)

    monkeypatch.setattr(
        'admin.routes.parse_practice_csv',
        lambda *args, **kwargs: {
            'lineup_efficiencies': {},
            'player_on_off': {},
        },
    )
    monkeypatch.setattr(
        'admin.routes.parse_csv',
        lambda *args, **kwargs: {
            'lineup_efficiencies': {},
            'offensive_breakdown': {},
            'defensive_breakdown': {},
        },
    )

    db.init_app(app)
    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):  # pragma: no cover - required by flask-login
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        db.session.add_all([
            Roster(season_id=1, player_name='#1 A'),
            Roster(season_id=1, player_name='#2 B'),
        ])
        admin = User(
            username='admin',
            password_hash=generate_password_hash('pw'),
            is_admin=True,
        )
        db.session.add(admin)
        upload = UploadedFile(
            id=1,
            season_id=1,
            filename='practice.csv',
            category='Official Practice',
            file_date=date(2024, 1, 1),
        )
        db.session.add(upload)
        db.session.commit()

    csv_path = Path(app.config['UPLOAD_FOLDER']) / 'practice.csv'
    csv_path.write_text('dummy,data\n')

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_parse_practice_skips_leaderboard_rebuild(client, app, monkeypatch):
    called = False

    def fail(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError('rebuild_leaderboards_for_season should not be called')
# HOTFIX disabled: 
    monkeypatch.setattr('services.cache_leaderboard.rebuild_leaderboards_for_season', fail, raising=False)

    resp = client.post('/admin/parse/1')
    assert resp.status_code == 302

    assert called is False
    with app.app_context():
        uploaded = db.session.get(UploadedFile, 1)
        assert uploaded.parse_status == 'Parsed Successfully'


def test_manual_leaderboard_rebuild_endpoint(client, monkeypatch):
    captured = {}

    def fake_cache_build_all(season_id, builder, keys):
        captured['season_id'] = season_id
        captured['builder'] = builder
        captured['keys'] = tuple(keys)

    monkeypatch.setattr('admin.routes.cache_build_all', fake_cache_build_all)

    resp = client.post('/admin/admin/rebuild_leaderboards/1')
    assert resp.status_code == 200
    assert resp.get_json() == {'status': 'ok', 'season_id': 1}

    assert captured['season_id'] == 1
    assert captured['builder'] is build_leaderboard_cache_payload
    assert captured['keys']
