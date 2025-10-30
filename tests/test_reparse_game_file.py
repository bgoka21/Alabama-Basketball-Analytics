import json
import os
from datetime import date
from pathlib import Path

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from admin.routes import admin_bp
from models.database import (
    db,
    Season,
    UploadedFile,
    Game,
    TeamStats,
)
from models.user import User


@pytest.fixture
def app(tmp_path):
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    upload_folder = tmp_path / 'uploads'
    upload_folder.mkdir()
    app.config['UPLOAD_FOLDER'] = str(upload_folder)

    db.init_app(app)
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'admin.login'

    @login_manager.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        admin_user = User(
            username='admin',
            password_hash=generate_password_hash('pw'),
            is_admin=True,
        )
        db.session.add(admin_user)
        upload = UploadedFile(
            id=1,
            season_id=1,
            filename='game.csv',
            category='Game',
            file_date=date(2024, 1, 1),
        )
        db.session.add(upload)
        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_reparse_game(client, app, monkeypatch):
    csv_path = Path(app.config['UPLOAD_FOLDER']) / 'game.csv'
    csv_path.write_text('Row\n')

    import admin.routes as routes

    call_count = {'value': 0}

    def fake_parse_csv(file_path, game_id, season_id):
        call_count['value'] += 1
        filename = os.path.basename(file_path)
        game = Game.query.filter_by(csv_filename=filename).first()
        if not game:
            game = Game(
                season_id=season_id,
                game_date=date(2024, 1, 1),
                opponent_name='Opponent',
                home_or_away='Home',
                result='W',
                csv_filename=filename,
            )
            db.session.add(game)
            db.session.commit()

        stat = TeamStats(
            game_id=game.id,
            season_id=season_id,
            total_points=call_count['value'],
        )
        db.session.add(stat)
        db.session.commit()

        return {
            'offensive_breakdown': {'run': call_count['value']},
            'defensive_breakdown': {'run': call_count['value']},
            'lineup_efficiencies': {},
        }

    monkeypatch.setattr(routes, 'parse_csv', fake_parse_csv)

    resp = client.post('/admin/parse/1')
    assert resp.status_code == 302

    with app.app_context():
        upload = db.session.get(UploadedFile, 1)
        assert call_count['value'] == 1
        assert TeamStats.query.count() == 1
        assert json.loads(upload.offensive_breakdown)['possession_type']['run'] == 1
        game = Game.query.filter_by(csv_filename='game.csv').one()
        first_game_id = game.id

    resp = client.post('/admin/reparse/1')
    assert resp.status_code == 302

    with app.app_context():
        upload = db.session.get(UploadedFile, 1)
        assert call_count['value'] == 2
        assert TeamStats.query.count() == 1
        assert TeamStats.query.first().total_points == 2
        assert json.loads(upload.offensive_breakdown)['possession_type']['run'] == 2
        assert json.loads(upload.defensive_breakdown)['possession_type']['run'] == 2
        assert upload.parse_status == 'Parsed Successfully'
        assert upload.last_parsed is not None
        game = Game.query.filter_by(csv_filename='game.csv').one()
        assert game.id == first_game_id
