import os
from datetime import date

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Season, UploadedFile, Game, Practice, TeamStats, PlayerStats, BlueCollarStats, OpponentBlueCollarStats, Possession, PlayerPossession, Roster
from models.user import User
from admin.routes import admin_bp


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
    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))
    app.register_blueprint(admin_bp, url_prefix='/admin')
    with app.app_context():
        db.create_all()
    yield app
    # teardown
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        with app.app_context():
            admin_user = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
            db.session.add(admin_user)
            season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
            db.session.add(season)
            roster = Roster(season_id=1, player_name='#1 Test')
            db.session.add(roster)
            db.session.commit()
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client


def _create_game(app):
    with app.app_context():
        uf = UploadedFile(id=1, season_id=1, filename='g.csv', category='Game', file_date=date.today())
        db.session.add(uf)
        game = Game(id=1, season_id=1, game_date=date.today(), opponent_name='Opp', home_or_away='H', csv_filename='g.csv')
        db.session.add(game)
        team = TeamStats(game_id=1, season_id=1)
        db.session.add(team)
        player = PlayerStats(game_id=1, season_id=1, player_name='Test')
        db.session.add(player)
        bc = BlueCollarStats(game_id=1, season_id=1)
        db.session.add(bc)
        obc = OpponentBlueCollarStats(game_id=1, season_id=1)
        db.session.add(obc)
        poss = Possession(id=1, game_id=1, season_id=1)
        db.session.add(poss)
        pp = PlayerPossession(possession_id=1, player_id=1)
        db.session.add(pp)
        db.session.commit()
        path = os.path.join(app.config['UPLOAD_FOLDER'], 'g.csv')
        with open(path, 'w') as f:
            f.write('x')
        return uf.id, game.id, path


def _create_practice(app):
    with app.app_context():
        uf = UploadedFile(id=2, season_id=1, filename='p.csv', category='Official Practice', file_date=date.today())
        db.session.add(uf)
        practice = Practice(id=1, season_id=1, date=uf.file_date, category='Official Practice')
        db.session.add(practice)
        team = TeamStats(practice_id=1, season_id=1)
        db.session.add(team)
        player = PlayerStats(practice_id=1, season_id=1, player_name='Test')
        db.session.add(player)
        bc = BlueCollarStats(practice_id=1, season_id=1)
        db.session.add(bc)
        obc = OpponentBlueCollarStats(practice_id=1, season_id=1)
        db.session.add(obc)
        poss = Possession(id=2, game_id=None, practice_id=1, season_id=1)
        db.session.add(poss)
        pp = PlayerPossession(possession_id=2, player_id=1)
        db.session.add(pp)
        db.session.commit()
        path = os.path.join(app.config['UPLOAD_FOLDER'], 'p.csv')
        with open(path, 'w') as f:
            f.write('x')
        return uf.id, practice.id, path


def test_delete_game_data(client, app):
    file_id, game_id, path = _create_game(app)
    assert os.path.exists(path)
    client.post(f'/admin/delete-data/{file_id}')
    with app.app_context():
        assert db.session.get(UploadedFile, file_id) is None
        assert db.session.get(Game, game_id) is None
        assert TeamStats.query.filter_by(game_id=game_id).count() == 0
        assert PlayerStats.query.filter_by(game_id=game_id).count() == 0
        assert BlueCollarStats.query.filter_by(game_id=game_id).count() == 0
        assert OpponentBlueCollarStats.query.filter_by(game_id=game_id).count() == 0
        assert Possession.query.filter_by(game_id=game_id).count() == 0
        assert PlayerPossession.query.count() == 0
    assert not os.path.exists(path)


def test_delete_practice_data(client, app):
    file_id, practice_id, path = _create_practice(app)
    assert os.path.exists(path)
    client.post(f'/admin/delete-data/{file_id}')
    with app.app_context():
        assert db.session.get(UploadedFile, file_id) is None
        assert db.session.get(Practice, practice_id) is None
        assert TeamStats.query.filter_by(practice_id=practice_id).count() == 0
        assert PlayerStats.query.filter_by(practice_id=practice_id).count() == 0
        assert BlueCollarStats.query.filter_by(practice_id=practice_id).count() == 0
        assert OpponentBlueCollarStats.query.filter_by(practice_id=practice_id).count() == 0
        assert Possession.query.filter_by(practice_id=practice_id).count() == 0
        assert PlayerPossession.query.count() == 0
    assert not os.path.exists(path)
