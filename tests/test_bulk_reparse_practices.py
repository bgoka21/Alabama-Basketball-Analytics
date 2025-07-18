import pytest
from datetime import date
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path

from models.database import db, Season, Roster, UploadedFile, Possession, PlayerPossession
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
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        db.session.add_all([
            Roster(season_id=1, player_name='#1 A'),
            Roster(season_id=1, player_name='#2 B'),
        ])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        uf1 = UploadedFile(id=1, season_id=1, filename='p1.csv', category='Official Practices', file_date=date(2024, 1, 1))
        uf2 = UploadedFile(id=2, season_id=1, filename='p2.csv', category='Official Practices', file_date=date(2024, 1, 2))
        db.session.add_all([uf1, uf2])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_bulk_reparse(client, app):
    p1 = Path(app.config['UPLOAD_FOLDER']) / 'p1.csv'
    p2 = Path(app.config['UPLOAD_FOLDER']) / 'p2.csv'
    content = 'Row,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,#1 A,#2 B\n'
    content += 'Crimson,"#1 A","#2 B",2FG+,\n'
    p1.write_text(content)
    p2.write_text(content)

    client.post('/admin/parse/1')
    client.post('/admin/parse/2')
    with app.app_context():
        assert Possession.query.count() == 4
        assert PlayerPossession.query.count() == 4

    resp = client.post('/admin/bulk-action', data={'selected_files': ['1', '2'], 'action': 'reparse'})
    assert resp.status_code == 302

    with app.app_context():
        assert Possession.query.count() == 4
        assert PlayerPossession.query.count() == 4
