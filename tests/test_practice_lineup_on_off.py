import json
from datetime import date
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path

from models.database import db, Season, Roster, UploadedFile
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
        season = Season(id=1, season_name='2024', start_date=date(2024,1,1))
        db.session.add(season)
        db.session.add_all([
            Roster(season_id=1, player_name='#1 A'),
            Roster(season_id=1, player_name='#2 B'),
        ])
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        uf = UploadedFile(id=1, season_id=1, filename='p.csv', category='Official Practice', file_date=date(2024,1,1))
        db.session.add(uf)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()

@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username':'admin','password':'pw'})
        yield client

def test_lineup_and_on_off(client, app):
    csv_path = Path(app.config['UPLOAD_FOLDER']) / 'p.csv'
    csv_content = 'Row,CRIMSON PLAYER POSSESSIONS,WHITE PLAYER POSSESSIONS,#1 A,#2 B\n'
    csv_content += 'Crimson,"#1 A, #2 B",,2FG+,\n'
    csv_content += 'Crimson,"#2 B",,,3FG+\n'
    csv_path.write_text(csv_content)

    resp = client.post('/admin/parse/1')
    assert resp.status_code == 302

    with app.app_context():
        uf = db.session.get(UploadedFile, 1)
        lineups = json.loads(uf.lineup_efficiencies)
        on_off = json.loads(uf.player_on_off)

    lineup_ppp = lineups['2']['Crimson']['#1 A,#2 B']
    assert pytest.approx(lineup_ppp, 0.01) == 2.0

    b_stats = on_off['#2 B']['Crimson']
    assert pytest.approx(b_stats['on'], 0.01) == 2.5
    assert b_stats['off'] is None
