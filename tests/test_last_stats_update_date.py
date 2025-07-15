import pytest
from datetime import date, datetime
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path

from models.database import db, Season, UploadedFile
from models.user import User
from admin.routes import admin_bp
from public.routes import public_bp

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
        season = Season(id=1, season_name='2024', start_date=date(2024, 1, 1))
        db.session.add(season)
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(admin)
        uf = UploadedFile(id=1, season_id=1, filename='g.csv', category='Game',
                          file_date=date(2024, 6, 28),
                          parse_status='Parsed Successfully',
                          last_parsed=datetime(2024, 7, 10))
        db.session.add(uf)
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()

@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client

def test_sidebar_uses_file_date(client):
    resp = client.get('/admin/dashboard')
    assert resp.status_code == 200
    html = resp.data.decode()
    assert 'Through June 28th' in html
