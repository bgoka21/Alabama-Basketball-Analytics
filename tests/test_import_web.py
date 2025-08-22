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


@pytest.fixture
def app():
    template_root = Path(__file__).resolve().parents[1] / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    db.init_app(app)
    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(recruits_bp, url_prefix='/recruits')

    with app.app_context():
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
        yield client


def login(client):
    client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})


def test_import_page_requires_login(client):
    rv = client.get("/recruits/import")
    assert rv.status_code in (302, 401)


def test_money_board_has_import_link(client):
    login(client)
    rv = client.get("/recruits/money")
    assert rv.status_code == 200
    assert b"Import Workbook" in rv.data
