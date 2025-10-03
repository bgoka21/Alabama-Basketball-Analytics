import os
import sys

import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from admin.routes import admin_bp
from models.database import db
from models.user import User


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        SECRET_KEY='test-secret',
        SQLALCHEMY_DATABASE_URI='sqlite:///:memory:',
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        WTF_CSRF_ENABLED=False,
    )

    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        if not db.session.get(User, 1):
            user = User(
                id=1,
                username='coach',
                password_hash=generate_password_hash('pw'),
                is_admin=True,
            )
            db.session.add(user)
            db.session.commit()

    yield app

    with app.app_context():
        db.session.remove()
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        with client.session_transaction() as session:
            session['_user_id'] = '1'
            session['_fresh'] = True
        yield client


@pytest.fixture
def admin_auth_headers(client):
    return {'Content-Type': 'application/json'}
