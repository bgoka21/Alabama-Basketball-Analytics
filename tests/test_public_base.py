import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path
from unittest.mock import patch

from models.database import db, Season
from models.user import User
from recruits import recruits_bp
from public.routes import public_bp
from admin.routes import admin_bp


@pytest.fixture
def app(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    template_root = repo_root / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    # Include public template folder so render_template('public/...') works
    app.jinja_loader.searchpath.append(str(repo_root / 'public' / 'templates'))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SECRET_KEY'] = 'test'
    app.config['TESTING'] = True
    app.config['UPLOAD_FOLDER'] = str(tmp_path)
    db.init_app(app)
    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'admin.login'

    @lm.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(public_bp)
    app.register_blueprint(recruits_bp, url_prefix='/recruits')
    app.jinja_env.globals['view_exists'] = lambda n: n in app.view_functions
    class DummyUser:
        is_authenticated = False
        is_player = False
    app.jinja_env.globals['current_user'] = DummyUser()

    with app.app_context():
        db.create_all()
        season = Season(id=1, season_name='2024')
        admin = User(username='admin', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add_all([season, admin])
        db.session.commit()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        client.post('/admin/login', data={'username': 'admin', 'password': 'pw'})
        yield client


def test_recruits_page_renders(client):
    rv = client.get('/recruits/')
    assert rv.status_code == 200
    assert b"Recruits" in rv.data


def test_public_templates_no_recursion(client):
    with patch('public.routes.get_yesterdays_summer_stats', return_value={}):
        rv = client.get('/summer_stats')
    assert rv.status_code == 200
