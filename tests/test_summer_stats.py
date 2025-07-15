import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from models.database import db
from models.user import User
from admin.routes import admin_bp
from public.routes import public_bp


@pytest.fixture
def app():
    repo_root = Path(__file__).resolve().parents[1]
    template_root = repo_root / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    # Include public template folder so render_template('public/...') works
    app.jinja_loader.searchpath.append(str(repo_root / 'public' / 'templates'))
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


def test_summer_stats_renders_table(client):
    sample = {
        'John Doe': SimpleNamespace(team='ATL', opponent='NYK', MP='10', PTS='12', REB='5', AST='3'),
        'Jane Roe': SimpleNamespace(team='LAL', opponent='BOS', MP='15', PTS='20', REB='7', AST='4'),
    }
    with patch('public.routes.get_yesterdays_summer_stats', return_value=sample):
        resp = client.get('/summer_stats')
    assert resp.status_code == 200
    html = resp.data.decode('utf-8')
    assert 'John Doe' in html
    assert 'NYK' in html
    assert 'Jane Roe' in html
    assert 'BOS' in html
