import pytest
from flask import Flask
from pathlib import Path
from flask_login import LoginManager, AnonymousUserMixin
from models.database import db
from models.recruit import Recruit
from admin.routes import admin_bp
import routes


@pytest.fixture
def app():
    template_root = Path(__file__).resolve().parents[1] / 'templates'
    app = Flask(__name__, template_folder=str(template_root))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    lm = LoginManager()
    lm.init_app(app)
    lm.login_view = 'login'
    @lm.user_loader
    def load_user(uid):
        return None
    app.login_manager = lm

    with app.app_context():
        db.create_all()
        app.register_blueprint(admin_bp, url_prefix='/admin')
        app.add_url_rule('/recruits', view_func=routes.recruits_view)
        app.jinja_env.globals['view_exists'] = lambda n: n in app.view_functions
        app.jinja_env.globals['current_user'] = AnonymousUserMixin()
    yield app
    with app.app_context():
        db.drop_all()

@pytest.fixture
def client(app):
    return app.test_client()


def test_recruits_page_lists_entries(client, app):
    with app.app_context():
        db.session.add(Recruit(name='Jane Doe', year=2025))
        db.session.commit()
    resp = client.get('/recruits')
    assert resp.status_code == 200
    assert b'Jane Doe' in resp.data
