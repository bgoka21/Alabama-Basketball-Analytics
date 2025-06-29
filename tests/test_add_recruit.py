import pytest
from flask import Flask
from pathlib import Path
from flask_login import LoginManager
from models.database import db, PageView
from models.recruit import Recruit
from models.user import User
from routes import recruit_bp
from admin.routes import admin_bp

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
        return User.query.get(int(uid))

    app.register_blueprint(recruit_bp, url_prefix='/recruiting')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.jinja_env.globals['view_exists'] = lambda name: name in app.view_functions

    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()

@pytest.fixture
def client(app):
    with app.test_client() as client:
        yield client

def test_duplicate_check_case_insensitive(client, app):
    data = {'name': '  John Doe  '}
    resp = client.post('/recruiting/add', data=data)
    # first add should redirect
    assert resp.status_code == 302
    with app.app_context():
        rec = Recruit.query.filter_by(name='John Doe').first()
        assert rec is not None

    resp = client.post('/recruiting/add', data={'name': 'john doe'})
    assert resp.status_code == 200
    assert b'Recruit already exists.' in resp.data
    with app.app_context():
        assert Recruit.query.count() == 1
