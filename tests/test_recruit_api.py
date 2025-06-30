import pytest
from flask import Flask
from pathlib import Path
from flask_login import LoginManager
from models.database import db
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

def test_api_add_and_list(client, app):
    resp = client.post('/recruiting/api/recruits', json={'name': 'T. Player'})
    assert resp.status_code == 201
    data = resp.get_json()
    assert data['name'] == 'T. Player'

    resp = client.get('/recruiting/api/recruits')
    assert resp.status_code == 200
    items = resp.get_json()
    assert len(items) == 1
    assert items[0]['name'] == 'T. Player'
