import json
import pytest
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from admin.routes import admin_bp
from models.database import db, SavedStatProfile
from models.user import User


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    app.config['SECRET_KEY'] = 'test-secret'
    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
        user = User(id=1, username='coach', password_hash=generate_password_hash('pw'), is_admin=True)
        db.session.add(user)
        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    with app.test_client() as client:
        with client.session_transaction() as session:
            session['_user_id'] = '1'
            session['_fresh'] = True
        yield client


def test_create_preset_with_player_ids(app, client):
    payload = {
        'name': 'Rotation',
        'fields': ['pts', 'ast'],
        'player_ids': [4, '7', 4],
        'mode_default': 'totals',
        'source_default': 'practice',
        'visibility': 'private'
    }

    response = client.post('/admin/api/presets', json=payload)
    assert response.status_code == 201
    data = response.get_json()
    assert data['player_ids'] == [4, 7]

    with app.app_context():
        stored = SavedStatProfile.query.one()
        assert json.loads(stored.players_json) == [4, 7]


def test_update_preset_player_ids(app, client):
    with app.app_context():
        profile = SavedStatProfile(
            name='Existing',
            fields_json=json.dumps(['pts']),
            players_json=json.dumps([2]),
            owner_id=1,
            visibility='private'
        )
        db.session.add(profile)
        db.session.commit()
        preset_id = profile.id

    response = client.patch('/admin/api/presets', json={'id': preset_id, 'player_ids': ['12', 35]})
    assert response.status_code == 200
    data = response.get_json()
    assert data['player_ids'] == [12, 35]

    with app.app_context():
        refreshed = db.session.get(SavedStatProfile, preset_id)
        assert json.loads(refreshed.players_json) == [12, 35]


def test_create_preset_rejects_invalid_player_ids(client):
    response = client.post(
        '/admin/api/presets',
        json={'name': 'Bad', 'fields': [], 'player_ids': 'nope'}
    )
    assert response.status_code == 400
    data = response.get_json()
    assert data['error'] == 'Player ids must be a list of integers'
