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
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    app.register_blueprint(admin_bp, url_prefix='/admin')

    with app.app_context():
        db.create_all()
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


def test_create_players_preset(app, client):
    payload = {
        'name': 'Rotation',
        'preset_type': 'players',
        'fields': [],
        'player_ids': [4, '7', 4],
        'mode_default': 'totals',
        'source_default': 'practice',
    }

    response = client.post('/admin/api/presets', json=payload)
    assert response.status_code == 201
    data = response.get_json()
    assert data['name'] == 'Rotation'
    assert data['preset_type'] == 'players'
    assert data['player_ids'] == [4, 7]
    assert data['fields'] == []
    assert data['date_from'] is None
    assert data['date_to'] is None

    with app.app_context():
        stored = SavedStatProfile.query.one()
        assert stored.preset_type == 'players'
        assert json.loads(stored.players_json) == [4, 7]


def test_create_rejects_invalid_inputs(client):
    invalid_type = client.post(
        '/admin/api/presets',
        json={'name': 'Bad', 'preset_type': 'nope', 'fields': [], 'player_ids': []},
    )
    assert invalid_type.status_code == 400
    assert invalid_type.get_json()['error'] == 'preset_type must be one of players, stats, dates, combined'

    missing_name = client.post(
        '/admin/api/presets',
        json={'name': '   ', 'fields': [], 'player_ids': []},
    )
    assert missing_name.status_code == 400
    assert missing_name.get_json()['error'] == 'name is required'

    invalid_players = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Players', 'fields': [], 'player_ids': 'nope'},
    )
    assert invalid_players.status_code == 400
    assert invalid_players.get_json()['error'] == 'player_ids must be a list of integers'


def _create_basic_preset(client, **overrides):
    payload = {
        'name': 'Sample',
        'fields': ['pts'],
        'player_ids': [],
    }
    payload.update(overrides)
    response = client.post('/admin/api/presets', json=payload)
    assert response.status_code == 201
    return response.get_json()


def test_list_and_filter_presets(client):
    players = _create_basic_preset(client, name='Players', preset_type='players')
    _create_basic_preset(client, name='Stats', preset_type='stats', fields=['fg3'])

    list_response = client.get('/admin/api/presets')
    assert list_response.status_code == 200
    payload = list_response.get_json()
    assert isinstance(payload['presets'], list)
    assert len(payload['presets']) == 2
    assert payload['team'] == payload['presets']
    assert payload['private'] == []

    filtered = client.get('/admin/api/presets?preset_type=players')
    assert filtered.status_code == 200
    filtered_payload = filtered.get_json()
    assert len(filtered_payload['presets']) == 1
    assert filtered_payload['presets'][0]['id'] == players['id']

    invalid_filter = client.get('/admin/api/presets?preset_type=nope')
    assert invalid_filter.status_code == 400
    assert invalid_filter.get_json()['error'] == 'preset_type must be one of players, stats, dates, combined'


def test_get_update_and_delete_preset(client):
    created = _create_basic_preset(
        client,
        name='Conference Only',
        preset_type='dates',
        date_from='2025-01-01',
        date_to='2025-03-31',
    )

    detail = client.get(f"/admin/api/presets/{created['id']}")
    assert detail.status_code == 200
    assert detail.get_json()['preset_type'] == 'dates'

    patch_response = client.patch(
        f"/admin/api/presets/{created['id']}",
        json={'name': 'Updated Dates', 'date_from': '2025-01-05', 'date_to': '2025-04-01'},
    )
    assert patch_response.status_code == 200
    patched = patch_response.get_json()
    assert patched['name'] == 'Updated Dates'
    assert patched['date_from'] == '2025-01-05'
    assert patched['date_to'] == '2025-04-01'

    invalid_patch = client.patch(
        f"/admin/api/presets/{created['id']}",
        json={'date_from': '2025-05-01', 'date_to': '2025-04-01'},
    )
    assert invalid_patch.status_code == 400
    assert invalid_patch.get_json()['error'] == 'date_from must be before or equal to date_to'

    delete_response = client.delete(f"/admin/api/presets/{created['id']}")
    assert delete_response.status_code == 200
    assert delete_response.get_json() == {'ok': True}

    missing = client.get(f"/admin/api/presets/{created['id']}")
    assert missing.status_code == 404
    assert missing.get_json()['error'] == 'preset not found'


def test_patch_updates_fields_and_players(client):
    created = _create_basic_preset(client, name='Combined')

    patch_response = client.patch(
        f"/admin/api/presets/{created['id']}",
        json={'fields': ['pts', 'ast', 'ast'], 'player_ids': [1, '2', '1']},
    )
    assert patch_response.status_code == 200
    payload = patch_response.get_json()
    assert payload['fields'] == ['pts', 'ast']
    assert payload['player_ids'] == [1, 2]

    detail = client.get(f"/admin/api/presets/{created['id']}")
    assert detail.status_code == 200
    stored = detail.get_json()
    assert stored['fields'] == ['pts', 'ast']
    assert stored['player_ids'] == [1, 2]
