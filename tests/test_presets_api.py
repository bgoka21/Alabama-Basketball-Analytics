import pytest


def _create_preset(client, admin_auth_headers, **overrides):
    payload = {
        'name': 'Example',
        'preset_type': 'combined',
        'fields': ['pts'],
        'player_ids': [1, 2],
        'date_from': None,
        'date_to': None,
    }
    payload.update(overrides)
    response = client.post('/admin/api/presets', json=payload, headers=admin_auth_headers)
    assert response.status_code == 201
    return response.get_json()


def test_create_players_preset(client, admin_auth_headers):
    payload = {
        'name': 'Unit Players',
        'preset_type': 'players',
        'player_ids': [1, '2', 3, 1],
        'fields': [],
    }
    response = client.post('/admin/api/presets', json=payload, headers=admin_auth_headers)
    assert response.status_code == 201
    data = response.get_json()
    assert data['name'] == 'Unit Players'
    assert data['preset_type'] == 'players'
    assert data['player_ids'] == [1, 2, 3]
    assert data['fields'] == []
    assert data['date_from'] is None
    assert data['date_to'] is None

    detail = client.get(f"/admin/api/presets/{data['id']}")
    assert detail.status_code == 200
    detail_payload = detail.get_json()
    assert detail_payload['id'] == data['id']
    assert detail_payload['player_ids'] == [1, 2, 3]


def test_list_filtering_and_sort_order(client, admin_auth_headers):
    first = _create_preset(
        client,
        admin_auth_headers,
        name='First Players',
        preset_type='players',
        player_ids=[10],
        fields=[],
    )
    second = _create_preset(
        client,
        admin_auth_headers,
        name='Second Players',
        preset_type='players',
        player_ids=[20],
        fields=[],
    )
    _create_preset(
        client,
        admin_auth_headers,
        name='Stats Preset',
        preset_type='stats',
        player_ids=[],
        fields=['fg3'],
    )

    rename = client.patch(
        f"/admin/api/presets/{first['id']}",
        json={'name': 'First Players Updated'},
        headers=admin_auth_headers,
    )
    assert rename.status_code == 200

    listing = client.get('/admin/api/presets?preset_type=players')
    assert listing.status_code == 200
    payload = listing.get_json()
    records = payload['presets']
    assert {row['id'] for row in records} == {first['id'], second['id']}
    sorted_ids = [
        row['id']
        for row in sorted(
            records,
            key=lambda row: (row.get('updated_at') or '', row['id']),
            reverse=True,
        )
    ]
    assert [row['id'] for row in records] == sorted_ids

    all_listing = client.get('/admin/api/presets')
    assert all_listing.status_code == 200
    all_payload = all_listing.get_json()
    assert len(all_payload['presets']) == 3
    assert all_payload['team'] == all_payload['presets']
    assert all_payload['private'] == []


def test_patch_and_delete(client, admin_auth_headers):
    created = _create_preset(
        client,
        admin_auth_headers,
        name='Date Window',
        preset_type='dates',
        player_ids=[],
        fields=[],
        date_from='2025-01-01',
        date_to='2025-03-31',
    )

    patch = client.patch(
        f"/admin/api/presets/{created['id']}",
        json={'name': 'Updated Window', 'fields': ['fg2', 'fg2'], 'player_ids': [7, 7, 8]},
        headers=admin_auth_headers,
    )
    assert patch.status_code == 200
    patched = patch.get_json()
    assert patched['name'] == 'Updated Window'
    assert patched['fields'] == ['fg2']
    assert patched['player_ids'] == [7, 8]

    invalid_patch = client.patch(
        f"/admin/api/presets/{created['id']}",
        json={'fields': 'not-a-list'},
        headers=admin_auth_headers,
    )
    assert invalid_patch.status_code == 400
    assert invalid_patch.get_json()['error'] == 'fields must be a list of strings'

    delete_response = client.delete(f"/admin/api/presets/{created['id']}")
    assert delete_response.status_code == 200
    assert delete_response.get_json() == {'ok': True}

    missing = client.get(f"/admin/api/presets/{created['id']}")
    assert missing.status_code == 404
    assert missing.get_json()['error'] == 'preset not found'


def test_validation_errors(client, admin_auth_headers):
    empty_name = client.post(
        '/admin/api/presets',
        json={'name': '   ', 'fields': [], 'player_ids': []},
        headers=admin_auth_headers,
    )
    assert empty_name.status_code == 400
    assert empty_name.get_json()['error'] == 'name is required'

    too_long_name = client.post(
        '/admin/api/presets',
        json={'name': 'x' * 101, 'fields': [], 'player_ids': []},
        headers=admin_auth_headers,
    )
    assert too_long_name.status_code == 400
    assert too_long_name.get_json()['error'] == 'name must be 100 characters or fewer'

    invalid_type = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Type', 'preset_type': 'nope', 'fields': [], 'player_ids': []},
        headers=admin_auth_headers,
    )
    assert invalid_type.status_code == 400
    assert invalid_type.get_json()['error'] == 'preset_type must be one of players, stats, dates, combined'

    invalid_players = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Players', 'fields': [], 'player_ids': 'nope'},
        headers=admin_auth_headers,
    )
    assert invalid_players.status_code == 400
    assert invalid_players.get_json()['error'] == 'player_ids must be a list of integers'

    invalid_fields = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Fields', 'fields': 'nope', 'player_ids': []},
        headers=admin_auth_headers,
    )
    assert invalid_fields.status_code == 400
    assert invalid_fields.get_json()['error'] == 'fields must be a list of strings'

    invalid_date = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Date', 'fields': [], 'player_ids': [], 'date_from': 'not-a-date'},
        headers=admin_auth_headers,
    )
    assert invalid_date.status_code == 400
    assert invalid_date.get_json()['error'] == 'date_from must be YYYY-MM-DD'

    invalid_order = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Order', 'fields': [], 'player_ids': [], 'date_from': '2025-03-02', 'date_to': '2025-03-01'},
        headers=admin_auth_headers,
    )
    assert invalid_order.status_code == 400
    assert invalid_order.get_json()['error'] == 'date_from must be before or equal to date_to'


def test_dates_preset_variations(client, admin_auth_headers):
    created = client.post(
        '/admin/api/presets',
        json={'name': 'From Only', 'preset_type': 'dates', 'fields': [], 'player_ids': [], 'date_from': '2025-02-01'},
        headers=admin_auth_headers,
    )
    assert created.status_code == 201
    payload = created.get_json()
    assert payload['date_from'] == '2025-02-01'
    assert payload['date_to'] is None

    invalid = client.post(
        '/admin/api/presets',
        json={'name': 'Bad Window', 'preset_type': 'dates', 'fields': [], 'player_ids': [], 'date_from': '2025-05-02', 'date_to': '2025-04-01'},
        headers=admin_auth_headers,
    )
    assert invalid.status_code == 400
    assert invalid.get_json()['error'] == 'date_from must be before or equal to date_to'


def test_get_unknown_returns_404(client):
    response = client.get('/admin/api/presets/9999')
    assert response.status_code == 404
    assert response.get_json()['error'] == 'preset not found'
