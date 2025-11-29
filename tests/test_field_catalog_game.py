from admin.routes import _group_game_field_catalog


def test_game_field_catalog_strips_practice_prefix_from_groups_and_labels():
    catalog = _group_game_field_catalog()

    assert isinstance(catalog, dict)
    assert not any(label.startswith('Practice •') for label in catalog)

    for group_label, fields in catalog.items():
        assert not group_label.startswith('Practice •')
        for field in fields:
            assert 'Practice ' not in str(field.get('label', ''))
