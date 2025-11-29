from app.stats.field_catalog_practice import PRACTICE_FIELD_GROUPS


def test_practice_field_catalog_preserves_practice_labels():
    advanced_fields = PRACTICE_FIELD_GROUPS.get('Advanced', [])
    assert any(field.get('label') == 'Practice OREB%' for field in advanced_fields)
