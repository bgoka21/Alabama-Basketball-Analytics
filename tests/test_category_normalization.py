from app.utils.category_normalization import normalize_category


def test_normalize_category_handles_plural_practice():
    assert normalize_category("Official Practices") == "Official Practice"
    assert normalize_category("official practice") == "Official Practice"
    assert normalize_category(" Summer Workouts ") == "Summer Workouts"
