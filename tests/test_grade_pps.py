import app


def test_grade_pps_filter():
    test_app = app.create_app()
    with test_app.app_context():
        f = test_app.jinja_env.filters['grade_pps']
        assert f(1.2, 5).strip().startswith("grade-token")
        assert f(0.9, 0) == ""


def test_grade_fg_pct_filters():
    test_app = app.create_app()
    with test_app.app_context():
        f2 = test_app.jinja_env.filters['grade_atr2fg_pct']
        f3 = test_app.jinja_env.filters['grade_3fg_pct']
        assert f2(80, 10).strip().startswith("grade-token")
        assert f3(35, 5).strip().startswith("grade-token")
        assert f2(60, 0) == ""


def test_grade_token_global():
    test_app = app.create_app()
    with test_app.app_context():
        helper = test_app.jinja_env.globals['grade_token']
        assert helper('pps', 1.1).startswith('grade-token')
        assert helper('fg3_pct', 0.37).startswith('grade-token')
        assert helper('unknown', 50) is None
