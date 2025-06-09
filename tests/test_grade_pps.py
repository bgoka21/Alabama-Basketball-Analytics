import app


def test_grade_pps_filter():
    test_app = app.create_app()
    with test_app.app_context():
        f = test_app.jinja_env.filters['grade_pps']
        assert f(1.2, 5).startswith("background-color: rgb(")
        assert f(0.9, 0) == ""


def test_grade_fg_pct_filters():
    test_app = app.create_app()
    with test_app.app_context():
        fa = test_app.jinja_env.filters['grade_atr_pct']
        f2 = test_app.jinja_env.filters['grade_fg2_pct']
        f3 = test_app.jinja_env.filters['grade_fg3_pct']
        assert fa(80, 10).startswith("background-color: rgb(")
        assert f2(55, 10).startswith("background-color: rgb(")
        assert f3(35, 5).startswith("background-color: rgb(")
        assert fa(60, 0) == ""
