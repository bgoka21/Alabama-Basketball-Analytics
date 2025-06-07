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
        grade_pps_f = test_app.jinja_env.filters['grade_pps']
        f2 = test_app.jinja_env.filters['grade_atr2fg_pct']
        f3 = test_app.jinja_env.filters['grade_3fg_pct']

        assert f2(60, 5) == grade_pps_f(1.2, 5)
        assert f3(50, 4) == grade_pps_f(1.5, 4)
        assert f2(70, 0) == ""
