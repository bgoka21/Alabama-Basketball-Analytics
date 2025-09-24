import pytest
from datetime import date, datetime, timedelta
from pathlib import Path
from flask import Flask
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Practice, Season
from models.user import User
from admin.routes import admin_bp


LAST_DT = date(2025, 9, 18)


@pytest.fixture
def app_client():
    template_root = Path(__file__).resolve().parents[1] / "templates"
    app = Flask(__name__, template_folder=str(template_root))
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SECRET_KEY"] = "test"
    app.config["TESTING"] = True
    db.init_app(app)

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = "admin.login"

    @login_manager.user_loader
    def load_user(uid):
        return db.session.get(User, int(uid))

    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.jinja_env.globals["view_exists"] = lambda name: name in app.view_functions

    with app.app_context():
        db.create_all()
        db.session.add(Season(id=1, season_name="2025-26", start_date=LAST_DT))
        db.session.add(User(username="admin", password_hash=generate_password_hash("pw"), is_admin=True))
        db.session.commit()

    ctx = app.app_context()
    ctx.push()
    try:
        with app.test_client() as client:
            client.post("/admin/login", data={"username": "admin", "password": "pw"})
            yield client
    finally:
        ctx.pop()
        with app.app_context():
            db.drop_all()
            db.session.remove()


def _patch_last_practice(monkeypatch):
    import admin._leaderboard_helpers as helpers

    class _Stub:
        def __init__(self, d):
            self.date = d
            self.created_at = datetime.combine(d, datetime.min.time())

    monkeypatch.setattr(helpers, "get_last_practice", lambda session, season_id: _Stub(LAST_DT))


def _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals):
    def _fake_compute(
        *,
        stat_key,
        season_id,
        start_dt=None,
        end_dt=None,
        label_set=None,
        session=None,
        **kwargs,
    ):
        is_last = start_dt == LAST_DT and end_dt == LAST_DT
        if is_last:
            return last_totals, last_rows
        return season_totals, season_rows

    return _fake_compute


def test_with_last_practice_returns_last_slice(app_client):
    from admin import _leaderboard_helpers as helpers

    earlier = LAST_DT - timedelta(days=1)
    db.session.add_all(
        [
            Practice(season_id=1, date=earlier, category="Test"),
            Practice(season_id=1, date=LAST_DT, category="Test"),
        ]
    )
    db.session.commit()

    calls = []

    def _fake_compute(
        *,
        stat_key,
        season_id,
        start_dt=None,
        end_dt=None,
        label_set=None,
        session=None,
        **kwargs,
    ):
        calls.append((start_dt, end_dt))
        if start_dt == LAST_DT and end_dt == LAST_DT:
            return ({"plus": 2, "opps": 3}, [{"player_name": "P", "plus": 2, "opps": 3}])
        return ({"plus": 5, "opps": 8}, [{"player_name": "P", "plus": 5, "opps": 8}])

    ctx = helpers.with_last_practice(
        db.session,
        season_id=1,
        compute_fn=_fake_compute,
        stat_key="defense",
    )

    assert calls[0] == (None, None)
    assert calls[1] == (LAST_DT, LAST_DT)
    assert ctx["season_rows"]
    assert ctx["last_rows"]
    assert ctx["last_practice_date"] == LAST_DT


def _assert_dual_table_basics(html, section_title, expected_texts):
    assert f"{section_title} — Practice Totals" in html
    assert f"{section_title} — Last Practice" in html
    assert "Sep 18, 2025" in html
    for text in expected_texts:
        assert text in html


@pytest.mark.usefixtures("app_client")
class TestDualViews:

    def test_defense_bumps_dual(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [
            {"player_name": "P1", "plus": 3, "opps": 5},
            {"player_name": "P2", "plus": 2, "opps": 3},
        ]
        season_totals = {"plus": 5, "opps": 8}

        last_rows = [
            {"player_name": "P1", "plus": 2, "opps": 3},
            {"player_name": "P2", "plus": 1, "opps": 1},
        ]
        last_totals = {"plus": 3, "opps": 4}

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_defense_bumps", fake)

        resp = app_client.get("/admin/leaderboard/defense/bumps")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="Defense — Bumps",
            expected_texts=[
                "3",
                "5",
                "2",
                "3",
                "1",
                "60.0%",
                "66.7%",
                "100.0%",
            ],
        )

    def test_reb_offense_split_crash_back(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [
            {"player_name": "A", "plus": 4, "opps": 8, "subtype": "crash"},
            {"player_name": "B", "plus": 2, "opps": 4, "subtype": "crash"},
            {"player_name": "C", "plus": 5, "opps": 10, "subtype": "back_man"},
        ]
        season_totals = {
            "crash": {"plus": 6, "opps": 12},
            "back_man": {"plus": 5, "opps": 10},
        }

        last_rows = [
            {"player_name": "A", "plus": 1, "opps": 2, "subtype": "crash"},
            {"player_name": "C", "plus": 3, "opps": 5, "subtype": "back_man"},
        ]
        last_totals = {
            "crash": {"plus": 1, "opps": 2},
            "back_man": {"plus": 3, "opps": 5},
        }

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_offensive_rebounding", fake)

        resp = app_client.get("/admin/leaderboard/rebounding/offense")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="Offensive Rebounding",
            expected_texts=[
                "Crash +",
                "Back Man +",
                "Box Out +",
                "4",
                "8",
                "2",
                "4",
                "5",
                "10",
                "1",
                "2",
                "3",
                "5",
                "50.0%",
                "60.0%",
            ],
        )

    def test_reb_defense_with_given_up(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [
            {"player_name": "A", "plus": 7, "opps": 10, "off_reb_given_up": 2},
            {"player_name": "B", "plus": 3, "opps": 5, "off_reb_given_up": 1},
        ]
        season_totals = {"plus": 10, "opps": 15, "off_reb_given_up": 3}

        last_rows = [
            {"player_name": "A", "plus": 2, "opps": 3, "off_reb_given_up": 1},
        ]
        last_totals = {"plus": 2, "opps": 3, "off_reb_given_up": 1}

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_defensive_rebounding", fake)

        resp = app_client.get("/admin/leaderboard/rebounding/defense")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="Defensive Rebounding",
            expected_texts=[
                "Given Up",
                "7",
                "10",
                "3",
                "5",
                "2",
                "1",
                "70.0%",
                "60.0%",
                "66.7%",
            ],
        )

    def test_collisions_gap_help(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [{"player_name": "X", "plus": 4, "opps": 5}]
        season_totals = {"plus": 4, "opps": 5}

        last_rows = [{"player_name": "X", "plus": 2, "opps": 2}]
        last_totals = {"plus": 2, "opps": 2}

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_collisions_gap_help", fake)

        resp = app_client.get("/admin/leaderboard/collisions/gap-help")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="Collisions — Gap Help",
            expected_texts=["4", "5", "2", "2", "80.0%", "100.0%"],
        )

    def test_pnr_gap_help_split(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        gap_season_rows = [
            {"player_name": "G1", "plus": 5, "opps": 10, "pct": 50.0},
        ]
        low_season_rows = [
            {"player_name": "L1", "plus": 6, "opps": 12, "pct": 50.0},
        ]
        gap_last_rows = [
            {"player_name": "G1", "plus": 3, "opps": 5, "pct": 60.0},
        ]
        low_last_rows = [
            {"player_name": "L1", "plus": 2, "opps": 4, "pct": 50.0},
        ]

        gap_season_totals = {"plus": 5, "opps": 10, "pct": 50.0}
        low_season_totals = {"plus": 6, "opps": 12, "pct": 50.0}
        gap_last_totals = {"plus": 3, "opps": 5, "pct": 60.0}
        low_last_totals = {"plus": 2, "opps": 4, "pct": 50.0}

        def _fake_compute(
            *,
            stat_key,
            season_id,
            start_dt=None,
            end_dt=None,
            label_set=None,
            session=None,
            role=None,
            **kwargs,
        ):
            is_last = start_dt == LAST_DT and end_dt == LAST_DT
            if role == "low_man":
                rows = low_last_rows if is_last else low_season_rows
                totals = low_last_totals if is_last else low_season_totals
            else:
                rows = gap_last_rows if is_last else gap_season_rows
                totals = gap_last_totals if is_last else gap_season_totals
            return totals, rows

        fake = _fake_compute
        monkeypatch.setattr(rmod, "compute_pnr_gap_help", fake)

        resp = app_client.get("/admin/leaderboard/pnr/gap-help")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        assert "PnR Gap Help" in html
        assert "PnR Gap Help — Low Man" in html
        assert "10" in html and "50.0%" in html
        assert "12" in html and "50.0%" in html
        assert "5" in html and "60.0%" in html
        assert "4" in html and "50.0%" in html

    def test_pnr_grade_split(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [
            {"player_name": "C", "plus": 7, "opps": 14, "subtype": "close_window"},
            {"player_name": "S", "plus": 9, "opps": 18, "subtype": "shut_door"},
        ]
        season_totals = {
            "close_window": {"plus": 7, "opps": 14},
            "shut_door": {"plus": 9, "opps": 18},
        }

        last_rows = [
            {"player_name": "C", "plus": 4, "opps": 5, "subtype": "close_window"},
            {"player_name": "S", "plus": 3, "opps": 6, "subtype": "shut_door"},
        ]
        last_totals = {
            "close_window": {"plus": 4, "opps": 5},
            "shut_door": {"plus": 3, "opps": 6},
        }

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_pnr_grade", fake)

        resp = app_client.get("/admin/leaderboard/pnr/grade")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="PnR Grade",
            expected_texts=["A", "B", "C", "D", "F", "Grade"],
        )
