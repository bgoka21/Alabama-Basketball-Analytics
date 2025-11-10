import pytest
from datetime import date, datetime, timedelta
from pathlib import Path
from flask import Flask, render_template_string
from flask_login import LoginManager
from werkzeug.security import generate_password_hash

from models.database import db, Practice, Season, Session, PlayerStats, Roster
from models.user import User
from admin.routes import admin_bp

import app as app_module


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
        db.session.add(
            Session(
                id=1,
                season_id=1,
                name="Official Practice",
                start_date=LAST_DT - timedelta(days=14),
                end_date=LAST_DT - timedelta(days=7),
            )
        )
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

    def test_practice_leaderboard_filters_non_roster_players(self, monkeypatch, app_client):
        import admin.routes as rmod

        practice = Practice(season_id=1, date=LAST_DT, category="Test")
        roster_entry = Roster(season_id=1, player_name="Roster Player")
        db.session.add_all([practice, roster_entry])
        db.session.flush()

        db.session.add_all(
            [
                PlayerStats(
                    season_id=1,
                    practice_id=practice.id,
                    player_name="Roster Player",
                    points=12,
                ),
                PlayerStats(
                    season_id=1,
                    practice_id=practice.id,
                    player_name="Walk On",
                    points=7,
                ),
            ]
        )
        db.session.commit()

        class _Summary:
            offensive_possessions_on = 10
            ppp_on_offense = 1.1
            ppp_off_offense = 0.95

        monkeypatch.setattr(rmod, "get_on_off_summary", lambda **kwargs: _Summary())
        monkeypatch.setattr(
            rmod,
            "get_turnover_rates_onfloor",
            lambda **kwargs: {
                'team_turnover_rate_on': 12.0,
                'indiv_turnover_rate': 8.0,
                'individual_team_turnover_pct': 5.0,
                'bamalytics_turnover_rate': 7.0,
            },
        )
        monkeypatch.setattr(
            rmod,
            "get_rebound_rates_onfloor",
            lambda **kwargs: {
                'off_reb_rate_on': 15.0,
                'def_reb_opportunities_on': 4,
                'def_reb_rate_on': 20.0,
            },
        )

        _, rows, _ = rmod.compute_leaderboard('points', 1)

        player_names = [name for name, _ in rows]
        assert "Roster Player" in player_names
        assert "Walk On" not in player_names

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

        assert "Official Practice" in html

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
                "Off Reb&#39;s Given Up",
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

        import app.grades as grades_mod

        grade_token_calls: list[str] = []
        grade_scale_calls: list[str] = []

        def fake_grade_token(metric_key, value):
            grade_token_calls.append(metric_key)
            return f"grade-token grade-token--{metric_key}"

        real_grade_scale = grades_mod.grade_scale

        def capture_grade_scale(metric_key):
            grade_scale_calls.append(metric_key)
            return real_grade_scale(metric_key)

        monkeypatch.setattr(grades_mod, "grade_token", fake_grade_token)
        monkeypatch.setattr(grades_mod, "grade_scale", capture_grade_scale)
        app_client.application.jinja_env.globals["grade_scale"] = capture_grade_scale

        resp = app_client.get("/admin/leaderboard/collisions/gap-help")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="Collisions — Gap Help",
            expected_texts=["4", "5", "2", "2", "80.0%", "100.0%"],
        )

        assert 'data-key="totals_collision_pct"' in html
        assert 'data-key="last_collision_pct"' in html
        assert "gap_pct" in grade_token_calls
        assert "collision_pct" not in grade_token_calls
        assert "gap_pct" in grade_scale_calls

    def test_collisions_gap_help_zero_percent_renders(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [{"player_name": "Zero", "plus": 0, "opps": 2}]
        season_totals = {"plus": 0, "opps": 2}

        last_rows = [{"player_name": "Zero", "plus": 0, "opps": 2}]
        last_totals = {"plus": 0, "opps": 2}

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_collisions_gap_help", fake)

        resp = app_client.get("/admin/leaderboard/collisions/gap-help")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        _assert_dual_table_basics(
            html,
            section_title="Collisions — Gap Help",
            expected_texts=["0", "2", "0.0%"],
        )
        assert "0.0%" in html

    def test_atr_fg_pct_dual_view(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [
            ("Player One", 5, 10, 50.0, 30.0),
            ("Player Two", 3, 6, 50.0, 20.0),
        ]
        season_totals = (8, 16, 50.0, 50.0)

        last_rows = [
            ("Player One", 2, 4, 50.0, 25.0),
        ]
        last_totals = (2, 4, 50.0, 25.0)

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setitem(rmod._PRACTICE_DUAL_MAP, "atr_fg_pct", lambda: fake)

        resp = app_client.get(
            "/admin/leaderboard",
            query_string={"season_id": 1, "stat": "atr_fg_pct"},
        )
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        assert "Season Totals" in html
        assert "Last Practice" in html
        assert "Sep 18, 2025" in html
        assert "5–10" in html
        assert "50.0%" in html
        assert html.count("percent-box") >= 2
        assert "grade-token--" in html

    def test_overall_gap_help_leaderboard_block(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [{"player_name": "G", "plus": 8, "opps": 10}]
        season_totals = {"plus": 8, "opps": 10}

        last_rows = [{"player_name": "G", "plus": 3, "opps": 5}]
        last_totals = {"plus": 3, "opps": 5}

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_overall_gap_help", fake)

        resp = app_client.get("/admin/leaderboard?season_id=1&stat=overall_gap_help")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        assert '<option value="overall_gap_help"' in html
        assert 'Overall Gap Help' in html
        for text in ["Gap +", "Gap Opp", "Gap %", "8", "10", "3", "5", "80.0%", "60.0%"]:
            assert text in html

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

    def test_overall_low_man_leaderboard_block(self, monkeypatch, app_client):
        import admin.routes as rmod

        _patch_last_practice(monkeypatch)

        season_rows = [{"player_name": "L", "plus": 7, "opps": 10}]
        season_totals = {"plus": 7, "opps": 10}

        last_rows = [{"player_name": "L", "plus": 4, "opps": 6}]
        last_totals = {"plus": 4, "opps": 6}

        fake = _mk_dual_compute_fake(season_rows, season_totals, last_rows, last_totals)
        monkeypatch.setattr(rmod, "compute_overall_low_man", fake)

        resp = app_client.get("/admin/leaderboard?season_id=1&stat=overall_low_man")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)

        assert '<option value="overall_low_man"' in html
        assert 'Overall Low Man' in html
        for text in ["Low +", "Low Opp", "Low %", "7", "10", "4", "6", "70.0%", "66.7%"]:
            assert text in html
        assert 'data-key="totals_low_pct"' in html
        assert 'data-key="last_low_pct"' in html

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


def test_build_dual_table_includes_grade_tokens():
    from admin._leaderboard_helpers import build_dual_table
    from app.grades import grade_token

    table = build_dual_table(
        base_columns=["FG%"],
        season_rows=[{"player": "Alpha", "plus": 4, "opps": 8, "pct": 50.0}],
        last_rows=[{"player": "Alpha", "plus": 2, "opps": 4, "pct": 50.0}],
        season_totals={"pct": 50.0},
        last_totals={"pct": 50.0},
        column_map={"FG%": {"keys": ("pct",), "format": "pct"}},
        pct_columns=["FG%"],
        grade_metrics={"fg_pct": "fg3_pct"},
    )

    expected_token = grade_token("fg3_pct", 50.0)
    row = table["rows"][0]
    assert row["totals_fg_pct_value"] == 50.0
    assert row["totals_fg_pct_token"] == expected_token
    assert row["last_fg_pct_token"] == expected_token
    totals = table["totals"]
    assert totals["totals_fg_pct_token"] == expected_token
    assert totals["last_fg_pct_token"] == expected_token


def test_collision_percent_wrappers_include_tokens(app_client):
    from admin._leaderboard_helpers import build_dual_table
    from admin.game_leaderboard_config import (
        columns_for,
        column_map_for,
        pct_columns_for,
        percent_specs_for,
        sort_default_for,
        table_id_for,
    )

    table = build_dual_table(
        base_columns=columns_for("collisions"),
        season_rows=[
            {
                "player": "Alpha",
                "jersey": 1,
                "gap_plus": 4,
                "gap_opps": 5,
                "gap_pct": 80.0,
            }
        ],
        last_rows=[
            {
                "player": "Alpha",
                "jersey": 1,
                "gap_plus": 2,
                "gap_opps": 2,
                "gap_pct": 100.0,
            }
        ],
        season_totals={
            "player": "Team Totals",
            "gap_plus": 4,
            "gap_opps": 5,
            "gap_pct": 80.0,
        },
        last_totals={
            "player": "Team Totals",
            "gap_plus": 2,
            "gap_opps": 2,
            "gap_pct": 100.0,
        },
        column_map=column_map_for("collisions"),
        pct_columns=pct_columns_for("collisions"),
        table_id=table_id_for("collisions"),
        default_sort=sort_default_for("collisions"),
    )

    macro = app_client.application.jinja_env.get_template("_macros/percent_box.html").module
    macro.apply_percent_wrappers(table, percent_specs_for("collisions"))

    row = table["rows"][0]
    assert "percent-box" in row["totals_collision_pct"]
    assert "percent-box" in row["last_collision_pct"]
    assert "grade-token" in row["totals_collision_pct"]
    assert "grade-token" in row["last_collision_pct"]
    totals = table["totals"]
    assert totals is not None
    assert "percent-box" in totals["totals_collision_pct"]
    assert "percent-box" in totals["last_collision_pct"]


def test_percent_wrappers_zero_attempts_use_neutral_token(app_client):
    from admin._leaderboard_helpers import build_dual_table
    from admin.game_leaderboard_config import (
        columns_for,
        column_map_for,
        pct_columns_for,
        percent_specs_for,
        sort_default_for,
        table_id_for,
    )

    table = build_dual_table(
        base_columns=columns_for("collisions"),
        season_rows=[
            {
                "player": "Alpha",
                "jersey": 1,
                "gap_plus": 0,
                "gap_opps": 0,
                "gap_pct": 0.0,
            }
        ],
        last_rows=[
            {
                "player": "Alpha",
                "jersey": 1,
                "gap_plus": 0,
                "gap_opps": 0,
                "gap_pct": 0.0,
            }
        ],
        season_totals={
            "player": "Team Totals",
            "gap_plus": 0,
            "gap_opps": 0,
            "gap_pct": 0.0,
        },
        last_totals={
            "player": "Team Totals",
            "gap_plus": 0,
            "gap_opps": 0,
            "gap_pct": 0.0,
        },
        column_map=column_map_for("collisions"),
        pct_columns=pct_columns_for("collisions"),
        table_id=table_id_for("collisions"),
        default_sort=sort_default_for("collisions"),
    )

    for row in table["rows"]:
        row["totals_gap_opps_value"] = 0.0
        row["totals_gap_opp_value"] = 0.0
        row["last_gap_opps_value"] = 0.0
        row["last_gap_opp_value"] = 0.0

    totals_row = table["totals"]
    if totals_row is not None:
        totals_row["totals_gap_opps_value"] = 0.0
        totals_row["totals_gap_opp_value"] = 0.0
        totals_row["last_gap_opps_value"] = 0.0
        totals_row["last_gap_opp_value"] = 0.0

    macro = app_client.application.jinja_env.get_template("_macros/percent_box.html").module
    macro.apply_percent_wrappers(table, percent_specs_for("collisions"))

    row = table["rows"][0]
    assert "grade-NA" in row["totals_collision_pct"]
    assert "grade-token--0" not in row["totals_collision_pct"]
    assert "grade-NA" in row["last_collision_pct"]
    assert "grade-token--0" not in row["last_collision_pct"]

    totals = table["totals"]
    assert totals is not None
    assert "grade-NA" in totals["totals_collision_pct"]
    assert "grade-token--0" not in totals["totals_collision_pct"]
    assert "grade-NA" in totals["last_collision_pct"]
    assert "grade-token--0" not in totals["last_collision_pct"]


def test_low_man_percent_wrappers_include_tokens(app_client):
    from admin._leaderboard_helpers import build_dual_table
    from admin.game_leaderboard_config import (
        columns_for,
        column_map_for,
        pct_columns_for,
        percent_specs_for,
        sort_default_for,
        table_id_for,
    )

    table = build_dual_table(
        base_columns=columns_for("overall_low_man"),
        season_rows=[
            {
                "player": "Beta",
                "jersey": 2,
                "low_plus": 7,
                "low_opps": 10,
                "low_pct": 70.0,
            }
        ],
        last_rows=[
            {
                "player": "Beta",
                "jersey": 2,
                "low_plus": 4,
                "low_opps": 6,
                "low_pct": 66.7,
            }
        ],
        season_totals={
            "player": "Team Totals",
            "low_plus": 7,
            "low_opps": 10,
            "low_pct": 70.0,
        },
        last_totals={
            "player": "Team Totals",
            "low_plus": 4,
            "low_opps": 6,
            "low_pct": 66.7,
        },
        column_map=column_map_for("overall_low_man"),
        pct_columns=pct_columns_for("overall_low_man"),
        table_id=table_id_for("overall_low_man"),
        default_sort=sort_default_for("overall_low_man"),
    )

    macro = app_client.application.jinja_env.get_template("_macros/percent_box.html").module
    macro.apply_percent_wrappers(table, percent_specs_for("overall_low_man"))

    row = table["rows"][0]
    assert "percent-box" in row["totals_low_man_pct"]
    assert "percent-box" in row["last_low_man_pct"]
    assert "grade-token" in row["totals_low_man_pct"]
    assert "grade-token" in row["last_low_man_pct"]
    totals = table["totals"]
    assert totals is not None
    assert "percent-box" in totals["totals_low_man_pct"]
    assert "percent-box" in totals["last_low_man_pct"]


def test_split_dual_table_restores_rank_and_player_headers():
    from admin._leaderboard_helpers import build_dual_table, split_dual_table
    from admin.game_leaderboard_config import (
        columns_for,
        column_map_for,
        pct_columns_for,
    )

    table = build_dual_table(
        base_columns=columns_for("shrinks_offense"),
        season_rows=[
            {
                "player": "Alpha",
                "jersey": 1,
                "FG": "1-3",
                "FG%": "33.3%",
                "Shrink 3FG": "1-2",
                "Shrink 3FG %": "50.0%",
                "Shrink 3FG Freq": "40.0%",
                "Non-Shrink 3FG": "0-1",
                "Non-Shrink 3FG %": "0.0%",
                "Non-Shrink 3FG Freq": "60.0%",
            }
        ],
        last_rows=[
            {
                "player": "Alpha",
                "jersey": 1,
                "FG": "0-1",
                "FG%": "0.0%",
                "Shrink 3FG": "0-1",
                "Shrink 3FG %": "0.0%",
                "Shrink 3FG Freq": "50.0%",
                "Non-Shrink 3FG": "0-0",
                "Non-Shrink 3FG %": "0.0%",
                "Non-Shrink 3FG Freq": "50.0%",
            }
        ],
        season_totals={"player": "Team"},
        last_totals={"player": "Team"},
        column_map=column_map_for("shrinks_offense"),
        pct_columns=pct_columns_for("shrinks_offense"),
        left_label="Season Shrink 3's",
        right_label="Last Game Shrink 3's",
        totals_label="Team Totals",
        table_id="shrink-test",
    )

    season_table = split_dual_table(table, prefix="totals_", table_id_suffix="season")
    last_table = split_dual_table(table, prefix="last_", table_id_suffix="last")

    def _header_flags(split_table):
        rank_col = next(col for col in split_table["columns"] if col["key"] == "rank")
        player_col = next(col for col in split_table["columns"] if col["key"] == "player")
        return rank_col.get("render_header", True), player_col.get("render_header", True)

    rank_flag, player_flag = _header_flags(season_table)
    assert rank_flag is not False
    assert player_flag is not False

    rank_flag, player_flag = _header_flags(last_table)
    assert rank_flag is not False
    assert player_flag is not False


def test_percent_box_prefers_cached_token():
    test_app = app_module.create_app()
    with test_app.app_context():
        markup = render_template_string(
            """
            {% from '_macros/percent_box.html' import percent_box %}
            {{ percent_box('fg3_pct', 40, token='grade-token grade-token--1') }}
            """
        )

    assert "grade-token--1" in markup
    assert "grade-token--6" not in markup
