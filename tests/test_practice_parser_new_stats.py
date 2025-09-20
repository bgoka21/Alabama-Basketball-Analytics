import os
from datetime import date

import pytest
from flask import Flask

from models.database import db, Season, Practice, Roster, PlayerStats
from models.user import User  # ensure users table exists for FK references
from parse_practice_csv import parse_practice_csv


FIELDS = [
    "crash_positive", "crash_missed", "back_man_positive", "back_man_missed",
    "box_out_positive", "box_out_missed", "off_reb_given_up",
    "collision_gap_positive", "collision_gap_missed",
    "pnr_gap_positive", "pnr_gap_missed",
    "low_help_positive", "low_help_missed",
    "close_window_positive", "close_window_missed",
    "shut_door_positive", "shut_door_missed",
]


def load_fixture_path(name):
    return os.path.join(os.path.dirname(__file__), "fixtures", name)


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["TESTING"] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


@pytest.mark.parametrize("fixture", ["practice_new_stats.csv"])
def test_parser_counts_for_new_stats(app, fixture):
    path = load_fixture_path(fixture)
    practice_date = date(2024, 1, 2)

    with app.app_context():
        season = Season(id=1, season_name="2024", start_date=practice_date)
        db.session.add(season)
        practice = Practice(id=1, season_id=1, date=practice_date, category="Official Practices")
        db.session.add(practice)
        roster_entries = [
            Roster(id=1, season_id=1, player_name="#1 Alice"),
            Roster(id=2, season_id=1, player_name="#2 Bob"),
            Roster(id=3, season_id=1, player_name="#3 Carol"),
        ]
        db.session.add_all(roster_entries)
        db.session.commit()

        parse_practice_csv(path, season_id=1, category="Official Practices", file_date=practice_date)

        rows = {ps.player_name: ps for ps in PlayerStats.query.order_by(PlayerStats.player_name).all()}

        assert "#1 Alice" in rows
        assert "#2 Bob" in rows
        assert "#3 Carol" in rows

        def slot(name_fragment):
            for key, value in rows.items():
                if name_fragment in key:
                    return value
            raise AssertionError(f"Player not found in parsed data: {name_fragment}")

        alice = slot("Alice")
        bob = slot("Bob")
        carol = slot("Carol")

        for player in (alice, bob, carol):
            for field in FIELDS:
                assert hasattr(player, field), f"Missing field {field} on player stats"

        assert alice.crash_positive == 1
        assert alice.crash_missed == 0
        assert alice.back_man_positive == 0
        assert alice.back_man_missed == 0
        assert alice.box_out_positive == 1
        assert alice.box_out_missed == 0
        assert alice.off_reb_given_up == 0
        assert alice.collision_gap_positive == 1
        assert alice.collision_gap_missed == 1
        assert alice.pnr_gap_positive == 1
        assert alice.pnr_gap_missed == 0
        assert alice.low_help_positive == 1
        assert alice.low_help_missed == 0
        assert alice.close_window_positive == 1
        assert alice.close_window_missed == 0
        assert alice.shut_door_positive == 0
        assert alice.shut_door_missed == 1

        assert bob.crash_positive == 0
        assert bob.crash_missed == 1
        assert bob.back_man_positive == 0
        assert bob.back_man_missed == 0
        assert bob.box_out_positive == 0
        assert bob.box_out_missed == 1
        assert bob.off_reb_given_up == 1
        assert bob.collision_gap_positive == 1
        assert bob.collision_gap_missed == 1
        assert bob.pnr_gap_positive == 0
        assert bob.pnr_gap_missed == 1
        assert bob.low_help_positive == 0
        assert bob.low_help_missed == 1
        assert bob.close_window_positive == 0
        assert bob.close_window_missed == 1
        assert bob.shut_door_positive == 1
        assert bob.shut_door_missed == 0

        assert carol.crash_positive == 1
        assert carol.crash_missed == 1
        assert carol.back_man_positive == 1
        assert carol.back_man_missed == 1
        assert carol.box_out_positive == 0
        assert carol.box_out_missed == 0
        assert carol.off_reb_given_up == 1
        assert carol.collision_gap_positive == 1
        assert carol.collision_gap_missed == 0
        assert carol.pnr_gap_positive == 0
        assert carol.pnr_gap_missed == 0
        assert carol.low_help_positive == 1
        assert carol.low_help_missed == 0
        assert carol.close_window_positive == 1
        assert carol.close_window_missed == 0
        assert carol.shut_door_positive == 1
        assert carol.shut_door_missed == 0
