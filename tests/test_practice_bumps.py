from datetime import date
from pathlib import Path
import re

import pandas as pd
import pytest
from flask import Flask

from app.services.csv_tokens import count_bump_tokens_in_cells
from models.database import db, Season, Practice, PlayerStats, Roster
from parse_practice_csv import parse_practice_csv


PLAYER_COL_RE = re.compile(r"^#\d+\s+\S+")

CSV_PATH = Path("tests/data/practice/25_09_18 Fall Workout #15.csv")


def _is_player_col(col: str) -> bool:
    return bool(col) and bool(PLAYER_COL_RE.match(str(col)))


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


@pytest.mark.skipif(not CSV_PATH.exists(), reason="practice CSV not present locally")
def test_practice_bumps_parsing_matches_csv_counts(app):
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)
    player_cols = [c for c in df.columns if _is_player_col(c)]

    expected_plus_total = 0
    expected_minus_total = 0
    per_player_expected: dict[str, tuple[int, int]] = {}

    for pcol in player_cols:
        series = df[pcol].astype(str).fillna("")
        p_plus, p_minus = count_bump_tokens_in_cells(series.tolist())
        if p_plus or p_minus:
            per_player_expected[pcol] = (p_plus, p_minus)
            expected_plus_total += p_plus
            expected_minus_total += p_minus

    expected_opps_total = expected_plus_total + expected_minus_total
    assert (
        expected_opps_total >= 2
    ), f"Expected at least 2 bump opps from CSV, got {expected_opps_total}"

    with app.app_context():
        season = Season(id=1, season_name="Test 2025", start_date=date(2025, 1, 1))
        db.session.add(season)
        for pcol in player_cols:
            db.session.add(Roster(season_id=season.id, player_name=pcol))
        practice_date = date(2025, 9, 18)
        practice = Practice(
            id=1,
            season_id=season.id,
            date=practice_date,
            category="Fall Workouts",
        )
        db.session.add(practice)
        db.session.commit()

        parse_practice_csv(
            str(CSV_PATH),
            season_id=season.id,
            category="Fall Workouts",
            file_date=practice_date,
        )

        stats_rows = PlayerStats.query.filter_by(practice_id=practice.id).all()
        assert stats_rows, "No PlayerStats rows created by parser."

        db_plus_total = 0
        db_minus_total = 0
        for row in stats_rows:
            bump_plus = row.bump_positive or 0
            bump_minus = row.bump_missed or 0
            db_plus_total += bump_plus
            db_minus_total += bump_minus

            if row.player_name in per_player_expected:
                exp_plus, exp_minus = per_player_expected[row.player_name]
                assert (
                    bump_plus == exp_plus
                ), f"{row.player_name} bump_plus mismatch: db={bump_plus} csv={exp_plus}"
                assert (
                    bump_minus == exp_minus
                ), f"{row.player_name} bump_minus mismatch: db={bump_minus} csv={exp_minus}"

        assert (
            db_plus_total == expected_plus_total
        ), f"Total bump_plus mismatch: db={db_plus_total} csv={expected_plus_total}"
        assert (
            db_minus_total == expected_minus_total
        ), f"Total bump_minus mismatch: db={db_minus_total} csv={expected_minus_total}"
        assert db_plus_total + db_minus_total == expected_opps_total
