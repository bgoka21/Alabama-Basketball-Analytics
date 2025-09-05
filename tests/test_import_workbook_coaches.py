import os
from tempfile import NamedTemporaryFile
import os
from tempfile import NamedTemporaryFile

import pandas as pd
import pytest
from flask import Flask

os.environ.setdefault("SKIP_CREATE_ALL", "1")

from models.database import db

from app.services.draft_stock_importer import import_workbook
from app.models.coach import Coach
from recruits.routes import _get_coach_names


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["TESTING"] = True
    db.init_app(app)
    with app.app_context():
        from app.models import prospect as _p, coach as _c  # noqa: F401
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def _write_workbook(coach_df: pd.DataFrame, prospect_df: pd.DataFrame) -> str:
    tmp = NamedTemporaryFile(suffix=".xlsx", delete=False)
    tmp.close()
    with pd.ExcelWriter(tmp.name) as writer:
        coach_df.to_excel(writer, sheet_name="COACHES ", index=False)
        prospect_df.to_excel(writer, sheet_name="Prospects", index=False)
    return tmp.name


def test_coaches_sheet_detection_and_union(app):
    with app.app_context():
        coach_df = pd.DataFrame(
            [
                {"coach": "Coach A", "current_team": "T1", "current_conference": "ConfA"},
                {"coach": "Coach B", "current_team": "T2", "current_conference": "ConfB"},
            ]
        )
        prospect_df = pd.DataFrame(
            [
                {"coach": "Coach B", "player": "P1", "team": "Team1", "year": 2024},
                {"coach": "Coach C", "player": "P2", "team": "Team2", "year": 2024},
            ]
        )
        path = _write_workbook(coach_df, prospect_df)
        try:
            import_workbook(path)
        finally:
            os.remove(path)

        names = [c.name for c in Coach.query.order_by(Coach.name.asc()).all()]
        assert names == ["Coach A", "Coach B", "Coach C"]

        assert _get_coach_names() == ["Coach A", "Coach B", "Coach C"]
