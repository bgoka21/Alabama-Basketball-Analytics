import os
from tempfile import NamedTemporaryFile

import pandas as pd
import pytest
from flask import Flask

os.environ.setdefault("SKIP_CREATE_ALL", "1")

from models.database import db

from app.services.draft_stock_importer import import_workbook
from app.models.prospect import Prospect


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["TESTING"] = True
    db.init_app(app)
    with app.app_context():
        # Ensure tables are registered
        from app.models import prospect as _p, coach as _c  # noqa: F401
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def _write_workbook(df: pd.DataFrame) -> str:
    tmp = NamedTemporaryFile(suffix=".xlsx", delete=False)
    tmp.close()
    df.to_excel(tmp.name, index=False)
    return tmp.name


def test_replace_removes_existing_rows(app):
    with app.app_context():
        df1 = pd.DataFrame([
            {"coach": "CoachA", "player": "P1", "team": "T1", "year": 2024}
        ])
        p1 = _write_workbook(df1)
        try:
            import_workbook(p1)
        finally:
            os.remove(p1)
        assert Prospect.query.count() == 1

        df2 = pd.DataFrame([
            {"coach": "CoachB", "player": "P2", "team": "T2", "year": 2024}
        ])
        p2 = _write_workbook(df2)
        try:
            import_workbook(p2, replace=True)
        finally:
            os.remove(p2)

        players = [p.player for p in Prospect.query.all()]
        assert players == ["P2"]
