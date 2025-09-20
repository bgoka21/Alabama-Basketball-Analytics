from datetime import date, datetime, timedelta

import pytest
from flask import Flask

from models.database import db, Practice, Season
from app.services.last_practice import (
    get_last_practice,
    get_last_practice_date,
    normalize_practice_date,
)


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


@pytest.fixture
def app_context(app):
    with app.app_context():
        yield


@pytest.mark.usefixtures("app_context")
class TestLastPracticeSelector:
    def test_returns_none_when_no_practices(self):
        season = Season(season_name="Test Season 2025", start_date=date(2025, 1, 1))
        db.session.add(season)
        db.session.commit()

        assert get_last_practice(season.id) is None
        assert get_last_practice_date(season.id) is None

    def test_orders_by_date_desc(self):
        season = Season(season_name="Test Season 2025", start_date=date(2025, 1, 1))
        db.session.add(season)
        db.session.flush()

        p1 = Practice(season_id=season.id, date=date(2025, 9, 18), category="P1")
        p2 = Practice(season_id=season.id, date=date(2025, 9, 19), category="P2")
        p3 = Practice(season_id=season.id, date=date(2025, 9, 17), category="P3")
        db.session.add_all([p1, p2, p3])
        db.session.commit()

        last_p = get_last_practice(season.id)
        assert last_p is not None
        assert last_p.date == date(2025, 9, 19)
        assert get_last_practice_date(season.id) == date(2025, 9, 19)

    def test_tie_breaks_by_created_at_desc(self):
        season = Season(season_name="Tie Season", start_date=date(2025, 1, 1))
        db.session.add(season)
        db.session.flush()

        common_date = date(2025, 9, 20)
        t1 = datetime(2025, 9, 20, 9, 0, 0)
        t2 = t1 + timedelta(hours=3)

        earlier = Practice(
            season_id=season.id,
            date=common_date,
            category="Earlier",
            created_at=t1,
        )
        later = Practice(
            season_id=season.id,
            date=common_date,
            category="Later",
            created_at=t2,
        )
        db.session.add_all([earlier, later])
        db.session.commit()

        last_p = get_last_practice(season.id)
        assert last_p is not None
        assert last_p.created_at == t2
        assert get_last_practice_date(season.id) == common_date

    def test_fallback_order_by_id_when_no_created_at(self):
        season = Season(season_name="Legacy Season", start_date=date(2025, 1, 1))
        db.session.add(season)
        db.session.flush()

        common_date = date(2025, 9, 21)

        first = Practice(
            season_id=season.id,
            date=common_date,
            category="Legacy1",
            created_at=None,
        )
        db.session.add(first)
        db.session.flush()

        second = Practice(
            season_id=season.id,
            date=common_date,
            category="Legacy2",
            created_at=None,
        )
        db.session.add(second)
        db.session.commit()

        last_p = get_last_practice(season.id)
        assert last_p is not None
        assert last_p.id == second.id

    def test_normalize_practice_date_sets_candidate_when_missing(self):
        season = Season(season_name="Normalize Season", start_date=date(2025, 1, 1))
        db.session.add(season)
        db.session.flush()

        practice = Practice(season_id=season.id, category="NoDateYet", date=None)
        db.session.add(practice)
        db.session.flush()

        target = date(2025, 9, 18)
        normalize_practice_date(practice, candidate_date=target)
        db.session.commit()

        assert practice.date == target

    def test_normalize_practice_date_does_not_override_existing(self):
        season = Season(season_name="Normalize Season 2", start_date=date(2025, 1, 1))
        db.session.add(season)
        db.session.flush()

        original = date(2025, 9, 15)
        practice = Practice(season_id=season.id, category="HasDate", date=original)
        db.session.add(practice)
        db.session.flush()

        normalize_practice_date(practice, candidate_date=date(2025, 9, 18))
        db.session.commit()

        assert practice.date == original
