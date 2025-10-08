import json
from datetime import date

import pytest
from flask import Flask

from models.database import CachedLeaderboard, Season, db
from services.cache_leaderboard import (
    cache_build_all,
    cache_build_one,
    cache_get_leaderboard,
)
from services.leaderboard_cache import FORMATTER_VERSION, SCHEMA_VERSION


def _dummy_builder(stat_key: str, season_id: int):
    return {
        "config": {"key": stat_key, "label": stat_key.title(), "format": "int"},
        "rows": [("#0 Test Player", 5)],
        "team_totals": {stat_key: 5},
    }


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(
        SQLALCHEMY_DATABASE_URI="sqlite:///:memory:",
        SECRET_KEY="test",
        TESTING=True,
    )
    db.init_app(app)

    with app.app_context():
        db.create_all()
        db.session.add(Season(id=42, season_name="Test", start_date=date(2024, 1, 1)))
        db.session.commit()

    yield app

    with app.app_context():
        db.drop_all()


def test_cache_build_one_persists_payload(app):
    with app.app_context():
        payload = cache_build_one("points", 42, _dummy_builder)
        entry = CachedLeaderboard.query.filter_by(season_id=42, stat_key="points").first()
        assert entry is not None
        stored = json.loads(entry.payload_json)
        assert stored["stat_key"] == "points"
        assert stored["rows"]
        cached = cache_get_leaderboard(42, "points")
        assert cached["stat_key"] == payload["stat_key"]


def test_cache_build_all_handles_multiple_keys(app):
    with app.app_context():
        payloads = cache_build_all(42, compute_fn=_dummy_builder, stat_keys=["points", "assists"])
        assert set(payloads) == {"points", "assists"}
        cached_points = cache_get_leaderboard(42, "points")
        assert cached_points is not None
        cached_assists = cache_get_leaderboard(42, "assists")
        assert cached_assists is not None


def test_cache_get_leaderboard_rebuilds_stale_payload(app, monkeypatch):
    with app.app_context():
        cache_build_one("points", 42, _dummy_builder)
        entry = CachedLeaderboard.query.filter_by(season_id=42, stat_key="points").first()
        assert entry is not None

        stored = json.loads(entry.payload_json)
        stored["schema_version"] = 0
        stored["formatter_version"] = 0
        entry.schema_version = 0
        entry.formatter_version = 0
        entry.payload_json = json.dumps(stored)
        db.session.commit()

        calls: list[str] = []

        def _fake_import():
            calls.append("called")
            return _dummy_builder

        monkeypatch.setattr(
            "services.cache_leaderboard._import_compute_leaderboard",
            _fake_import,
        )

        cached = cache_get_leaderboard(42, "points")
        assert calls, "schedule_refresh should rebuild stale payloads"
        assert cached is not None
        assert int(cached.get("schema_version")) == SCHEMA_VERSION
        assert int(cached.get("formatter_version")) == FORMATTER_VERSION
