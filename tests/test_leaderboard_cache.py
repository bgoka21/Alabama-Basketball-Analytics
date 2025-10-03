import json

import pytest
from flask import Flask

from admin.routes import compute_leaderboard
from app import cache
from models.database import db, PlayerStats, Roster, Season
from utils import cache_utils
from utils.cache_utils import (
    LEADERBOARD_REGISTRY_KEY,
    invalidate_leaderboard_cache,
)
from utils.shottype import persist_player_shot_details


class DummyCache:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def delete(self, key):
        self.store.pop(key, None)

    def clear(self):
        self.store.clear()


@pytest.fixture
def cached_app(monkeypatch):
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["TESTING"] = True
    app.config["CACHE_TYPE"] = "SimpleCache"
    app.config["CACHE_DEFAULT_TIMEOUT"] = 60

    cache.init_app(app)
    db.init_app(app)
    dummy_cache = DummyCache()
    monkeypatch.setattr("admin.routes.get_cache", lambda: dummy_cache)
    monkeypatch.setattr(cache_utils, "get_cache", lambda: dummy_cache)

    shots = [
        {
            "shot_class": "3fg",
            "result": "made",
            "drill_labels": ["Special"],
        },
        {
            "shot_class": "3fg",
            "result": "miss",
            "drill_labels": ["Special"],
        },
        {
            "shot_class": "3fg",
            "result": "made",
            "drill_labels": ["Other"],
        },
        {
            "shot_class": "3fg",
            "result": "miss",
            "drill_labels": ["Other"],
        },
    ]

    with app.app_context():
        db.create_all()
        db.session.add(Season(id=1, season_name="2024", start_date=None))
        db.session.add(Roster(id=1, season_id=1, player_name="Test Player"))
        player_stat = PlayerStats(
            player_name="Test Player",
            season_id=1,
            practice_id=None,
            game_id=None,
            fg3_attempts=4,
            fg3_makes=2,
            shot_type_details=json.dumps(shots),
            stat_details=json.dumps(shots),
        )
        db.session.add(player_stat)
        persist_player_shot_details(player_stat, shots, replace=True)
        db.session.commit()

    yield app, dummy_cache

    with app.app_context():
        db.drop_all()
    dummy_cache.clear()


def test_leaderboard_cache_and_invalidation(cached_app):
    app, dummy_cache = cached_app

    with app.app_context():
        first_result = compute_leaderboard("fg3_fg_pct", season_id=1)

        stats = PlayerStats.query.filter_by(player_name="Test Player").first()
        stats.fg3_makes = 3
        stats.fg3_attempts = 5
        db.session.commit()

        second_result = compute_leaderboard("fg3_fg_pct", season_id=1)
        assert second_result is first_result

        invalidate_leaderboard_cache(1, stat_key="fg3_fg_pct")
        cached_keys = [k for k in dummy_cache.store if k != LEADERBOARD_REGISTRY_KEY]
        assert not cached_keys

        third_result = compute_leaderboard("fg3_fg_pct", season_id=1)
        assert third_result is not first_result


def test_label_set_normalization_reuses_cache(cached_app):
    app, dummy_cache = cached_app

    with app.app_context():
        first_result = compute_leaderboard("fg3_fg_pct", season_id=1, label_set={"Special"})

        second_result = compute_leaderboard("fg3_fg_pct", season_id=1, label_set={"special"})
        assert second_result is first_result

        invalidate_leaderboard_cache(1, stat_key="fg3_fg_pct", label_set={"SPECIAL"})

        third_result = compute_leaderboard("fg3_fg_pct", season_id=1, label_set={"Special"})
        assert third_result is not first_result
