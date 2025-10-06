from __future__ import annotations

from decimal import Decimal

import pytest

from services import leaderboard_cache as lc


@pytest.fixture(autouse=True)
def _reset_cache(monkeypatch):
    monkeypatch.setattr(lc, "cache", lc._InMemoryCache())


def test_build_leaderboard_cache_formats_rows(monkeypatch):
    def fake_query(stat_key: str, season_id: int):
        assert stat_key == "points"
        assert season_id == 1
        return [
            {"player_number": 2, "player_name": "Aden Holloway", "value": Decimal("838")},
            {"player_number": 10, "player_name": "Mark Sears", "value": 702.0},
        ]

    monkeypatch.setattr(lc, "query_stat_rows", fake_query)

    payload = lc.build_leaderboard_cache("points", 1)

    assert payload["schema_version"] == lc.SCHEMA_VERSION
    assert payload["stat_key"] == "points"
    assert payload["season_id"] == 1
    rows = payload["rows"]
    assert rows[0]["rank"] == "1"
    assert rows[0]["player"] == "#2 Aden Holloway"
    assert rows[0]["value"] == "838"
    assert rows[0]["value_sort"] == pytest.approx(838.0)
    assert rows[1]["value"] == "702"


def test_format_stat_value_percent_and_rate():
    assert lc.format_stat_value("fg3_pct", 37.44) == "37.4%"
    assert lc.format_stat_value("turnover_rate", Decimal("18")) == "18.0%"
    assert lc.format_stat_value("ppp_on", Decimal("1.267")) == "1.3"
    assert lc.format_stat_value("points", 231.0) == "231"


def test_get_leaderboard_payload_invalidates_old_schema(monkeypatch):
    rows = [
        {"player_number": 5, "player_name": "Shooter", "value": 100},
    ]

    monkeypatch.setattr(lc, "query_stat_rows", lambda *args, **kwargs: rows)

    # Seed an old schema cache entry
    old_key = "leaderboard:1:points"
    lc.cache.set(old_key, {"schema_version": 1, "rows": []})

    payload = lc.get_leaderboard_payload("points", 1)

    assert payload["schema_version"] == lc.SCHEMA_VERSION
    assert lc.cache.get(old_key) is None
    assert lc.cache.get(f"leaderboard:{lc.SCHEMA_VERSION}:1:points") == payload
