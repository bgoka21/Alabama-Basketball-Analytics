import pytest

import admin.routes as routes


def test_compute_overall_gap_help_combines_sources(monkeypatch):
    def fake_collisions(**kwargs):
        rows = [
            ("Player A", 4, 6, 0.0),
            ("Player B", 2, 3, 0.0),
        ]
        totals = (6, 9, 0.0)
        return totals, rows

    def fake_pnr(**kwargs):
        rows = [
            {"player_name": "Player A", "plus": 3, "opps": 5, "pct": 0.0},
            {"player_name": "Player C", "plus": 1, "opps": 2, "pct": 0.0},
        ]
        totals = {"plus": 4, "opps": 7, "pct": 0.0}
        return totals, rows

    monkeypatch.setattr(routes, "compute_collisions_gap_help", fake_collisions)
    monkeypatch.setattr(routes, "compute_pnr_gap_help", fake_pnr)

    totals, rows = routes.compute_overall_gap_help(season_id=1)

    assert totals["plus"] == 10
    assert totals["opps"] == 16
    assert totals["pct"] == pytest.approx(62.5)

    assert [row["player_name"] for row in rows] == ["Player A", "Player B", "Player C"]
    assert rows[0]["plus"] == 7
    assert rows[0]["opps"] == 11
    expected_pct = (7 / 11) * 100
    assert rows[0]["pct"] == pytest.approx(expected_pct)


def test_compute_overall_gap_help_single_source(monkeypatch):
    def fake_collisions(**kwargs):
        return None, []

    def fake_pnr(**kwargs):
        rows = [{"player_name": "Only", "plus": 5, "opps": 10, "pct": 50.0}]
        totals = {"plus": 5, "opps": 10, "pct": 50.0}
        return totals, rows

    monkeypatch.setattr(routes, "compute_collisions_gap_help", fake_collisions)
    monkeypatch.setattr(routes, "compute_pnr_gap_help", fake_pnr)

    totals, rows = routes.compute_overall_gap_help(season_id=1)

    assert totals == {"plus": 5, "opps": 10, "pct": 50.0}
    assert rows == [{"player_name": "Only", "plus": 5, "opps": 10, "pct": 50.0}]


def test_compute_overall_low_man_combines_sources(monkeypatch):
    def fake_collisions(**kwargs):
        rows = [
            ("Player A", 4, 6, 0.0, 2, 3, 0.0),
            ("Player B", 2, 3, 0.0, 2, 2, 0.0),
        ]
        totals = (6, 9, 0.0, 4, 5, 0.0)
        return totals, rows

    captured = {}

    def fake_pnr(**kwargs):
        captured["role"] = kwargs.get("role")
        rows = [
            {"player_name": "Player A", "plus": 3, "opps": 4, "pct": 0.0},
            {"player_name": "Player C", "plus": 2, "opps": 3, "pct": 0.0},
        ]
        totals = {"plus": 5, "opps": 7, "pct": 0.0}
        return totals, rows

    monkeypatch.setattr(routes, "compute_collisions_gap_help", fake_collisions)
    monkeypatch.setattr(routes, "compute_pnr_gap_help", fake_pnr)

    totals, rows = routes.compute_overall_low_man(season_id=1)

    assert captured["role"] == "low_man"
    assert totals["plus"] == 9
    assert totals["opps"] == 12
    assert totals["pct"] == pytest.approx(75.0)

    assert [row["player_name"] for row in rows] == ["Player A", "Player C", "Player B"]
    assert rows[0]["plus"] == 5
    assert rows[0]["opps"] == 7
    expected_pct = (5 / 7) * 100
    assert rows[0]["pct"] == pytest.approx(expected_pct)


def test_compute_overall_low_man_handles_missing_collisions(monkeypatch):
    def fake_collisions(**kwargs):
        rows = [("Player A", 4, 6, 0.0)]
        totals = (4, 6, 0.0)
        return totals, rows

    def fake_pnr(**kwargs):
        rows = [{"player_name": "Player A", "plus": 1, "opps": 2, "pct": 0.0}]
        totals = {"plus": 1, "opps": 2, "pct": 0.0}
        return totals, rows

    monkeypatch.setattr(routes, "compute_collisions_gap_help", fake_collisions)
    monkeypatch.setattr(routes, "compute_pnr_gap_help", fake_pnr)

    totals, rows = routes.compute_overall_low_man(season_id=1)

    assert totals["plus"] == 1
    assert totals["opps"] == 2
    assert rows == [{"player_name": "Player A", "plus": 1, "opps": 2, "pct": 50.0}]


def test_compute_overall_requires_season_id():
    assert routes.compute_overall_gap_help(season_id=None) == (None, [])
    assert routes.compute_overall_low_man(season_id=None) == (None, [])
