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

    row_map = {row["player_name"]: row for row in rows}
    player_a = row_map["Player A"]
    player_b = row_map["Player B"]

    assert player_a["plus"] == 5
    assert player_a["opps"] == 7
    expected_pct = (5 / 7) * 100
    assert player_a["pct"] == pytest.approx(expected_pct)

    # Player B should reflect only the collision totals since no PnR stats were provided.
    assert player_b["plus"] == 2
    assert player_b["opps"] == 2
    assert player_b["pct"] == pytest.approx(100.0)


def test_compute_overall_low_man_handles_missing_collisions(monkeypatch):
    def fake_collisions(**kwargs):
        rows = [("Player A", 4, 6, 0.0, 0, 0, None)]
        totals = (4, 6, 0.0, 0, 0, None)
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


def test_overall_gap_help_stat_key_includes_all_sources(monkeypatch):
    captured = {}

    def fake_collisions(**kwargs):
        captured["collisions_kwargs"] = kwargs
        rows = [
            ("Crimson", 3, 5, 0.0),
            ("White", 2, 4, 0.0),
        ]
        totals = {
            "Crimson": {"gap_plus": 3, "gap_opp": 5},
            "White": {"gap_plus": 2, "gap_opp": 4},
            "gap_plus": 5,
            "gap_opp": 9,
        }
        return totals, rows

    def fake_pnr(**kwargs):
        captured["pnr_kwargs"] = kwargs
        rows = [
            {"player_name": "PnR Helper", "plus": 4, "opps": 6, "pct": 0.0},
        ]
        totals = {"plus": 4, "opps": 6, "pct": 0.0}
        return totals, rows

    monkeypatch.setattr(routes, "compute_collisions_gap_help", fake_collisions)
    monkeypatch.setattr(routes, "compute_pnr_gap_help", fake_pnr)

    totals, rows = routes.compute_overall_gap_help(
        season_id=1,
        stat_key="overall_gap_help",
    )

    assert "stat_key" not in captured["collisions_kwargs"]
    assert "stat_key" not in captured["pnr_kwargs"]

    assert totals["plus"] == 9
    assert totals["opps"] == 15
    assert totals["pct"] == pytest.approx((9 / 15) * 100)

    names = {row["player_name"] for row in rows}
    assert names == {"Crimson", "White", "PnR Helper"}


def test_overall_low_man_stat_key_includes_all_sources(monkeypatch):
    captured = {}

    def fake_collisions(**kwargs):
        captured["collisions_kwargs"] = kwargs
        rows = [
            ("Crimson", 0, 0, 0.0, 1, 2, 0.0),
            ("White", 0, 0, 0.0, 2, 3, 0.0),
        ]
        totals = {
            "Crimson": {"low_plus": 1, "low_opp": 2},
            "White": {"low_plus": 2, "low_opp": 3},
            "low_plus": 3,
            "low_opp": 5,
        }
        return totals, rows

    def fake_pnr(**kwargs):
        captured["pnr_kwargs"] = kwargs
        rows = [
            {"player_name": "PnR Low Man", "plus": 3, "opps": 4, "pct": 0.0},
        ]
        totals = {"plus": 3, "opps": 4, "pct": 0.0}
        return totals, rows

    monkeypatch.setattr(routes, "compute_collisions_gap_help", fake_collisions)
    monkeypatch.setattr(routes, "compute_pnr_gap_help", fake_pnr)

    totals, rows = routes.compute_overall_low_man(
        season_id=1,
        stat_key="overall_low_man",
    )

    assert "stat_key" not in captured["collisions_kwargs"]
    assert captured["pnr_kwargs"].get("role") == "low_man"
    assert "stat_key" not in captured["pnr_kwargs"]

    assert totals["plus"] == 6
    assert totals["opps"] == 9
    assert totals["pct"] == pytest.approx((6 / 9) * 100)

    names = {row["player_name"] for row in rows}
    assert names == {"Crimson", "White", "PnR Low Man"}


def test_compute_overall_requires_season_id():
    assert routes.compute_overall_gap_help(season_id=None) == (None, [])
    assert routes.compute_overall_low_man(season_id=None) == (None, [])
