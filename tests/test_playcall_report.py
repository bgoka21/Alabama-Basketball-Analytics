import copy
import importlib
import sys
from datetime import date

import pytest

import app as app_module
from app import db
from models.database import Game, Season
from services.reports import playcall as playcall_service


def _make_play_payload(ran, off_set_pts, off_set_chances, in_flow_pts, in_flow_chances):
    return {
        "ran": ran,
        "off_set": {"pts": off_set_pts, "chances": off_set_chances, "ppc": 0.0},
        "in_flow": {"pts": in_flow_pts, "chances": in_flow_chances, "ppc": 0.0},
    }


@pytest.fixture
def sample_game_payloads():
    game_one = {
        "series": {
            "HORNS": {
                "plays": {
                    "Horns Flex": _make_play_payload(2, 4, 2, 3, 1),
                },
                "totals": {
                    "off_set": {"pts": 4, "chances": 2, "ppc": 2.0},
                    "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                },
            },
            "FLOW": {
                "plays": [
                    {
                        "playcall": "Horns Flex",
                        "ran_in_flow": 1,
                        "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                    },
                    {
                        "playcall": "FLOW",
                        "ran_in_flow": 1,
                        "in_flow": {"pts": 2, "chances": 1, "ppc": 2.0},
                    },
                ],
                "totals": {"in_flow": {"pts": 5, "chances": 2, "ppc": 2.5}},
            },
        },
        "meta": {"total_chances_off_set": 2, "total_chances_in_flow": 2},
    }

    game_two = {
        "series": {
            "HORNS": {
                "plays": {
                    "Horns Flex": _make_play_payload(3, 5, 3, 4, 2),
                },
                "totals": {
                    "off_set": {"pts": 5, "chances": 3, "ppc": 1.67},
                    "in_flow": {"pts": 4, "chances": 2, "ppc": 2.0},
                },
            },
            "ZONE": {
                "plays": {
                    "Zone 23": _make_play_payload(1, 0, 0, 3, 1),
                },
                "totals": {
                    "off_set": {"pts": 0, "chances": 0, "ppc": 0.0},
                    "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                },
            },
            "FLOW": {
                "plays": [
                    {
                        "playcall": "Horns Flex",
                        "ran_in_flow": 2,
                        "in_flow": {"pts": 4, "chances": 2, "ppc": 2.0},
                    },
                    {
                        "playcall": "Zone 23",
                        "ran_in_flow": 1,
                        "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                    },
                    {
                        "playcall": "FLOW",
                        "ran_in_flow": 1,
                        "in_flow": {"pts": 1, "chances": 1, "ppc": 1.0},
                    },
                ],
                "totals": {"in_flow": {"pts": 8, "chances": 4, "ppc": 2.0}},
            },
        },
        "meta": {"total_chances_off_set": 3, "total_chances_in_flow": 4},
    }

    payloads = {
        1: (game_one, {"updated_at": "2023-11-01T00:00:00Z"}),
        2: (game_two, {"updated_at": "2023-12-15T00:00:00Z"}),
    }
    return payloads


def test_aggregate_playcall_reports_combines_games(monkeypatch, sample_game_payloads):
    call_order = []

    def fake_cache(game_id):
        call_order.append(game_id)
        data, meta = sample_game_payloads[game_id]
        return copy.deepcopy(data), dict(meta)

    monkeypatch.setattr(
        playcall_service,
        "cache_get_or_compute_playcall_report",
        fake_cache,
    )

    aggregated, meta = playcall_service.aggregate_playcall_reports([1, 2, 1])

    assert call_order == [1, 2]
    assert meta["game_ids"] == [1, 2]
    assert meta["game_count"] == 2
    assert meta["updated_at"] == "2023-12-15T00:00:00Z"

    horns = aggregated["series"]["HORNS"]
    horns_flex = horns["plays"]["Horns Flex"]
    assert horns_flex["ran"] == 5
    assert horns_flex["off_set"]["pts"] == 9
    assert horns_flex["off_set"]["chances"] == 5
    assert horns_flex["off_set"]["ppc"] == pytest.approx(1.8)
    assert horns_flex["in_flow"]["pts"] == 7
    assert horns_flex["in_flow"]["chances"] == 3
    assert horns_flex["in_flow"]["ppc"] == pytest.approx(2.33, rel=1e-2)

    zone = aggregated["series"]["ZONE"]
    zone_play = zone["plays"]["Zone 23"]
    assert zone_play["ran"] == 1
    assert zone_play["in_flow"]["pts"] == 3
    assert aggregated["meta"]["total_chances_off_set"] == 5
    assert aggregated["meta"]["total_chances_in_flow"] == 6

    flow_totals = aggregated["series"]["FLOW"]["totals"]["in_flow"]
    assert flow_totals["pts"] == 13
    assert flow_totals["chances"] == 6
    assert flow_totals["ppc"] == pytest.approx(2.17, rel=1e-2)


def test_api_playcall_report_season_view(app, monkeypatch):
    with app.app_context():
        app.config["PLAYCALL_REPORT_ENABLED"] = True
        season = Season(season_name="2023-24")
        db.session.add(season)
        db.session.flush()
        game_one = Game(
            season_id=season.id,
            game_date=date(2023, 11, 1),
            opponent_name="Opponent A",
            home_or_away="Home",
        )
        game_two = Game(
            season_id=season.id,
            game_date=date(2023, 11, 15),
            opponent_name="Opponent B",
            home_or_away="Away",
        )
        db.session.add_all([game_one, game_two])
        db.session.commit()
        season_id_value = season.id
        season_game_ids = [game_one.id, game_two.id]

    aggregated_payload = {
        "series": {
            "FLOW": {"plays": [], "totals": {"in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}}}
        },
        "meta": {"total_chances_off_set": 0, "total_chances_in_flow": 0},
    }
    aggregated_meta = {"source": "aggregate", "game_ids": season_game_ids, "game_count": 2}
    calls = []

    app_module.app = app
    if "routes" in sys.modules:
        routes_module = importlib.reload(sys.modules["routes"])
    else:
        routes_module = importlib.import_module("routes")

    def fake_aggregate(game_ids):
        calls.append(list(game_ids))
        return copy.deepcopy(aggregated_payload), dict(aggregated_meta)

    monkeypatch.setattr(routes_module, "aggregate_playcall_reports", fake_aggregate)

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = "1"
        session["_fresh"] = True

    response = client.get(
        "/api/reports/playcall",
        query_string={"view": "season", "season_id": season_id_value},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["data"] == aggregated_payload
    assert payload["meta"]["season_id"] == season_id_value
    assert payload["meta"]["season_name"] == "2023-24"
    assert payload["meta"]["view"] == "season"
    assert calls == [season_game_ids]

    csv_response = client.get(
        "/api/reports/playcall",
        query_string={
            "view": "season",
            "season_id": season_id_value,
            "format": "csv",
            "family": "FLOW",
        },
    )
    assert csv_response.status_code == 200
    assert csv_response.headers["Content-Type"] == "text/csv"
    disposition = csv_response.headers["Content-Disposition"]
    assert "season_2023_24_flow.csv" in disposition
    assert calls[-1] == season_game_ids
    assert len(calls) == 2
