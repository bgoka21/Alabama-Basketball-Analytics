import copy
import csv
import importlib
import sys
from datetime import date
from io import StringIO

import pandas as pd
import pytest

import app as app_module
from app import db
from flask import template_rendered
from models.database import Game, Season
from services.reports import playcall as playcall_service


def _make_play_payload(ran, off_set_pts, off_set_chances, in_flow_pts, in_flow_chances):
    return {
        "ran": ran,
        "off_set": {"pts": off_set_pts, "chances": off_set_chances, "ppc": 0.0},
        "in_flow": {"pts": in_flow_pts, "chances": in_flow_chances, "ppc": 0.0},
    }


def _build_sample_series_payload():
    return {
        "series": {
            "HORNS": {
                "plays": {
                    "Horns Flex": {
                        "ran": 2,
                        "off_set": {"pts": 4, "chances": 2, "ppc": 2.0},
                        "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                    }
                },
                "totals": {
                    "off_set": {"pts": 4, "chances": 2, "ppc": 2.0},
                    "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                },
            },
            "ZONE": {
                "plays": {
                    "Zone 23": {
                        "ran": 1,
                        "off_set": {"pts": 0, "chances": 0, "ppc": 0.0},
                        "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                    }
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
                        "ran_in_flow": 1,
                        "in_flow": {"pts": 3, "chances": 1, "ppc": 3.0},
                    },
                    {
                        "playcall": "Angle",
                        "ran_in_flow": 2,
                        "in_flow": {"pts": 5, "chances": 2, "ppc": 2.5},
                    },
                ],
                "totals": {
                    "in_flow": {"pts": 8, "chances": 3, "ppc": 2.67},
                },
            },
        },
        "meta": {"total_chances_off_set": 2, "total_chances_in_flow": 4},
    }


def test_compute_from_dataframe_normalizes_playcall_tokens():
    df = pd.DataFrame(
        [
            {
                "Row": "Offense",
                "SERIES": "2 Keep M Rip",
                "PLAYCALL": "2 Keep M Rip",
                "TEAM": "Alabama",
                "#1": "2FG+",
            },
            {
                "Row": "Offense",
                "SERIES": "2 Keep M Rip, Flow",
                "PLAYCALL": "2 Keep M Rip, Flow - Power",
                "TEAM": "Alabama",
                "#1": "3FG+",
            },
            {
                "Row": "Offense",
                "SERIES": "FLOW",
                "PLAYCALL": "Flow - Angle",
                "TEAM": "Alabama",
                "#1": "3FG+",
            },
        ]
    )

    payload = playcall_service._compute_from_dataframe(df)

    family_payload = payload["series"].get("2 Keep M Rip")
    assert family_payload is not None
    plays = family_payload["plays"]
    assert set(plays.keys()) == {"2 Keep M Rip"}

    base_play = plays["2 Keep M Rip"]
    assert base_play["ran"] == 2
    assert base_play["off_set"]["pts"] == 2
    assert base_play["off_set"]["chances"] == 1
    assert base_play["in_flow"]["pts"] == 3
    assert base_play["in_flow"]["chances"] == 1

    flow_entries = payload["series"]["FLOW"]["plays"]
    assert len(flow_entries) == 2
    flow_entry = flow_entries[0]
    assert flow_entry["playcall"] == "2 Keep M Rip"
    assert flow_entry["ran_in_flow"] == 1
    assert flow_entry["in_flow"]["pts"] == 3
    assert flow_entry["in_flow"]["chances"] == 1

    flow_angle_entry = flow_entries[1]
    assert flow_angle_entry["playcall"] == "Angle"
    assert flow_angle_entry["ran_in_flow"] == 1
    assert flow_angle_entry["in_flow"]["pts"] == 3
    assert flow_angle_entry["in_flow"]["chances"] == 1


def test_compute_from_dataframe_prefers_non_flow_series_label():
    df = pd.DataFrame(
        [
            {
                "Row": "Offense",
                "SERIES": "FLOW, NOVA",
                "PLAYCALL": "NOVA Rip",
                "TEAM": "Alabama",
                "#1": "3FG+",
            }
        ]
    )

    payload = playcall_service._compute_from_dataframe(df)

    nova_family = payload["series"].get("NOVA")
    assert nova_family is not None
    assert "NOVA Rip" in nova_family["plays"]

    nova_play = nova_family["plays"]["NOVA Rip"]
    assert nova_play["ran"] == 1
    assert nova_play["in_flow"]["pts"] == 3
    assert nova_play["in_flow"]["chances"] == 1

    flow_payload = payload["series"].get("FLOW")
    assert flow_payload is not None

    flow_entries = flow_payload["plays"]
    assert any(entry["playcall"] == "NOVA Rip" for entry in flow_entries)

    flow_totals = flow_payload["totals"]["in_flow"]
    assert flow_totals["pts"] == 3
    assert flow_totals["chances"] == 1


def test_compute_from_dataframe_normalizes_misc_unknown_rows():
    df = pd.DataFrame(
        [
            {
                "Row": "Offense",
                "SERIES": "UKNOWN",
                "PLAYCALL": "",
                "TEAM": "Alabama",
                "#1": "2FG+",
            },
            {
                "Row": "Offense",
                "SERIES": "UKNOWN",
                "PLAYCALL": "Backdoor",
                "TEAM": "Alabama",
                "#1": "3FG+",
            },
            {
                "Row": "Offense",
                "SERIES": "UKNOWN",
                "PLAYCALL": "UNKNOWN",
                "TEAM": "Alabama",
                "#1": "FT+",
            },
            {
                "Row": "Offense",
                "SERIES": "UNKNOWN",
                "PLAYCALL": "",
                "TEAM": "Alabama",
                "#2": "FT+",
            },
            {
                "Row": "Offense",
                "SERIES": "UNKNOWN",
                "PLAYCALL": "Flare",
                "TEAM": "Alabama",
                "#2": "2FG+",
            },
            {
                "Row": "Offense",
                "SERIES": "UNKNOWN",
                "PLAYCALL": "UNKNOWN",
                "TEAM": "Alabama",
                "#2": "3FG+",
            },
        ]
    )

    payload = playcall_service._compute_from_dataframe(df)

    assert "UKNOWN" not in payload["series"]
    assert "UNKNOWN" not in payload["series"]
    misc_family = payload["series"].get("MISC")
    assert misc_family is not None
    assert set(misc_family["plays"].keys()) == {"Backdoor", "Flare"}

    misc_play = misc_family["plays"]["Backdoor"]
    assert misc_play["ran"] == 1
    assert misc_play["off_set"]["pts"] == 3
    assert misc_play["off_set"]["chances"] == 1
    assert misc_play["in_flow"]["pts"] == 0
    assert misc_play["in_flow"]["chances"] == 0

    flare_play = misc_family["plays"]["Flare"]
    assert flare_play["ran"] == 1
    assert flare_play["off_set"]["pts"] == 2
    assert flare_play["off_set"]["chances"] == 1
    assert flare_play["in_flow"]["pts"] == 0
    assert flare_play["in_flow"]["chances"] == 0

    totals = misc_family["totals"]
    assert totals["off_set"]["pts"] == 5
    assert totals["off_set"]["chances"] == 2
    assert payload["meta"]["total_chances_off_set"] == 2
    assert payload["meta"]["total_chances_in_flow"] == 0


def test_compute_from_dataframe_skips_unknown_series_without_playcall():
    df = pd.DataFrame(
        [
            {"Row": "Offense", "SERIES": "UKNOWN", "PLAYCALL": "", "TEAM": "Alabama"},
            {"Row": "Offense", "SERIES": "UNKNOWN", "PLAYCALL": None, "TEAM": "Alabama"},
        ]
    )

    payload = playcall_service._compute_from_dataframe(df)

    assert payload["series"] == {
        "FLOW": {
            "plays": [],
            "totals": {"in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}},
        }
    }
    assert payload["meta"] == {"total_chances_off_set": 0, "total_chances_in_flow": 0}


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
                        "playcall": "Angle",
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
                        "playcall": "Angle",
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


def test_aggregate_playcall_reports_filters_misc_unknown_entries(monkeypatch):
    misc_payload = {
        "plays": {
            "UNKNOWN": _make_play_payload(1, 2, 1, 0, 0),
            "Backdoor": _make_play_payload(1, 3, 1, 2, 1),
        },
        "totals": {
            "off_set": {"pts": 5, "chances": 2, "ppc": 2.5},
            "in_flow": {"pts": 2, "chances": 1, "ppc": 2.0},
        },
    }
    unknown_payload = {
        "plays": {
            "": _make_play_payload(1, 1, 1, 0, 0),
            "Flare": _make_play_payload(2, 4, 2, 0, 0),
        },
        "totals": {
            "off_set": {"pts": 5, "chances": 3, "ppc": 1.67},
            "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0},
        },
    }
    flow_payload = {
        "plays": [
            {"playcall": "UNKNOWN", "ran_in_flow": 1, "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}},
            {"playcall": "Backdoor", "ran_in_flow": 1, "in_flow": {"pts": 2, "chances": 1, "ppc": 2.0}},
        ],
        "totals": {"in_flow": {"pts": 2, "chances": 1, "ppc": 2.0}},
    }

    cached_payload = {
        "series": {"UKNOWN": misc_payload, "UNKNOWN": unknown_payload, "FLOW": flow_payload},
        "meta": {"total_chances_off_set": 3, "total_chances_in_flow": 1},
    }

    def fake_cache(game_id):
        return copy.deepcopy(cached_payload), {"updated_at": "2024-01-01T00:00:00Z"}

    monkeypatch.setattr(
        playcall_service,
        "cache_get_or_compute_playcall_report",
        fake_cache,
    )

    aggregated, meta = playcall_service.aggregate_playcall_reports([10])

    assert meta["game_ids"] == [10]
    assert meta["game_count"] == 1
    assert aggregated["meta"]["total_chances_off_set"] == 3
    assert aggregated["meta"]["total_chances_in_flow"] == 1

    assert "UKNOWN" not in aggregated["series"]
    assert "UNKNOWN" not in aggregated["series"]
    misc_family = aggregated["series"].get("MISC")
    assert misc_family is not None
    assert set(misc_family["plays"].keys()) == {"Backdoor", "Flare"}

    misc_play = misc_family["plays"]["Backdoor"]
    assert misc_play["ran"] == 1
    assert misc_play["off_set"]["pts"] == 3
    assert misc_play["off_set"]["chances"] == 1
    assert misc_play["in_flow"]["pts"] == 2
    assert misc_play["in_flow"]["chances"] == 1

    flare_misc = misc_family["plays"]["Flare"]
    assert flare_misc["ran"] == 2
    assert flare_misc["off_set"]["pts"] == 4
    assert flare_misc["off_set"]["chances"] == 2
    assert flare_misc["in_flow"]["pts"] == 0
    assert flare_misc["in_flow"]["chances"] == 0

    flow_series = aggregated["series"].get("FLOW")
    assert flow_series is not None
    flow_rows = flow_series["plays"]
    assert len(flow_rows) == 1
    assert flow_rows[0]["playcall"] == "Backdoor"
    assert flow_rows[0]["ran_in_flow"] == 1
    assert flow_rows[0]["in_flow"]["chances"] == 1


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


def test_playcall_report_all_card_includes_flow_only(app, monkeypatch):
    payload = _build_sample_series_payload()
    meta = {"updated_at": "2024-01-02T00:00:00Z"}

    app_module.app = app
    if "routes" in sys.modules:
        routes_module = importlib.reload(sys.modules["routes"])
    else:
        routes_module = importlib.import_module("routes")

    def fake_cache(game_id):
        return copy.deepcopy(payload), dict(meta)

    monkeypatch.setattr(
        routes_module,
        "cache_get_or_compute_playcall_report",
        fake_cache,
    )

    app.jinja_env.filters.setdefault(
        "date",
        lambda value, fmt="%Y-%m-%d": value.strftime(fmt)
        if hasattr(value, "strftime")
        else value,
    )

    with app.app_context():
        app.config["PLAYCALL_REPORT_ENABLED"] = True
        season = Season(season_name="2024-25")
        db.session.add(season)
        db.session.flush()
        game = Game(
            season_id=season.id,
            game_date=date(2024, 1, 5),
            opponent_name="Sample Opponent",
            home_or_away="Home",
        )
        db.session.add(game)
        db.session.commit()
        game_id = game.id

    captured = {}

    def record(sender, template, context, **extra):
        if template.name == "reports/playcall.html":
            captured["context"] = context

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = "1"
        session["_fresh"] = True

    with template_rendered.connected_to(record, app):
        response = client.get(
            "/reports/playcall",
            query_string={"view": "game", "game_id": game_id},
        )

    assert response.status_code == 200
    assert "context" in captured
    context = captured["context"]

    assert context["series_options"][0] == "ALL"
    assert "ALL" in context["selected_series"]

    all_section = context["all_section"]
    assert all_section is not None
    rows = all_section["rows"]
    assert len(rows) == 3
    combos = {(row["series"], row["playcall"]) for row in rows}
    assert combos == {("HORNS", "Horns Flex"), ("ZONE", "Zone 23"), ("FLOW", "Angle")}

    flow_row = next(row for row in rows if row["series"] == "FLOW")
    assert flow_row["ran"] == 2
    assert flow_row["off_set_pts"] == 0
    assert flow_row["off_set_chances"] == 0
    assert flow_row["in_flow_pts"] == 5
    assert flow_row["in_flow_chances"] == 2
    totals = all_section["totals"]
    assert totals["ran"] == 5
    assert totals["off_set_pts"] == 4
    assert totals["off_set_chances"] == 2
    assert totals["in_flow_pts"] == 11
    assert totals["in_flow_chances"] == 4
    assert totals["off_set_ppc"] == "2.00"
    assert totals["in_flow_ppc"] == "2.75"


def test_flatten_playcall_series_normalizes_misc_unknown_rows():
    body = {
        "series": {
            "UKNOWN": {
                "plays": {
                    "UNKNOWN": _make_play_payload(1, 2, 1, 0, 0),
                    "Backdoor": _make_play_payload(1, 3, 1, 1, 1),
                },
                "totals": {
                    "off_set": {"pts": 5, "chances": 2, "ppc": 2.5},
                    "in_flow": {"pts": 1, "chances": 1, "ppc": 1.0},
                },
            },
            "UNKNOWN": {
                "plays": {
                    "": _make_play_payload(1, 1, 1, 0, 0),
                    "Flare": _make_play_payload(2, 4, 2, 0, 0),
                },
                "totals": {
                    "off_set": {"pts": 5, "chances": 3, "ppc": 1.67},
                    "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0},
                },
            },
            "FLOW": {
                "plays": [
                    {"playcall": "UNKNOWN", "ran_in_flow": 1, "in_flow": {"pts": 0, "chances": 0, "ppc": 0.0}},
                    {"playcall": "Backdoor", "ran_in_flow": 1, "in_flow": {"pts": 1, "chances": 1, "ppc": 1.0}},
                ],
                "totals": {"in_flow": {"pts": 1, "chances": 1, "ppc": 1.0}},
            },
        }
    }

    routes_module = sys.modules.get("routes") or importlib.import_module("routes")

    flat = routes_module._flatten_playcall_series(body["series"])

    rows = flat["rows"]
    assert all(row["series"] not in {"UKNOWN", "UNKNOWN"} for row in rows)
    assert all(row["playcall"].upper() != "UNKNOWN" for row in rows)

    misc_rows = [row for row in rows if row["series"] == "MISC"]
    misc_pairs = {(row["series"], row["playcall"]) for row in misc_rows}
    assert misc_pairs == {("MISC", "Backdoor"), ("MISC", "Flare")}

    flow_rows = [row for row in rows if row["series"] == "FLOW"]
    assert flow_rows == []

    assert flat["totals"]["ran"] == 3
    assert flat["totals"]["off_set"]["chances"] == 3
    assert flat["totals"]["in_flow"]["chances"] == 1


def test_api_playcall_all_csv_and_json(app, monkeypatch):
    payload = _build_sample_series_payload()
    meta = {"updated_at": "2024-01-02T00:00:00Z"}

    app_module.app = app
    if "routes" in sys.modules:
        routes_module = importlib.reload(sys.modules["routes"])
    else:
        routes_module = importlib.import_module("routes")

    def fake_cache(game_id):
        return copy.deepcopy(payload), dict(meta)

    monkeypatch.setattr(
        routes_module,
        "cache_get_or_compute_playcall_report",
        fake_cache,
    )

    with app.app_context():
        app.config["PLAYCALL_REPORT_ENABLED"] = True
        season = Season(season_name="2024-25")
        db.session.add(season)
        db.session.flush()
        game = Game(
            season_id=season.id,
            game_date=date(2024, 1, 5),
            opponent_name="Sample Opponent",
            home_or_away="Home",
        )
        db.session.add(game)
        db.session.commit()
        game_id = game.id

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = "1"
        session["_fresh"] = True

    json_response = client.get(
        "/api/reports/playcall",
        query_string={"game_id": game_id},
    )
    assert json_response.status_code == 200
    body = json_response.get_json()
    assert body["data"]["meta"] == payload["meta"]

    flat = routes_module._flatten_playcall_series(body["data"].get("series"))
    rows = flat["rows"]
    assert len(rows) == 3
    pairs = {(row["series"], row["playcall"]) for row in rows}
    assert pairs == {("HORNS", "Horns Flex"), ("ZONE", "Zone 23"), ("FLOW", "Angle")}

    flow_entry = next(row for row in rows if row["series"] == "FLOW")
    assert flow_entry["off_set"]["pts"] == 0
    assert flow_entry["off_set"]["chances"] == 0
    assert flow_entry["ran"] == 2
    assert flow_entry["in_flow"]["pts"] == 5
    assert flow_entry["in_flow"]["chances"] == 2

    totals = flat["totals"]
    assert totals["ran"] == 5
    assert totals["off_set"]["pts"] == 4
    assert totals["off_set"]["chances"] == 2
    assert totals["in_flow"]["pts"] == 11
    assert totals["in_flow"]["chances"] == 4

    csv_response = client.get(
        "/api/reports/playcall",
        query_string={"game_id": game_id, "format": "csv", "family": "ALL"},
    )
    assert csv_response.status_code == 200
    assert csv_response.headers["Content-Type"] == "text/csv"
    assert f"game_{game_id}_all.csv" in csv_response.headers["Content-Disposition"]

    csv_reader = csv.DictReader(StringIO(csv_response.data.decode("utf-8")))
    csv_rows = list(csv_reader)
    assert len(csv_rows) == 4
    flow_csv = next(row for row in csv_rows if row["SERIES"] == "FLOW")
    assert flow_csv["OFF SET PTS"] == "0"
    assert flow_csv["OFF SET CHANCES"] == "0"
    assert flow_csv["RAN"] == "2"

    totals_row = csv_rows[-1]
    assert totals_row["SERIES"] == "Totals"
    assert totals_row["PLAYCALL"] == "Totals"
    assert totals_row["RAN"] == "5"
    assert totals_row["OFF SET PTS"] == "4"
    assert totals_row["OFF SET CHANCES"] == "2"
    assert totals_row["IN FLOW PTS"] == "11"
    assert totals_row["IN FLOW CHANCES"] == "4"
    assert totals_row["OFF SET PPC"] == "2.00"
    assert totals_row["IN FLOW PPC"] == "2.75"
