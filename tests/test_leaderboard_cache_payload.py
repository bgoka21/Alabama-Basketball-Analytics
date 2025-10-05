from services.cache_leaderboard import format_leaderboard_payload
from stats_config import LEADERBOARD_STATS


def _config_for(key: str) -> dict:
    cfg = next((c for c in LEADERBOARD_STATS if c["key"] == key), None)
    if cfg is None:
        raise AssertionError(f"Missing config for {key}")
    return dict(cfg)


def test_format_payload_points_rows_are_strings():
    cfg = _config_for("points")
    compute_result = {
        "config": cfg,
        "rows": [("#2 Aden Holloway", 838), ("#10 Mark Sears", 702)],
        "team_totals": {"points": 1540},
    }

    payload = format_leaderboard_payload("points", compute_result)

    labels = [col["label"] for col in payload["columns"]]
    assert labels == ["#", "Player", "Points"]
    assert len(payload["rows"]) == 2
    first_row = payload["rows"][0]
    assert len(first_row) == len(payload["columns"])
    assert all(isinstance(value, str) for value in first_row)
    totals = payload["totals"]
    assert isinstance(totals, list)
    assert len(totals) == len(payload["columns"])
    assert totals[-1] == "1540"
    assert payload["last_built_at"].endswith("Z")


def test_format_payload_offense_summary_multi_columns():
    cfg = _config_for("offense_summary")
    row = {
        "player": "#2 Aden Holloway",
        "offensive_possessions": 120,
        "ppp_on": 1.12,
        "ppp_off": 0.95,
        "individual_turnover_rate": 14.3,
        "bamalytics_turnover_rate": 17.8,
        "individual_team_turnover_pct": 19.0,
        "turnover_rate": 15.4,
        "individual_off_reb_rate": 8.2,
        "off_reb_rate": 28.5,
        "individual_foul_rate": 7.5,
        "fouls_drawn_rate": 23.4,
    }
    totals = {
        "offensive_possessions": 240,
        "ppp_on": 1.08,
        "ppp_off": 0.98,
        "individual_turnover_rate": 15.0,
        "bamalytics_turnover_rate": 18.5,
        "individual_team_turnover_pct": 19.2,
        "turnover_rate": 16.2,
        "individual_off_reb_rate": 9.1,
        "off_reb_rate": 30.4,
        "individual_foul_rate": 8.0,
        "fouls_drawn_rate": 24.1,
    }
    compute_result = {"config": cfg, "rows": [row], "team_totals": totals}

    payload = format_leaderboard_payload("offense_summary", compute_result)

    labels = [col["label"] for col in payload["columns"]]
    assert "PPP On" in labels
    assert "Team TO Rate" in labels
    first_row = payload["rows"][0]
    assert len(first_row) == len(payload["columns"])
    assert all(isinstance(value, str) for value in first_row)
    totals_row = payload.get("totals")
    assert totals_row is not None
    assert len(totals_row) == len(payload["columns"])


def test_format_payload_fg3_pct_includes_subcolumns():
    cfg = _config_for("fg3_fg_pct")
    row = {
        "player": "#5 Marksman",
        "fg3_makes": 45,
        "fg3_attempts": 120,
        "fg3_fg_pct": 37.5,
        "fg3_freq_pct": 42.0,
        "fg3_shrink_makes": 18,
        "fg3_shrink_att": 44,
        "fg3_shrink_pct": 40.9,
        "fg3_nonshrink_makes": 27,
        "fg3_nonshrink_att": 76,
        "fg3_nonshrink_pct": 35.5,
    }
    totals = {
        "fg3_makes": 90,
        "fg3_attempts": 260,
        "fg3_fg_pct": 34.6,
        "fg3_freq_pct": 39.0,
        "fg3_shrink_makes": 36,
        "fg3_shrink_att": 90,
        "fg3_shrink_pct": 40.0,
        "fg3_nonshrink_makes": 54,
        "fg3_nonshrink_att": 170,
        "fg3_nonshrink_pct": 31.8,
    }
    compute_result = {"config": cfg, "rows": [row], "team_totals": totals}

    payload = format_leaderboard_payload("fg3_fg_pct", compute_result)

    labels = [col["label"] for col in payload["columns"]]
    assert "Shrink 3FG %" in labels
    assert "Non-Shrink 3FG %" in labels
    first_row = payload["rows"][0]
    assert len(first_row) == len(payload["columns"])
    assert all(isinstance(value, str) for value in first_row)
    assert len(payload["column_keys"]) == len(payload["columns"])
