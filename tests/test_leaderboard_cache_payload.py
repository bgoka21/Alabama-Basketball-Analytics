import pytest

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


@pytest.mark.parametrize(
    "stat_key, rows, totals, expected_labels",
    [
        (
            "defense",
            [("#1 Stopper", 10, 20, 50.0)],
            (10, 20, 50.0),
            ["#", "Player", "Bump +", "Bump Opps", "Bump %"],
        ),
        (
            "off_rebounding",
            [("#2 Crasher", 8, 16, 50.0, 4, 8, 50.0)],
            (8, 16, 50.0, 4, 8, 50.0),
            [
                "#",
                "Player",
                "Crash +",
                "Crash Att",
                "Crash %",
                "Back Man +",
                "Back Man Att",
                "Back Man %",
            ],
        ),
        (
            "def_rebounding",
            [("#3 Anchor", 6, 12, 50.0, 2)],
            (6, 12, 50.0, 2),
            [
                "#",
                "Player",
                "Box Out +",
                "Box Out Att",
                "Box Out %",
                "Off Reb's Given Up",
            ],
        ),
        (
            "collision_gap_help",
            [("#4 Helper", 9, 18, 50.0, 7, 14, 50.0)],
            (9, 18, 50.0, 7, 14, 50.0),
            [
                "#",
                "Player",
                "Gap +",
                "Gap Opp",
                "Gap %",
                "Low +",
                "Low Opp",
                "Low %",
            ],
        ),
        (
            "pass_contest",
            [("#5 Disruptor", 5, 10, 50.0)],
            (5, 10, 50.0),
            ["#", "Player", "Contest +", "Contest Att", "Contest %"],
        ),
        (
            "overall_gap_help",
            [
                {
                    "player_name": "#6 Glue",
                    "plus": 11,
                    "opps": 22,
                    "pct": 50.0,
                }
            ],
            {"plus": 11, "opps": 22, "pct": 50.0},
            ["#", "Player", "Gap +", "Gap Opp", "Gap %"],
        ),
        (
            "overall_low_man",
            [
                {
                    "player_name": "#7 Anchor",
                    "plus": 13,
                    "opps": 26,
                    "pct": 50.0,
                }
            ],
            {"plus": 13, "opps": 26, "pct": 50.0},
            ["#", "Player", "Low +", "Low Opp", "Low %"],
        ),
        (
            "pnr_gap_help",
            [("#8 Switch", 7, 14, 50.0, 6, 12, 50.0)],
            (7, 14, 50.0, 6, 12, 50.0),
            [
                "#",
                "Player",
                "Gap +",
                "Gap Opp",
                "Gap %",
                "Low +",
                "Low Opp",
                "Low %",
            ],
        ),
        (
            "pnr_grade",
            [("#9 Wall", 6, 12, 50.0, 5, 10, 50.0)],
            (6, 12, 50.0, 5, 10, 50.0),
            [
                "#",
                "Player",
                "Close Window +",
                "Close Window Att",
                "Close Window %",
                "Shut Door +",
                "Shut Door Att",
                "Shut Door %",
            ],
        ),
        (
            "atr_contest_breakdown",
            [
                {
                    "player": "#10 Contestant",
                    "contest_attempts": 10,
                    "contest_makes": 4,
                    "contest_pct": 40.0,
                    "late_attempts": 6,
                    "late_makes": 2,
                    "late_pct": 33.3,
                    "no_contest_attempts": 3,
                    "no_contest_makes": 1,
                    "no_contest_pct": 33.3,
                }
            ],
            {
                "contest": {"plus": 4, "opps": 10, "pct": 40.0},
                "late": {"plus": 2, "opps": 6, "pct": 33.3},
                "no_contest": {"plus": 1, "opps": 3, "pct": 33.3},
            },
            [
                "#",
                "Player",
                "Contest Att",
                "Contest Makes",
                "Contest FG%",
                "Late Att",
                "Late Makes",
                "Late FG%",
                "No Contest Att",
                "No Contest Makes",
                "No Contest FG%",
            ],
        ),
    ],
)
def test_format_payload_special_stats(stat_key, rows, totals, expected_labels):
    cfg = _config_for(stat_key)
    compute_result = {"config": cfg, "rows": rows, "team_totals": totals}

    payload = format_leaderboard_payload(stat_key, compute_result)

    labels = [col["label"] for col in payload["columns"]]
    assert labels == expected_labels
