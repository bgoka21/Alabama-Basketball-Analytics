import json
from types import SimpleNamespace

from admin.routes import compute_team_shot_details
from utils.shottype import gather_labels_for_shot


def test_gather_labels_includes_balance_tag():
    shot = {
        "shot_class": "3fg",
        "result": "made",
        "possession_type": "Halfcourt",
        "event": "shot_attempt",
        "Assisted": "Assisted",
        "Non-Assisted": "",
        "3fg_balance": "On Balance",
        "drill_labels": [],
    }

    labels = gather_labels_for_shot(shot)

    assert "On Balance" in labels
    assert "Assisted" in labels


def test_compute_team_shot_details_counts_balance_rows():
    shots = [
        {
            "shot_class": "3fg",
            "result": "made",
            "possession_type": "Halfcourt",
            "event": "shot_attempt",
            "Assisted": "Assisted",
            "Non-Assisted": "",
            "3fg_balance": "On Balance",
            "drill_labels": [],
        },
        {
            "shot_class": "3fg",
            "result": "miss",
            "possession_type": "Transition",
            "event": "shot_attempt",
            "Assisted": "",
            "Non-Assisted": "Non-Assisted",
            "3fg_balance": "Off Balance",
            "drill_labels": [],
        },
    ]

    record = SimpleNamespace(shot_type_details=json.dumps(shots))

    _, summaries = compute_team_shot_details([record], set())

    on_balance = summaries["fg3"].cats["On Balance"]
    off_balance = summaries["fg3"].cats["Off Balance"]

    assert on_balance.total.attempts == 1
    assert on_balance.total.makes == 1
    assert on_balance.halfcourt.attempts == 1
    assert off_balance.total.attempts == 1
    assert off_balance.total.makes == 0
    assert off_balance.transition.attempts == 1
