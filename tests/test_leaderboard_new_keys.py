import pytest


ALICE = "Alice"
BOB = "Bob"
CAROL = "Carol"


def rows_for_off_reb():
    return [
        {"player": ALICE, "crash_positive": 1, "crash_missed": 0, "back_man_positive": 0, "back_man_missed": 0},
        {"player": BOB, "crash_positive": 0, "crash_missed": 1, "back_man_positive": 0, "back_man_missed": 0},
        {"player": CAROL, "crash_positive": 1, "crash_missed": 1, "back_man_positive": 1, "back_man_missed": 1},
    ]


def rows_for_def_reb():
    return [
        {"player": ALICE, "box_out_positive": 1, "box_out_missed": 0, "off_reb_given_up": 0},
        {"player": BOB, "box_out_positive": 0, "box_out_missed": 1, "off_reb_given_up": 1},
        {"player": CAROL, "box_out_positive": 0, "box_out_missed": 0, "off_reb_given_up": 1},
    ]


def rows_for_collision_gap():
    return [
        {
            "player": ALICE,
            "collision_gap_positive": 1,
            "collision_gap_missed": 1,
            "low_help_positive": 1,
            "low_help_missed": 0,
        },
        {
            "player": BOB,
            "collision_gap_positive": 1,
            "collision_gap_missed": 1,
            "low_help_positive": 0,
            "low_help_missed": 1,
        },
        {
            "player": CAROL,
            "collision_gap_positive": 1,
            "collision_gap_missed": 0,
            "low_help_positive": 2,
            "low_help_missed": 0,
        },
    ]


def rows_for_pnr_gap_help():
    return [
        {"player": ALICE, "pnr_gap_positive": 1, "pnr_gap_missed": 0, "low_help_positive": 1, "low_help_missed": 0},
        {"player": BOB, "pnr_gap_positive": 0, "pnr_gap_missed": 1, "low_help_positive": 0, "low_help_missed": 1},
        {"player": CAROL, "pnr_gap_positive": 0, "pnr_gap_missed": 0, "low_help_positive": 1, "low_help_missed": 0},
    ]


def rows_for_pnr_grade():
    return [
        {"player": ALICE, "close_window_positive": 1, "close_window_missed": 0, "shut_door_positive": 0, "shut_door_missed": 1},
        {"player": BOB, "close_window_positive": 0, "close_window_missed": 1, "shut_door_positive": 1, "shut_door_missed": 0},
        {"player": CAROL, "close_window_positive": 1, "close_window_missed": 0, "shut_door_positive": 1, "shut_door_missed": 0},
    ]


@pytest.mark.parametrize("key, rows", [
    ("off_rebounding", rows_for_off_reb()),
    ("def_rebounding", rows_for_def_reb()),
    ("collision_gap_help", rows_for_collision_gap()),
    ("pnr_gap_help", rows_for_pnr_gap_help()),
    ("pnr_grade", rows_for_pnr_grade()),
])
def test_leaderboard_new_keys_sanity(key, rows):
    from admin.routes import compute_leaderboard_for_key

    out = compute_leaderboard_for_key(key, rows)
    assert "rows" in out and isinstance(out["rows"], list)

    if key == "off_rebounding":
        r0 = next(r for r in out["rows"] if r[0] == ALICE)
        assert r0[1] == 1 and r0[2] == 1 and r0[3] == 100.0
        assert r0[4] == 0 and r0[5] == 0 and r0[6] is None
    elif key == "def_rebounding":
        r2 = next(r for r in out["rows"] if r[0] == CAROL)
        assert r2[1] == 0 and r2[2] == 0 and r2[3] is None and r2[4] == 1
    elif key == "collision_gap_help":
        r2 = next(r for r in out["rows"] if r[0] == CAROL)
        assert r2[1] == 1 and r2[2] == 1 and r2[3] == 100.0
        assert r2[4] == 2 and r2[5] == 2 and r2[6] == 100.0
    elif key == "pnr_gap_help":
        r0 = next(r for r in out["rows"] if r[0] == ALICE)
        assert r0[1] == 1 and r0[2] == 1 and r0[3] == 100.0
        assert r0[4] == 1 and r0[5] == 1 and r0[6] == 100.0
    elif key == "pnr_grade":
        r1 = next(r for r in out["rows"] if r[0] == BOB)
        assert r1[1] == 0 and r1[2] == 1 and r1[3] == 0.0
        assert r1[4] == 1 and r1[5] == 1 and r1[6] == 100.0
