import logging

from utils.shot_location_map import (
    clear_unknown_shot_locations,
    get_unknown_shot_locations,
    normalize_shot_location,
)


def test_normalize_shot_location_seeded_values():
    clear_unknown_shot_locations()

    assert normalize_shot_location("Rim") == "rim"
    assert normalize_shot_location("Paint") == "paint"
    assert normalize_shot_location("Corner") == "corner"
    assert normalize_shot_location("Corner 3") == "corner"
    assert normalize_shot_location("Left Corner") == "corner_left"
    assert normalize_shot_location("Right Corner") == "corner_right"
    assert normalize_shot_location("Wing") == "wing"
    assert normalize_shot_location("Right Wing") == "wing_right"
    assert normalize_shot_location("Left Slot") == "slot_left"
    assert normalize_shot_location("Right Slot") == "slot_right"
    assert normalize_shot_location("Left Houston") == "short_wing_left"
    assert normalize_shot_location("Right Houston") == "short_wing_right"
    assert normalize_shot_location("Top") == "top"
    assert normalize_shot_location("Logo") == "logo"
    assert normalize_shot_location("Right SW") == "short_wing_right"

    assert get_unknown_shot_locations() == set()


def test_normalize_shot_location_unknown_values_are_recorded(caplog):
    clear_unknown_shot_locations()

    with caplog.at_level(logging.WARNING):
        assert normalize_shot_location("Mystery Zone") == "unknown"
        assert normalize_shot_location("") == "unknown"
        assert normalize_shot_location(None) == "unknown"

    assert {
        "Mystery Zone",
        "<empty>",
        "<missing>",
    } <= get_unknown_shot_locations()

    assert any(
        "Unknown shot location encountered" in record.message
        for record in caplog.records
    )
