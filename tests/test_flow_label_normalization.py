import app as app_module

if app_module.app is None:
    app_module.app = app_module.create_app()

from routes import _normalize_flow_label


def test_normalize_flow_label_removes_hyphen_prefix():
    assert _normalize_flow_label("Flow - Drive & Kick") == "Drive & Kick"


def test_normalize_flow_label_removes_en_dash_prefix():
    assert _normalize_flow_label("Flow – Angle") == "Angle"


def test_normalize_flow_label_is_case_insensitive_and_trims_whitespace():
    assert _normalize_flow_label("  flow – wide  ") == "wide"


def test_normalize_flow_label_leaves_non_flow_labels_untouched():
    assert _normalize_flow_label("Early Spread") == "Early Spread"
