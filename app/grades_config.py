"""Grade thresholds for percentage-based metrics.

Each entry defines the lower bounds for the nine color bins used by
``grade_token``. Values are expressed as percentages unless noted.
"""

from __future__ import annotations

from typing import Dict, List

# Thresholds are ascending lists that produce nine bins (eight thresholds)
# when evaluated by ``grade_token``. These values intentionally mirror the
# previous inline-style gradients so existing visual expectations are kept.
_DEFENSE_CONTACT_THRESHOLDS = [45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0]
_GAP_HELP_THRESHOLDS = [40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0]
_CONTEST_BREAKDOWN_THRESHOLDS = [45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0]


GRADES: Dict[str, List[float]] = {
    # Points per shot. Numbers are the direct PPS thresholds that were used to
    # determine the gradient cut points in the legacy implementation.
    "pps": [0.80, 0.90, 1.00, 1.05, 1.10, 1.15, 1.20, 1.30],
    # At the rim / 2FG%. These thresholds reflect the PPS breakpoints above
    # converted to field-goal percentage when a made shot is worth two points.
    "atr2fg_pct": [40.0, 45.0, 50.0, 52.5, 55.0, 57.5, 60.0, 65.0],
    # Catch-all two-point percentage for contexts that are not strictly ATR.
    "fg2_pct": [38.0, 42.0, 46.0, 50.0, 54.0, 58.0, 62.0, 66.0],
    # 3FG% tends to have lower absolute values so the thresholds skew lower.
    "fg3_pct": [20.0, 25.0, 30.0, 33.0, 36.0, 39.0, 42.0, 45.0],
    # Free throw percentage thresholds for any FT displays that leverage the
    # shared grading helper.
    "ft_pct": [60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0],
    # Defensive rebounding events and contact percentages.
    "bump_pct": _DEFENSE_CONTACT_THRESHOLDS,
    "crash_pct": _DEFENSE_CONTACT_THRESHOLDS,
    "back_man_pct": _DEFENSE_CONTACT_THRESHOLDS,
    "box_out_pct": _DEFENSE_CONTACT_THRESHOLDS,
    # Gap / Low man help and pick-and-roll coverage responsibilities.
    "gap_pct": _GAP_HELP_THRESHOLDS,
    "low_pct": _GAP_HELP_THRESHOLDS,
    "close_window_pct": _GAP_HELP_THRESHOLDS,
    "shut_door_pct": _GAP_HELP_THRESHOLDS,
    # Shot contest breakdowns (contest, late contest, no contest).
    "contest_pct": _CONTEST_BREAKDOWN_THRESHOLDS,
    "late_pct": _CONTEST_BREAKDOWN_THRESHOLDS,
    "no_contest_pct": _CONTEST_BREAKDOWN_THRESHOLDS,
}
