"""Helpers for mapping percentage metrics to CSS grade tokens."""

from __future__ import annotations

import math
from typing import Optional

from app.grades_config import GRADES

_BASE_CLASS = "grade-token"


def _coerce_value(value) -> Optional[float]:
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(coerced) or math.isinf(coerced):
        return None
    return coerced


def grade_token(metric_key: str, value_pct) -> Optional[str]:
    """Return the grade token for ``metric_key`` at ``value_pct``.

    ``value_pct`` may be provided as a raw percentage (0-100) or as a decimal
    (0-1). ``None`` is returned when the metric is unknown or the value cannot
    be evaluated.
    """

    thresholds = GRADES.get(metric_key)
    if not thresholds:
        return None

    value = _coerce_value(value_pct)
    if value is None:
        return None

    # Support decimal inputs such as 0.45 when the thresholds are percentage
    # based values (e.g., 45.0). A heuristic is used so non-percentage metrics
    # such as PPS, where thresholds are <= 2, are not scaled.
    if value <= 1 and max(thresholds) > 1.5:
        value *= 100

    bin_index = 0
    for threshold in thresholds:
        if value < threshold:
            break
        bin_index += 1

    return f"{_BASE_CLASS} {_BASE_CLASS}--{bin_index}"
