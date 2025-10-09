"""Helpers for mapping percentage metrics to CSS grade tokens."""

from __future__ import annotations

import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

from app.grades_config import GRADE_LABELS, GRADES

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


def _format_threshold(value: float, is_percent: bool) -> str:
    decimal_value = Decimal(str(value))
    if is_percent:
        if decimal_value == decimal_value.to_integral_value():
            return f"{int(decimal_value)}%"
        quantized = decimal_value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        return f"{format(quantized, 'f')}%"

    quantized = decimal_value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return format(quantized, "f")


def _fallback_label(metric_key: str) -> str:
    base = metric_key.replace("_pct", "")
    base = base.replace("_", " ")
    base = base.upper()
    return base + (" %" if metric_key.endswith("_pct") else "")


def grade_scale(metric_key: str) -> Optional[Dict[str, Any]]:
    """Return the grading scale buckets for ``metric_key``.

    The response mirrors the color bins that ``grade_token`` produces so
    templates can surface accurate legends directly from the shared config.
    """

    thresholds = GRADES.get(metric_key)
    if not thresholds:
        return None

    is_percent = max(thresholds) > 1.5
    unit_suffix = ""
    if not is_percent and metric_key == "pps":
        unit_suffix = " PPS"

    buckets: List[Dict[str, Any]] = []
    lower_bound: Optional[float] = None

    for index, threshold in enumerate(thresholds):
        upper_display = _format_threshold(threshold, is_percent)
        if lower_bound is None:
            range_label = f"< {upper_display}"
        else:
            lower_display = _format_threshold(lower_bound, is_percent)
            range_label = f"{lower_display} – < {upper_display}"
        if unit_suffix:
            range_label = f"{range_label}{unit_suffix}"

        buckets.append(
            {
                "index": index,
                "token": f"{_BASE_CLASS} {_BASE_CLASS}--{index}",
                "range": range_label,
                "badge": str(index + 1),
            }
        )
        lower_bound = threshold

    if lower_bound is not None:
        lower_display = _format_threshold(lower_bound, is_percent)
        range_label = f"≥ {lower_display}"
        if unit_suffix:
            range_label = f"{range_label}{unit_suffix}"
        buckets.append(
            {
                "index": len(thresholds),
                "token": f"{_BASE_CLASS} {_BASE_CLASS}--{len(thresholds)}",
                "range": range_label,
                "badge": str(len(thresholds) + 1),
            }
        )

    label = GRADE_LABELS.get(metric_key, _fallback_label(metric_key))

    return {
        "metric": metric_key,
        "label": label,
        "buckets": buckets,
        "is_percent": is_percent,
    }
