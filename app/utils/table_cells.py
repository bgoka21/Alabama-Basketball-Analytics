"""Helper utilities for preparing table cell values with sortable metadata."""

from __future__ import annotations

from typing import Any, Dict, Optional


def _make_cell(display: str, data_value: Optional[str]) -> Dict[str, Any]:
    return {
        "display": display,
        "data_value": data_value if data_value is not None else "",
    }


def pct(value: Optional[float]) -> Dict[str, Any]:
    """Return a percentage cell from a 0..1 value."""

    if value is None:
        return _make_cell("-", "")
    return _make_cell(f"{value * 100:.1f}%", f"{value:.6f}")


def ratio(makes: Optional[float], atts: Optional[float], show_pct: bool = True) -> Dict[str, Any]:
    """Return a makes/attempts cell with optional percent display."""

    if makes is None or atts is None:
        return _make_cell("-", "")

    pct_val = (makes / atts) if atts else 0.0
    pct_display = f" ({pct_val * 100:.1f}%)" if show_pct else ""
    display = f"{makes}/{atts}{pct_display}"
    return _make_cell(display, f"{pct_val:.6f}")


def num(value: Optional[float]) -> Dict[str, Any]:
    """Return a numeric cell with consistent data value formatting."""

    if value is None:
        return _make_cell("-", "")
    return _make_cell(f"{value}", f"{float(value):.6f}")


def dt_iso(iso_str: Optional[str], pretty: Optional[str]) -> Dict[str, Any]:
    """Return a date cell sorted by ISO timestamp while displaying a pretty value."""

    if not iso_str:
        display = pretty or "-"
        return _make_cell(display, "")
    return _make_cell(pretty or iso_str, iso_str)

