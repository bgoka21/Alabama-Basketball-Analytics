from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple


DualContextResult = Dict[str, Any]


def _default_context() -> DualContextResult:
    return {
        "season_rows": [],
        "season_team_totals": None,
        "last_rows": None,
        "last_team_totals": None,
        "last_practice_date": None,
    }


def _normalize_compute_result(result: Any) -> Tuple[Any, Any]:
    """Return ``(team_totals, rows)`` from a compute function response."""

    if isinstance(result, dict):
        return result.get("team_totals"), result.get("rows")

    if not isinstance(result, tuple):
        raise TypeError(f"Unexpected compute_fn return type: {type(result)!r}")

    if len(result) == 3:
        first, second, third = result
        if isinstance(second, list):
            return third, second
        if isinstance(first, list):
            return third, first
        if isinstance(third, list):
            return second, third
        return third, second

    if len(result) == 2:
        first, second = result
        if isinstance(second, list) or second is None:
            return first, second
        if isinstance(first, list) or first is None:
            return second, first
        return first, second

    raise ValueError(
        "compute_fn is expected to return a tuple of length 2 or 3, "
        f"got length {len(result)}"
    )


def build_dual_context(
    *,
    season_id: Optional[int],
    compute_fn: Callable[..., Any],
    stat_key: Optional[str] = None,
    label_set: Optional[Any] = None,
    extra_kwargs: Optional[Dict[str, Any]] = None,
) -> DualContextResult:
    """Return combined season and last-practice leaderboard contexts."""

    context = _default_context()

    if season_id is None:
        return context

    extra_kwargs = extra_kwargs or {}

    season_result = compute_fn(
        stat_key=stat_key,
        season_id=season_id,
        start_dt=None,
        end_dt=None,
        label_set=label_set,
        **extra_kwargs,
    )
    season_team_totals, season_rows = _normalize_compute_result(season_result)
    context.update(
        {
            "season_rows": season_rows,
            "season_team_totals": season_team_totals,
        }
    )

    from app.services.last_practice import get_last_practice  # inline to avoid circular import

    last_practice = get_last_practice(season_id)
    if last_practice and getattr(last_practice, "date", None):
        last_result = compute_fn(
            stat_key=stat_key,
            season_id=season_id,
            start_dt=last_practice.date,
            end_dt=last_practice.date,
            label_set=label_set,
            **extra_kwargs,
        )
        last_team_totals, last_rows = _normalize_compute_result(last_result)
        context.update(
            {
                "last_rows": last_rows,
                "last_team_totals": last_team_totals,
                "last_practice_date": last_practice.date,
            }
        )

    return context
