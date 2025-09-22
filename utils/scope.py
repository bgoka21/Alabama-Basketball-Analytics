# >>> SCOPE HELPERS START
from datetime import date
from typing import Optional, Tuple

from models.database import Practice
from sqlalchemy import and_


def get_last_practice_date(season_id: int, session_range: Optional[Tuple[date, date]] = None) -> Optional[date]:
    q = Practice.query.filter(Practice.season_id == season_id)
    if session_range:
        q = q.filter(and_(Practice.date >= session_range[0], Practice.date <= session_range[1]))
    last = q.order_by(Practice.date.desc()).first()
    return last.date if last else None


def resolve_scope(args, season_id: int, session_range: Optional[Tuple[date, date]]):
    """
    Returns (scope, start_dt, end_dt).
    - scope: 'last' | 'session' | 'season'
    - start_dt/end_dt: None if 'season'; single-day window if 'last'; session window if 'session'.
    """
    scope = (args.get("scope") or "last").lower()
    if scope not in ("last", "session", "season"):
        scope = "last"

    if scope == "season":
        return scope, None, None

    if scope == "session":
        # Use provided session_range (from Prompt #1). If missing, treat as season.
        if session_range:
            return scope, session_range[0], session_range[1]
        return "season", None, None

    # scope == 'last'
    last_dt = get_last_practice_date(season_id, session_range)
    if last_dt:
        return scope, last_dt, last_dt
    # If no practices present, fall back to season
    return "season", None, None
# <<< SCOPE HELPERS END
