# >>> SESSION FILTER LOGIC START
from datetime import date
from typing import Optional
from . import session_windows


def get_session_from_args(args):
    session_name = args.get("session")
    # Normalize but preserve exact labels
    if not session_name:
        return None
    if session_name.lower() == "all":
        return "All"
    return session_name


def apply_session_range(args, start_dt: Optional[date] = None, end_dt: Optional[date] = None):
    """
    Returns (start_dt, end_dt, session_name) where start/end may override provided values
    if a specific session is selected. If 'All' or None, returns original start/end.
    """
    session_name = get_session_from_args(args)
    if session_name and session_name != "All":
        rng = session_windows.resolve_session_range(session_name)
        if rng:
            return rng[0], rng[1], session_name
    return start_dt, end_dt, session_name or "All"
# <<< SESSION FILTER LOGIC END
