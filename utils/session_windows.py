# >>> SESSION WINDOWS START
from datetime import date

# EDIT THESE WINDOWS to match our calendar
SESSION_WINDOWS = {
    "Summer 1": (date(2025, 6, 1), date(2025, 6, 30)),
    "Summer 2": (date(2025, 7, 1), date(2025, 7, 31)),
    "Fall": (date(2025, 9, 1), date(2025, 10, 14)),
    "Official Practice": (date(2025, 10, 15), date(2025, 11, 30)),
}

def resolve_session_range(name: str):
    if not name or name.lower() == "all":
        return None
    return SESSION_WINDOWS.get(name)
# <<< SESSION WINDOWS END
