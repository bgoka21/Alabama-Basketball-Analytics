"""
HOTFIX: temporarily disable leaderboard cache rebuilds to unblock uploads.
Any attribute imported from here resolves to a no-op, including cache_build_all.
Revert this file after the meeting.
"""
try:
    from services import cache_leaderboard as _base
except Exception:
    _base = None

def _noop(*args, **kwargs):
    return None

# Export all non-private attributes from the real module (reads are safe)
if _base is not None:
    for k, v in _base.__dict__.items():
        if not k.startswith("_"):
            globals()[k] = v

# Known rebuild/warm names we want to force to no-op
_NOOP_NAMES = {
    "rebuild_all",
    "rebuild_for_scope",
    "rebuild_leaderboard_cache",
    "rebuild_leaderboards",
    "rebuild",
    "warm_all",
    "warm",
    "warm_leaderboard_cache",
    "schedule_rebuild",
    "schedule_warm",
    "ensure_cache_after_parse",
    "rebuild_after_parse",
    "cache_build_all",   # <-- specifically requested by admin/routes.py
}

for _name in _NOOP_NAMES:
    globals()[_name] = _noop

# Final safety net: ANY missing attribute becomes a no-op so `from ... import X` never breaks.
def __getattr__(name):
    return _noop
