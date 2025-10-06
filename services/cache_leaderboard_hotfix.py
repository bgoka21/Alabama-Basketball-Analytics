"""
HOTFIX: temporarily disable leaderboard cache rebuilds to unblock uploads.
This module proxies everything from services.cache_leaderboard EXCEPT any
rebuild/warm functions, which are replaced with no-ops.
Revert this file once parsing/caching is fixed.
"""
try:
    from services import cache_leaderboard as _base
except Exception:
    _base = None

# Names we will override with no-ops
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
}

def _noop(*args, **kwargs):
    return None

# Export base attributes first
if _base is not None:
    for k, v in _base.__dict__.items():
        if not k.startswith("_") and k not in _NOOP_NAMES:
            globals()[k] = v

# Overlay our no-ops (these will exist even if base didn't define them)
for _name in _NOOP_NAMES:
    globals()[_name] = _noop
