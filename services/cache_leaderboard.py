"""
HOTFIX STUB: All rebuild/warm functions are disabled to unblock uploads.
Restore the original file from services/cache_leaderboard_real.py after the meeting.
"""
def rebuild_all(*args, **kwargs): return None
def rebuild_for_scope(*args, **kwargs): return None
def rebuild_leaderboard_cache(*args, **kwargs): return None
def rebuild_leaderboards(*args, **kwargs): return None
def rebuild(*args, **kwargs): return None
def warm_all(*args, **kwargs): return None
def warm(*args, **kwargs): return None
def warm_leaderboard_cache(*args, **kwargs): return None
def schedule_rebuild(*args, **kwargs): return None
def schedule_warm(*args, **kwargs): return None
def ensure_cache_after_parse(*args, **kwargs): return None
def rebuild_after_parse(*args, **kwargs): return None
# If other helpers are imported elsewhere, import them lazily from the real module:
try:
    from .cache_leaderboard_real import (
        get_cached_leaderboard, get_cache_key, get_cache_client,
        # add any harmless read-only helpers here if needed
    )
except Exception:
    pass
