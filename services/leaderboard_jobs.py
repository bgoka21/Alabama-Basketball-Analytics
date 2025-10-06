"""Background job definitions for rebuilding leaderboard caches."""

from __future__ import annotations

from flask import current_app

from services.progress_store import set_progress
from services.leaderboard_cache import (
    build_leaderboard_cache,
    list_all_leaderboard_stats,
)


def rebuild_leaderboards_job(season_id: int) -> None:
    """Rebuild leaderboard caches for ``season_id`` inside an app context."""

    app = current_app
    log = app.logger
    key = f"leaderboard:progress:{season_id}"
    stats = list_all_leaderboard_stats()
    total = len(stats) or 1

    try:
        log.info(f"[LEADERS] START season={season_id} total={total}")
        set_progress(key, 1, "Starting…")

        for i, stat_key in enumerate(stats, start=1):
            build_leaderboard_cache(stat_key, season_id)
            pct = max(1, int(i * 100 / total))
            set_progress(key, pct, f"Built {stat_key} ({i}/{total})")

        set_progress(key, 100, "Complete", done=True)
        log.info(f"[LEADERS] DONE season={season_id}")
    except Exception as exc:  # pragma: no cover - surfaced to scheduler/logs
        log.exception(f"[LEADERS] FAILED season={season_id}: {exc}")
        set_progress(key, 0, "Failed", done=True, error=str(exc))
        raise
