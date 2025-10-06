"""Background job definitions for leaderboard cache rebuilding."""

from __future__ import annotations

import logging
from typing import Sequence

from constants import LEADERBOARD_STAT_KEYS
from services.progress_store import set_progress

LOGGER = logging.getLogger(__name__)
STATS_TO_BUILD: Sequence[str] = tuple(LEADERBOARD_STAT_KEYS)


def _progress_key(season_id: int) -> str:
    return f"leaderboard:progress:{season_id}"


def rebuild_leaderboards_job(season_id: int) -> None:
    """Rebuild cached leaderboard payloads for ``season_id``."""

    total = len(STATS_TO_BUILD)
    key = _progress_key(season_id)

    if total == 0:
        LOGGER.info(
            "Leaderboard rebuild job finished immediately; no stat keys configured (season=%s)",
            season_id,
        )
        set_progress(key, 100, "Complete", done=True)
        return

    LOGGER.info(
        "Starting leaderboard rebuild job (season=%s, total_stats=%s)",
        season_id,
        total,
    )

    from services.leaderboard_cache import build_leaderboard_cache

    for index, stat_key in enumerate(STATS_TO_BUILD, start=1):
        try:
            build_leaderboard_cache(stat_key, season_id)
        except Exception as exc:  # pragma: no cover - surfaced to scheduler/logs
            LOGGER.exception(
                "Leaderboard rebuild job failed (season=%s, stat=%s)",
                season_id,
                stat_key,
            )
            set_progress(key, 0, f"Failed on {stat_key}", done=True, error=str(exc))
            raise

        percent = int(index * 100 / total)
        set_progress(key, percent, f"Built {stat_key} ({index}/{total})")

    set_progress(key, 100, "Complete", done=True)
    LOGGER.info(
        "Finished leaderboard rebuild job (season=%s, total_stats=%s)",
        season_id,
        total,
    )
