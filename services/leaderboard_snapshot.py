"""Helpers for building and refreshing leaderboard snapshot data."""
from __future__ import annotations

from datetime import date
from typing import Iterable, Optional, Sequence

from models.database import db, Season
from models.leaderboard_snapshot import LeaderboardSnapshot
from stats_config import LEADERBOARD_STATS
from utils.cache_utils import normalize_label_set, invalidate_leaderboard_cache
from admin.routes import build_leaderboard_baseline


def refresh_snapshot(
    stat_key: str,
    season_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    labels: Optional[Iterable[str]] = None,
    *,
    commit: bool = True,
    invalidate_cache: bool = True,
):
    """Refresh a single snapshot entry from live aggregates."""

    normalized_labels = normalize_label_set(labels)
    baseline = build_leaderboard_baseline(
        stat_key,
        season_id,
        start_dt=start_date,
        end_dt=end_date,
        label_set=labels,
    )
    payload = {
        "player_totals": baseline.get("player_totals") or {},
        "shot_details": baseline.get("shot_details") or {},
        "all_players": baseline.get("all_players") or [],
        "leaderboard": baseline.get("leaderboard") or [],
        "team_totals": baseline.get("team_totals"),
    }
    snapshot = LeaderboardSnapshot.upsert(
        season_id,
        stat_key,
        LeaderboardSnapshot.normalize_date(start_date),
        LeaderboardSnapshot.normalize_date(end_date),
        normalized_labels,
        payload,
    )

    if invalidate_cache:
        invalidate_leaderboard_cache(
            season_id,
            stat_key=stat_key,
            start_dt=start_date,
            end_dt=end_date,
            label_set=labels,
        )

    if commit:
        db.session.commit()

    return snapshot


def refresh_season_baselines(
    season_id: int,
    *,
    stat_keys: Optional[Iterable[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    labels: Optional[Iterable[str]] = None,
    commit: bool = True,
    invalidate_cache: bool = True,
) -> int:
    """Refresh snapshots for a season across multiple stat keys."""

    keys = list(stat_keys) if stat_keys is not None else [cfg["key"] for cfg in LEADERBOARD_STATS]
    total = 0
    for key in keys:
        refresh_snapshot(
            key,
            season_id,
            start_date=start_date,
            end_date=end_date,
            labels=labels,
            commit=False,
            invalidate_cache=invalidate_cache,
        )
        total += 1

    if commit:
        db.session.commit()

    return total


def get_season_ids() -> Sequence[int]:
    """Return the list of known season identifiers."""

    return [season.id for season in Season.query.order_by(Season.id).all()]
