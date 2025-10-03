import click

from datetime import datetime

from models.database import db
from services.leaderboard_snapshot import (
    get_season_ids,
    refresh_season_baselines,
    refresh_snapshot,
)
from stats_config import LEADERBOARD_STATS


@click.command("refresh_leaderboard_snapshots")
@click.option("--season-id", "season_ids", type=int, multiple=True, help="Limit refresh to specific season ids.")
@click.option("--stat-key", "stat_keys", type=str, multiple=True, help="Limit refresh to one or more leaderboard stat keys.")
@click.option("--start-date", type=click.DateTime(["%Y-%m-%d"]), help="Optional inclusive start date filter (YYYY-MM-DD).")
@click.option("--end-date", type=click.DateTime(["%Y-%m-%d"]), help="Optional inclusive end date filter (YYYY-MM-DD).")
@click.option("--label", "labels", multiple=True, help="Repeatable label filter to apply to the snapshot slice.")
@click.option("--no-cache-invalidate", is_flag=True, default=False, help="Skip cache invalidation after refresh.")
def refresh_leaderboard_snapshots(
    season_ids,
    stat_keys,
    start_date,
    end_date,
    labels,
    no_cache_invalidate,
):
    """Refresh persisted leaderboard snapshot slices."""

    start = start_date.date() if isinstance(start_date, datetime) else None
    end = end_date.date() if isinstance(end_date, datetime) else None
    seasons = list(season_ids) if season_ids else list(get_season_ids())
    keys = list(stat_keys) if stat_keys else [cfg["key"] for cfg in LEADERBOARD_STATS]
    label_list = list(labels) if labels else None

    total = 0
    for sid in seasons:
        if start is not None or end is not None or label_list:
            for key in keys:
                refresh_snapshot(
                    key,
                    sid,
                    start_date=start,
                    end_date=end,
                    labels=label_list,
                    commit=False,
                    invalidate_cache=not no_cache_invalidate,
                )
                total += 1
        else:
            total += refresh_season_baselines(
                sid,
                stat_keys=keys,
                start_date=start,
                end_date=end,
                labels=None,
                commit=False,
                invalidate_cache=not no_cache_invalidate,
            )

    db.session.commit()
    click.echo(f"Refreshed {total} snapshot(s) across {len(seasons)} season(s).")
