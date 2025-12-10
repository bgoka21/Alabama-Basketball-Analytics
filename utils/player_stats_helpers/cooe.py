"""Crimson On/Off Engine (COOE) helpers for game possessions.

These helpers mirror the possession-based logic used in the Sportscode CSV
ingest, but they pull directly from the database so the Custom Stats Table can
render without needing CSV uploads.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Iterable, Optional, Sequence, Tuple

from sqlalchemy import case, func, select

from models.database import PlayerPossession, Possession, ShotDetail, db


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    try:
        return None if denominator in (0, None) else numerator / denominator
    except Exception:  # pragma: no cover - defensive
        return None


def _normalize_game_ids(game_ids: Optional[Iterable[int]]) -> Tuple[int, ...]:
    if game_ids is None:
        return tuple()

    normalized = []
    for value in game_ids:
        try:
            as_int = int(value)
        except (TypeError, ValueError):
            continue
        normalized.append(as_int)

    return tuple(normalized)


def _build_game_possession_query(
    *, game_ids: Sequence[int], side: str, player_id: Optional[int] = None
):
    base = db.session.query(Possession.id).filter(Possession.game_id.in_(game_ids))

    normalized = (side or "").strip().lower()
    if normalized in {"offense", "defense"}:
        base = base.filter(func.lower(Possession.time_segment) == normalized)
    else:
        base = base.filter(func.lower(Possession.possession_side) == normalized)

    if player_id is not None:
        base = base.join(PlayerPossession, PlayerPossession.possession_id == Possession.id)
        base = base.filter(PlayerPossession.player_id == player_id)

    return base.distinct()


def _summarize_game_possessions(possession_query) -> Tuple[int, float]:
    """
    Return (possession_count, points_scored) for the provided possession ids.

    This should mirror the same possession logic used in the game reports and
    leaderboard helpers:

    - Start from all Offense/Defense rows ("runs")
    - Subtract any runs that are Neutral
    - Subtract any runs that are TEAM Off Reb extensions

    NOTE:
    - Only TEAM Off Reb (from the TEAM column) should reduce possessions.
      Player Off Reb blue-collar tags must NOT change the possession count.
    """
    poss_subquery = possession_query.subquery()

    event_counts = (
        db.session.query(
            ShotDetail.possession_id.label("pid"),
            func.sum(
                case(
                    (ShotDetail.event_type.ilike("%Neutral%"), 1),
                    else_=0,
                )
            ).label("neutral_hits"),
            func.sum(
                case((ShotDetail.event_type.ilike("%Off Reb%"), 1), else_=0)
            ).label("off_reb_hits"),
        )
        .filter(ShotDetail.possession_id.in_(select(poss_subquery.c.id)))
        .group_by(ShotDetail.possession_id)
        .subquery()
    )

    row = (
        db.session.query(
            func.count(poss_subquery.c.id).label("run_count"),
            func.coalesce(
                func.sum(
                    case((event_counts.c.neutral_hits > 0, 1), else_=0)
                ),
                0,
            ).label("neutral_count"),
            func.coalesce(
                func.sum(
                    case((event_counts.c.off_reb_hits > 0, 1), else_=0)
                ),
                0,
            ).label("off_reb_count"),
            func.coalesce(func.sum(Possession.points_scored), 0).label("points"),
        )
        .select_from(poss_subquery)
        .outerjoin(Possession, Possession.id == poss_subquery.c.id)
        .outerjoin(event_counts, event_counts.c.pid == poss_subquery.c.id)
        .one()
    )

    run_count = int(row.run_count or 0)
    neutral_count = int(row.neutral_count or 0)
    off_reb_count = int(row.off_reb_count or 0)

    possessions = max(run_count - neutral_count - off_reb_count, 0)
    return possessions, float(row.points or 0.0)


def get_game_on_off_stats(game_ids: Optional[Iterable[int]], player_id: int):
    """Return COOE on/off metrics for the given player across game_ids.

    PPP ON  = total points scored on possessions where the player is ON offense
              divided by offensive_possessions_on.
    PPP OFF = total team points scored on possessions where the player is OFF
              offense divided by offensive_possessions_off.
    Offensive Leverage = PPP ON − PPP OFF.

    All multi-game values are calculated from summed per-game totals (never by
    averaging per-game PPP outputs).
    """

    normalized_game_ids = tuple(dict.fromkeys(_normalize_game_ids(game_ids)))
    if not normalized_game_ids:
        return None

    def _accumulate(side: str, *, player: bool = False) -> Tuple[int, float]:
        """Sum possessions/points per game to avoid any cross-game averaging.

        The per-game summaries already mirror Sportscode, so adding them together
        guarantees multi-game requests are pure totals (never averages of PPP).
        """

        total_possessions = 0
        total_points = 0.0
        for gid in normalized_game_ids:
            poss, pts = _summarize_game_possessions(
                _build_game_possession_query(
                    game_ids=(gid,),
                    side=side,
                    player_id=player_id if player else None,
                )
            )
            total_possessions += poss
            total_points += pts
        return total_possessions, total_points

    team_off_poss, team_off_points = _accumulate("Offense")
    team_def_poss, team_def_points = _accumulate("Defense")

    player_off_poss, player_off_points = _accumulate("Offense", player=True)
    player_def_poss, player_def_points = _accumulate("Defense", player=True)

    off_possessions_off = max(team_off_poss - player_off_poss, 0)
    def_possessions_off = max(team_def_poss - player_def_poss, 0)

    ppp_on_offense = _safe_div(player_off_points, player_off_poss)
    ppp_on_defense = _safe_div(player_def_points, player_def_poss)
    ppp_off_offense = _safe_div(team_off_points - player_off_points, off_possessions_off)
    ppp_off_defense = _safe_div(team_def_points - player_def_points, def_possessions_off)

    return SimpleNamespace(
        offensive_possessions_on=player_off_poss,
        defensive_possessions_on=player_def_poss,
        offensive_possessions_off=off_possessions_off,
        defensive_possessions_off=def_possessions_off,
        ppp_on_offense=ppp_on_offense,
        ppp_on_defense=ppp_on_defense,
        ppp_off_offense=ppp_off_offense,
        ppp_off_defense=ppp_off_defense,
        adv_ppp_on_offense=ppp_on_offense,
        adv_ppp_on_defense=ppp_on_defense,
        adv_ppp_off_offense=ppp_off_offense,
        adv_ppp_off_defense=ppp_off_defense,
        adv_offensive_leverage=
            (ppp_on_offense - ppp_off_offense) if ppp_on_offense is not None and ppp_off_offense is not None else None,
        adv_defensive_leverage=
            (ppp_off_defense - ppp_on_defense) if ppp_on_defense is not None and ppp_off_defense is not None else None,
        adv_off_possession_pct=_safe_div(player_off_poss, team_off_poss),
        adv_def_possession_pct=_safe_div(player_def_poss, team_def_poss),
        team_offensive_possessions=team_off_poss,
        team_defensive_possessions=team_def_poss,
        points_on_offense=player_off_points,
        points_on_defense=player_def_points,
        points_off_offense=max(team_off_points - player_off_points, 0.0),
        points_off_defense=max(team_def_points - player_def_points, 0.0),
    )

