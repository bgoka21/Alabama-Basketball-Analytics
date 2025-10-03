from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import case, func, or_, and_
from sqlalchemy.orm import Query

from models.database import (
    db, PlayerStats, BlueCollarStats, Roster,
    Possession, PlayerPossession, ShotDetail,
    Game, Practice
)


@dataclass
class OnOffSummary:
    offensive_possessions_on: int
    defensive_possessions_on: int
    ppp_on_offense: Optional[float]
    ppp_on_defense: Optional[float]
    offensive_possessions_off: int
    defensive_possessions_off: int
    ppp_off_offense: Optional[float]
    ppp_off_defense: Optional[float]


def _coerce_date(value: Optional[object]) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"Unsupported date value: {value!r}")


def _normalize_labels(labels: Optional[object]) -> Set[str]:
    if not labels:
        return set()
    if isinstance(labels, str):
        iterable: Iterable[str] = [labels]
    elif isinstance(labels, dict):
        collected: List[str] = []
        for key, value in labels.items():
            if isinstance(value, bool):
                if value and isinstance(key, str):
                    collected.append(key)
            elif isinstance(value, str):
                collected.append(value)
            elif value and isinstance(key, str):
                collected.append(key)
        iterable = collected
    else:
        iterable = labels  # type: ignore[assignment]
    return {
        str(lbl).strip().upper()
        for lbl in iterable
        if isinstance(lbl, str) and lbl.strip()
    }


def _apply_possession_filters(
    query: Query,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Query:
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        query = query.filter(or_(*clauses))
    if start_dt or end_dt:
        query = (
            query.outerjoin(Game, Possession.game_id == Game.id)
            .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_dt:
            query = query.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_dt),
                    and_(
                        Possession.practice_id != None,
                        Practice.date >= start_dt,
                    ),
                )
            )
        if end_dt:
            query = query.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_dt),
                    and_(
                        Possession.practice_id != None,
                        Practice.date <= end_dt,
                    ),
                )
            )
    return query


def _apply_playerstats_filters(
    query: Query,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Query:
    if label_set:
        clauses = []
        for lbl in label_set:
            pattern = f"%{lbl}%"
            clauses.append(PlayerStats.shot_type_details.ilike(pattern))
            clauses.append(PlayerStats.stat_details.ilike(pattern))
        query = query.filter(or_(*clauses))
    if start_dt or end_dt:
        query = (
            query.outerjoin(Game, PlayerStats.game_id == Game.id)
            .outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        )
        if start_dt:
            query = query.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_dt),
                    and_(
                        PlayerStats.practice_id != None,
                        Practice.date >= start_dt,
                    ),
                )
            )
        if end_dt:
            query = query.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_dt),
                    and_(
                        PlayerStats.practice_id != None,
                        Practice.date <= end_dt,
                    ),
                )
            )
    return query


def _row_to_dict(row, keys: Tuple[str, ...]) -> Dict[str, float]:
    if row is None:
        return {key: 0 for key in keys}
    mapping = getattr(row, "_mapping", None)
    if mapping is None:  # pragma: no cover - compatibility with older SQLAlchemy
        mapping = row._asdict()
    return {key: (mapping.get(key) or 0) for key in keys}


def _get_player_possession_totals(
    player_id: int,
    roster: Roster,
    side: str,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Tuple[int, float]:
    q = (
        db.session.query(
            func.count(PlayerPossession.id),
            func.coalesce(func.sum(Possession.points_scored), 0),
        )
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
            Possession.possession_side == side,
        )
    )
    q = _apply_possession_filters(q, start_dt, end_dt, label_set)
    count, points = q.one()
    return int(count or 0), float(points or 0)


def _get_team_possession_totals(
    season_id: int,
    side: str,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Tuple[int, float]:
    q = (
        db.session.query(
            func.count(Possession.id),
            func.coalesce(func.sum(Possession.points_scored), 0),
        )
        .filter(
            Possession.season_id == season_id,
            Possession.possession_side == side,
        )
    )
    q = _apply_possession_filters(q, start_dt, end_dt, label_set)
    count, points = q.one()
    return int(count or 0), float(points or 0)


def _get_possession_ids(
    season_id: int,
    side: str,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Tuple[List[int], List[int]]:
    q = db.session.query(Possession.practice_id, Possession.game_id).filter(
        Possession.season_id == season_id,
        Possession.possession_side == side,
    )
    q = _apply_possession_filters(q, start_dt, end_dt, label_set)
    rows = q.distinct().all()
    practice_ids = [pid for pid, gid in rows if pid]
    game_ids = [gid for pid, gid in rows if gid]
    return practice_ids, game_ids


_OFFENSE_EVENT_COLUMNS: Tuple[str, ...] = (
    "turnovers_on",
    "off_reb_on",
    "team_off_reb_on",
    "fouls_on",
    "team_misses_on",
)


def _get_offense_events(
    player_id: int,
    roster: Roster,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Dict[str, float]:
    events_q = (
        db.session.query(
            func.coalesce(
                func.sum(case((ShotDetail.event_type == "Turnover", 1), else_=0)),
                0,
            ).label("turnovers_on"),
            func.coalesce(
                func.sum(case((ShotDetail.event_type == "Off Reb", 1), else_=0)),
                0,
            ).label("off_reb_on"),
            func.coalesce(
                func.sum(case((ShotDetail.event_type == "TEAM Off Reb", 1), else_=0)),
                0,
            ).label("team_off_reb_on"),
            func.coalesce(
                func.sum(case((ShotDetail.event_type == "Foul", 1), else_=0)),
                0,
            ).label("fouls_on"),
            func.coalesce(
                func.sum(
                    case(
                        (ShotDetail.event_type.in_(("ATR-", "2FG-", "3FG-")), 1),
                        else_=0,
                    )
                ),
                0,
            ).label("team_misses_on"),
        )
        .select_from(PlayerPossession)
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .join(ShotDetail, ShotDetail.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
            Possession.possession_side == "Offense",
        )
    )
    events_q = _apply_possession_filters(events_q, start_dt, end_dt, label_set)
    row = events_q.one_or_none()
    return _row_to_dict(row, _OFFENSE_EVENT_COLUMNS)


_DEFENSE_EVENT_COLUMNS: Tuple[str, ...] = (
    "opp_misses_on",
    "opp_team_off_reb_on",
    "opp_player_off_reb_on",
    "team_def_reb_on",
)


def _get_defense_events(
    player_id: int,
    roster: Roster,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> Dict[str, float]:
    events_q = (
        db.session.query(
            func.coalesce(
                func.sum(
                    case(
                        (ShotDetail.event_type.in_(("ATR-", "2FG-", "3FG-")), 1),
                        else_=0,
                    )
                ),
                0,
            ).label("opp_misses_on"),
            func.coalesce(
                func.sum(case((ShotDetail.event_type == "TEAM Off Reb", 1), else_=0)),
                0,
            ).label("opp_team_off_reb_on"),
            func.coalesce(
                func.sum(case((ShotDetail.event_type == "Off Reb", 1), else_=0)),
                0,
            ).label("opp_player_off_reb_on"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            ShotDetail.event_type.in_(("Def Reb", "TEAM Def Reb")),
                            1,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("team_def_reb_on"),
        )
        .select_from(PlayerPossession)
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .join(ShotDetail, ShotDetail.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
            Possession.possession_side == "Defense",
        )
    )
    events_q = _apply_possession_filters(events_q, start_dt, end_dt, label_set)
    row = events_q.one_or_none()
    return _row_to_dict(row, _DEFENSE_EVENT_COLUMNS)


def _get_player_stats_totals(
    roster: Roster,
    start_dt: Optional[date],
    end_dt: Optional[date],
    label_set: Set[str],
) -> SimpleNamespace:
    stats_query = PlayerStats.query.filter(
        PlayerStats.player_name == roster.player_name,
        PlayerStats.season_id == roster.season_id,
    )
    stats_query = _apply_playerstats_filters(stats_query, start_dt, end_dt, label_set)
    records = stats_query.all()
    if not records:
        return SimpleNamespace(
            turnovers=0,
            pot_assists=0,
            assists=0,
            atr_attempts=0,
            fg2_attempts=0,
            fg3_attempts=0,
            foul_by=0,
        )
    if label_set:
        from admin.routes import compute_filtered_totals

        totals = compute_filtered_totals(records, label_set)
    else:
        from admin.routes import aggregate_stats

        totals = aggregate_stats(records)
    return totals


def _fetch_personal_turnovers(
    roster: Roster,
    practice_ids: List[int],
    game_ids: List[int],
) -> float:
    q = db.session.query(func.coalesce(func.sum(PlayerStats.turnovers), 0)).filter(
        PlayerStats.season_id == roster.season_id,
        PlayerStats.player_name == roster.player_name,
    )
    if practice_ids:
        q = q.filter(PlayerStats.practice_id.in_(practice_ids))
    if game_ids:
        q = q.filter(PlayerStats.game_id.in_(game_ids))
    return float(q.scalar() or 0)


def _sum_team_off_rebounds(season_id: int) -> float:
    return float(
        db.session.query(func.coalesce(func.sum(BlueCollarStats.off_reb), 0))
        .filter(BlueCollarStats.season_id == season_id)
        .scalar()
        or 0
    )


def get_on_off_summary(
    player_id: int,
    date_from: Optional[object] = None,
    date_to: Optional[object] = None,
    labels: Optional[object] = None,
) -> OnOffSummary:
    roster = db.session.get(Roster, player_id)
    if not roster:
        return OnOffSummary(0, 0, None, None, 0, 0, None, None)

    start_dt = _coerce_date(date_from)
    end_dt = _coerce_date(date_to)
    label_set = _normalize_labels(labels)

    on_poss, on_pts = _get_player_possession_totals(
        player_id, roster, "Offense", start_dt, end_dt, label_set
    )
    team_off_poss, team_off_pts = _get_team_possession_totals(
        roster.season_id, "Offense", start_dt, end_dt, label_set
    )

    def_poss_on, def_pts_on = _get_player_possession_totals(
        player_id, roster, "Defense", start_dt, end_dt, label_set
    )
    team_def_poss, team_def_pts = _get_team_possession_totals(
        roster.season_id, "Defense", start_dt, end_dt, label_set
    )

    off_poss_off = max(team_off_poss - on_poss, 0)
    off_pts_off = max(team_off_pts - on_pts, 0.0)

    def_poss_off = max(team_def_poss - def_poss_on, 0)
    def_pts_off = max(team_def_pts - def_pts_on, 0.0)

    summary = OnOffSummary(
        offensive_possessions_on=on_poss,
        defensive_possessions_on=def_poss_on,
        ppp_on_offense=round(on_pts / on_poss, 2) if on_poss else 0.0,
        ppp_on_defense=round(def_pts_on / def_poss_on, 2) if def_poss_on else 0.0,
        offensive_possessions_off=off_poss_off,
        defensive_possessions_off=def_poss_off,
        ppp_off_offense=round(off_pts_off / off_poss_off, 2) if off_poss_off else 0.0,
        ppp_off_defense=round(def_pts_off / def_poss_off, 2) if def_poss_off else 0.0,
    )
    return summary


def get_turnover_rates_onfloor(
    player_id: int,
    date_from: Optional[object] = None,
    date_to: Optional[object] = None,
    labels: Optional[object] = None,
) -> Dict[str, Optional[float]]:
    roster = db.session.get(Roster, player_id)
    if not roster:
        return {
            "team_turnover_rate_on": None,
            "indiv_turnover_rate": None,
            "bamalytics_turnover_rate": None,
            "individual_team_turnover_pct": None,
        }

    start_dt = _coerce_date(date_from)
    end_dt = _coerce_date(date_to)
    label_set = _normalize_labels(labels)

    on_poss, _ = _get_player_possession_totals(
        player_id, roster, "Offense", start_dt, end_dt, label_set
    )
    offense_events = _get_offense_events(
        player_id, roster, start_dt, end_dt, label_set
    )
    practice_ids, game_ids = _get_possession_ids(
        roster.season_id, "Offense", start_dt, end_dt, label_set
    )
    personal_turnovers = _fetch_personal_turnovers(roster, practice_ids, game_ids)
    totals = _get_player_stats_totals(roster, start_dt, end_dt, label_set)

    player_turnovers = float(getattr(totals, "turnovers", 0) or 0)
    total_fga = float(
        (getattr(totals, "atr_attempts", 0) or 0)
        + (getattr(totals, "fg2_attempts", 0) or 0)
        + (getattr(totals, "fg3_attempts", 0) or 0)
    )
    denominator = (
        player_turnovers
        + total_fga
        + float(getattr(totals, "pot_assists", 0) or 0)
        + float(getattr(totals, "assists", 0) or 0)
    )
    team_turnovers_on = float(offense_events.get("turnovers_on", 0))

    # Example keys: {'team_turnover_rate_on', 'indiv_turnover_rate',
    #                'bamalytics_turnover_rate', 'individual_team_turnover_pct'}
    return {
        "team_turnover_rate_on": round(team_turnovers_on / on_poss * 100, 1)
        if on_poss
        else 0.0,
        "indiv_turnover_rate": round(personal_turnovers / on_poss * 100, 1)
        if on_poss
        else 0.0,
        "bamalytics_turnover_rate": round(player_turnovers / denominator * 100, 1)
        if denominator
        else 0.0,
        "individual_team_turnover_pct": round(
            personal_turnovers / team_turnovers_on * 100, 1
        )
        if team_turnovers_on
        else 0.0,
    }


def get_rebound_rates_onfloor(
    player_id: int,
    date_from: Optional[object] = None,
    date_to: Optional[object] = None,
    labels: Optional[object] = None,
) -> Dict[str, Optional[float]]:
    roster = db.session.get(Roster, player_id)
    if not roster:
        return {"off_reb_rate_on": None, "def_reb_rate_on": None}

    start_dt = _coerce_date(date_from)
    end_dt = _coerce_date(date_to)
    label_set = _normalize_labels(labels)

    on_poss, _ = _get_player_possession_totals(
        player_id, roster, "Offense", start_dt, end_dt, label_set
    )
    team_off_poss, _ = _get_team_possession_totals(
        roster.season_id, "Offense", start_dt, end_dt, label_set
    )
    offense_events = _get_offense_events(
        player_id, roster, start_dt, end_dt, label_set
    )

    team_misses = float(offense_events.get("team_misses_on", 0) or 0)
    recorded_team_oreb = float(offense_events.get("team_off_reb_on", 0) or 0)
    if recorded_team_oreb > 0:
        team_oreb_on = recorded_team_oreb
    else:
        total_team_oreb = _sum_team_off_rebounds(roster.season_id)
        team_oreb_on = (
            total_team_oreb * (on_poss / team_off_poss)
            if team_off_poss and total_team_oreb
            else 0.0
        )
    off_reb_rate = (
        round(team_oreb_on / team_misses * 100, 1)
        if team_misses
        else 0.0
    )

    defense_events = _get_defense_events(
        player_id, roster, start_dt, end_dt, label_set
    )
    opp_misses = float(defense_events.get("opp_misses_on", 0) or 0)
    opp_off_reb = float(defense_events.get("opp_team_off_reb_on", 0) or 0)
    opp_player_off_reb = float(defense_events.get("opp_player_off_reb_on", 0) or 0)
    team_def_reb = float(defense_events.get("team_def_reb_on", 0) or 0)

    opp_off_total = opp_off_reb + opp_player_off_reb
    if team_def_reb <= 0 and opp_misses:
        team_def_reb = max(opp_misses - opp_off_total, 0.0)
    def_den = team_def_reb + opp_off_total
    def_reb_rate = (
        round(team_def_reb / def_den * 100, 1)
        if def_den
        else 0.0
    )

    # Example sanity key set: {'off_reb_rate_on', 'def_reb_rate_on'}
    return {
        "off_reb_rate_on": off_reb_rate,
        "def_reb_rate_on": def_reb_rate,
    }


def get_on_court_metrics(player_id, start_date=None, end_date=None, labels=None):
    """Return on-court offensive metrics for a player."""
    roster = db.session.get(Roster, player_id)
    if not roster:
        return {}

    label_set = {lbl.strip().upper() for lbl in labels or [] if lbl.strip()}
    offense_sides = ("Offense", "Crimson", "White")

    # Determine the player's squad based on practice scrimmage data. If the
    # player appears in possessions tagged with a specific squad name (e.g.
    # "Crimson" or "White"), treat the most common value as the player's squad
    # and restrict team calculations to that side.  This allows us to exclude
    # possessions from the opposing squad when computing PPP_on/PPP_off.
    squad_row = (
        db.session.query(Possession.possession_side, func.count(Possession.id))
        .join(PlayerPossession, Possession.id == PlayerPossession.possession_id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
            Possession.possession_side.in_(('Crimson', 'White')),
        )
        .group_by(Possession.possession_side)
        .order_by(func.count(Possession.id).desc())
        .first()
    )
    player_squad = squad_row[0] if squad_row else None

    poss_q = (
        db.session.query(
            func.count(PlayerPossession.id),
            func.coalesce(func.sum(Possession.points_scored), 0),
        )
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            PlayerPossession.player_id == player_id,
            Possession.season_id == roster.season_id,
        )
    )
    if player_squad:
        poss_q = poss_q.filter(Possession.possession_side == player_squad)
    else:
        poss_q = poss_q.filter(Possession.possession_side.in_(offense_sides))
    if start_date or end_date:
        poss_q = (
            poss_q.outerjoin(Game, Possession.game_id == Game.id)
                   .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_date:
            poss_q = poss_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_date),
                    and_(Possession.practice_id != None, Practice.date >= start_date),
                )
            )
        if end_date:
            poss_q = poss_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_date),
                    and_(Possession.practice_id != None, Practice.date <= end_date),
                )
            )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        poss_q = poss_q.filter(or_(*clauses))
    ON_poss, ON_pts = poss_q.one()

    team_q = (
        db.session.query(
            func.count(Possession.id),
            func.coalesce(func.sum(Possession.points_scored), 0),
        )
        .filter(Possession.season_id == roster.season_id)
    )
    if player_squad:
        team_q = team_q.filter(Possession.possession_side == player_squad)
    else:
        team_q = team_q.filter(Possession.possession_side.in_(offense_sides))
    if start_date or end_date:
        team_q = (
            team_q.outerjoin(Game, Possession.game_id == Game.id)
                    .outerjoin(Practice, Possession.practice_id == Practice.id)
        )
        if start_date:
            team_q = team_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date >= start_date),
                    and_(Possession.practice_id != None, Practice.date >= start_date),
                )
            )
        if end_date:
            team_q = team_q.filter(
                or_(
                    and_(Possession.game_id != None, Game.game_date <= end_date),
                    and_(Possession.practice_id != None, Practice.date <= end_date),
                )
            )
    if label_set:
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        team_q = team_q.filter(or_(*clauses))
    TEAM_poss, TEAM_pts = team_q.one()

    def ev_count(ev_type: str) -> int:
        q = (
            db.session.query(func.count(ShotDetail.id))
            .join(Possession, ShotDetail.possession_id == Possession.id)
            .join(PlayerPossession, Possession.id == PlayerPossession.possession_id)
            .filter(
                PlayerPossession.player_id == player_id,
                Possession.season_id == roster.season_id,
                ShotDetail.event_type == ev_type,
            )
        )
        if player_squad:
            q = q.filter(Possession.possession_side == player_squad)
        else:
            q = q.filter(Possession.possession_side.in_(offense_sides))
        if start_date or end_date:
            q = q.outerjoin(Game, Possession.game_id == Game.id).outerjoin(Practice, Possession.practice_id == Practice.id)
            if start_date:
                q = q.filter(
                    or_(
                        and_(Possession.game_id != None, Game.game_date >= start_date),
                        and_(Possession.practice_id != None, Practice.date >= start_date),
                    )
                )
            if end_date:
                q = q.filter(
                    or_(
                        and_(Possession.game_id != None, Game.game_date <= end_date),
                        and_(Possession.practice_id != None, Practice.date <= end_date),
                    )
                )
        if label_set:
            q = q.filter(or_(*[Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]))
        return q.scalar() or 0

    turnovers_on = ev_count("Turnover")
    # Personal off rebounds and fouls are tracked in BlueCollarStats and
    # PlayerStats respectively. Pull those aggregates using the same
    # date/label filters applied above so values mirror the leaderboard.
    bc_q = BlueCollarStats.query.filter(
        BlueCollarStats.player_id == player_id,
        BlueCollarStats.season_id == roster.season_id,
    )
    ps_filter_q = PlayerStats.query.filter(
        PlayerStats.player_name == roster.player_name,
        PlayerStats.season_id == roster.season_id,
    )
    if start_date or end_date:
        bc_q = bc_q.outerjoin(Game, BlueCollarStats.game_id == Game.id)
        bc_q = bc_q.outerjoin(Practice, BlueCollarStats.practice_id == Practice.id)
        ps_filter_q = ps_filter_q.outerjoin(Game, PlayerStats.game_id == Game.id)
        ps_filter_q = ps_filter_q.outerjoin(Practice, PlayerStats.practice_id == Practice.id)
        if start_date:
            bc_q = bc_q.filter(
                or_(
                    and_(BlueCollarStats.game_id != None, Game.game_date >= start_date),
                    and_(BlueCollarStats.practice_id != None, Practice.date >= start_date),
                )
            )
            ps_filter_q = ps_filter_q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date >= start_date),
                    and_(PlayerStats.practice_id != None, Practice.date >= start_date),
                )
            )
        if end_date:
            bc_q = bc_q.filter(
                or_(
                    and_(BlueCollarStats.game_id != None, Game.game_date <= end_date),
                    and_(BlueCollarStats.practice_id != None, Practice.date <= end_date),
                )
            )
            ps_filter_q = ps_filter_q.filter(
                or_(
                    and_(PlayerStats.game_id != None, Game.game_date <= end_date),
                    and_(PlayerStats.practice_id != None, Practice.date <= end_date),
                )
            )
    if label_set:
        bc_q = bc_q.join(
            PlayerStats,
            and_(
                PlayerStats.season_id == BlueCollarStats.season_id,
                PlayerStats.player_name == roster.player_name,
                PlayerStats.practice_id == BlueCollarStats.practice_id,
                PlayerStats.game_id == BlueCollarStats.game_id,
            ),
        )
        clauses = [
            PlayerStats.shot_type_details.ilike(f"%{lbl}%") | PlayerStats.stat_details.ilike(f"%{lbl}%")
            for lbl in label_set
        ]
        bc_q = bc_q.filter(or_(*clauses))
        ps_clauses = [
            PlayerStats.shot_type_details.ilike(f"%{lbl}%") | PlayerStats.stat_details.ilike(f"%{lbl}%")
            for lbl in label_set
        ]
        ps_filter_q = ps_filter_q.filter(or_(*ps_clauses))
    off_reb_on = bc_q.with_entities(func.coalesce(func.sum(BlueCollarStats.off_reb), 0)).scalar() or 0
    records = ps_filter_q.all()
    if label_set:
        from admin.routes import compute_filtered_totals
        totals = compute_filtered_totals(records, label_set)
    else:
        from admin.routes import aggregate_stats
        totals = aggregate_stats(records)
    fouls_drawn_on = totals.foul_by
    player_turnovers = totals.turnovers
    team_missed_on = sum(ev_count(ev) for ev in ["ATR-", "2FG-", "3FG-"])
    total_fga = totals.atr_attempts + totals.fg2_attempts + totals.fg3_attempts
    denominator = (
        player_turnovers
        + total_fga
        + totals.pot_assists
        + totals.assists
    )
    indiv_team_to_pct = (
        round(player_turnovers / turnovers_on * 100, 1)
        if turnovers_on
        else None
    )


    return {
        'offensive_poss_on': round(ON_poss, 0),
        'ppp_on': round(ON_pts / ON_poss, 2) if ON_poss else None,
        'team_turnover_rate_on': round(turnovers_on / ON_poss * 100, 1) if ON_poss else None,
        'indiv_turnover_rate': round(player_turnovers / ON_poss * 100, 1) if ON_poss else None,
        'bamalytics_turnover_rate': round(player_turnovers / denominator * 100, 1) if denominator else None,
        'individual_team_turnover_pct': indiv_team_to_pct,
        'ind_off_reb_pct': round(off_reb_on / team_missed_on * 100, 1) if team_missed_on else None,
        'ind_fouls_drawn_pct': round(fouls_drawn_on / ON_poss * 100, 1) if ON_poss else None,
    }


def get_player_overall_stats(player_id: int, labels=None):
    """Return season totals and on-court metrics for one player."""
    # Import heavy helpers lazily to avoid circular deps
    from admin.routes import aggregate_stats, compute_filtered_totals, compute_filtered_blue

    roster = db.session.get(Roster, player_id)
    if not roster:
        return SimpleNamespace()

    label_set = {lbl.strip().upper() for lbl in labels or [] if lbl.strip()}

    # --- PlayerStats/BlueCollar aggregates ---
    stats_query = PlayerStats.query.filter(
        PlayerStats.player_name == roster.player_name,
        PlayerStats.season_id == roster.season_id,
    )
    records = stats_query.all()

    if label_set:
        totals = compute_filtered_totals(records, label_set)
        blue = compute_filtered_blue(records, label_set)
    else:
        totals = aggregate_stats(records)
        blue = compute_filtered_blue(records, None)

    stats_map = totals.__dict__.copy()
    total_shots = (
        stats_map.get("atr_attempts", 0)
        + stats_map.get("fg2_attempts", 0)
        + stats_map.get("fg3_attempts", 0)
    )
    stats_map["two_fg_pct"] = (
        round(stats_map.get("fg2_makes", 0) / stats_map.get("fg2_attempts", 0) * 100, 1)
        if stats_map.get("fg2_attempts", 0)
        else 0.0
    )
    stats_map["two_fg_freq_pct"] = (
        round(stats_map.get("fg2_attempts", 0) / total_shots * 100, 1) if total_shots else 0.0
    )
    # rename fg3_pct/freq to three_fg_*
    stats_map["three_fg_pct"] = stats_map.pop("fg3_pct", 0)
    stats_map["three_fg_freq_pct"] = stats_map.pop("fg3_freq_pct", 0)

    stats_map.update(blue.__dict__)

    # --- On-court metrics ---
    on_court = get_on_court_metrics(
        player_id,
        labels=labels,
    )
    stats_map.update(on_court)

    return SimpleNamespace(**stats_map)
