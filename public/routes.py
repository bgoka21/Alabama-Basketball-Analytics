# basketball_analytics/public/routes.py

from flask import Blueprint, request, render_template, redirect, url_for
from flask_login import login_required
from sqlalchemy import func, desc, and_, case
from collections import defaultdict
import json
from models.database import (
    db,
    BlueCollarStats,
    PlayerStats,
    Game,
    Season,
    TeamStats,
    PlayerPossession,
    Possession,
    Practice,
    Roster,
)


public_bp = Blueprint(
    "public",
    __name__,
    template_folder="templates/public",  # points at public/templates/public/
)


# ───────────────────────────────────────────────
#  Helper functions
# ───────────────────────────────────────────────


def get_current_season_id():
    """Return the ID of the most recent season (by start_date)."""
    latest = Season.query.order_by(Season.start_date.desc()).first()
    return latest.id if latest else None


def get_all_game_ids_for_current_season():
    """Return a list of all game IDs in the current season."""
    season_id = get_current_season_id()
    if not season_id:
        return []
    return [g.id for g in Game.query.filter_by(season_id=season_id).all()]


def get_last_n_game_ids(n):
    """Return the IDs of the last n games by date."""
    return [g.id for g in Game.query.order_by(Game.game_date.desc()).limit(n).all()]


# ───────────────────────────────────────────────
#  Root → Login redirect
# ───────────────────────────────────────────────


@public_bp.route("/", methods=["GET"])
def root():
    # always send new visitors to the login page
    return redirect(url_for("admin.login"))


# ───────────────────────────────────────────────
#  Homepage with Leaderboards
# ───────────────────────────────────────────────


@public_bp.route("/game_home", methods=["GET"])
@login_required
def game_homepage():
    # 1) Read filter options from query string
    filter_opt = request.args.get("filter", "season")  # 'season', 'last5', 'true_data'
    view_opt = request.args.get("view", "season")  # reserved for future use
    # Read sort choice from query string (default to total BCP)
    sort_by = request.args.get("sort", "bcp")  # 'bcp' or 'efficiency'

    # 2) Pick games to include
    if filter_opt == "last5":
        game_ids = get_last_n_game_ids(5)
    else:
        # both 'season' and 'true_data' use the full season
        game_ids = get_all_game_ids_for_current_season()

    # 3) Attempt‐thresholds: only apply for season & last5
    min_3fg = None if filter_opt == "true_data" else 10
    min_atr = None if filter_opt == "true_data" else 10

    # ─── 4A) Blue Collar Points Leaders ──────────────
    bcp_sub = (
        db.session.query(
            PlayerStats.player_name.label("player_name"),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label(
                "total_bcp"
            ),
        )
        .join(BlueCollarStats, BlueCollarStats.player_id == PlayerStats.id)
        .filter(BlueCollarStats.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
        .subquery()
    )

    bcp_leaders = (
        db.session.query(bcp_sub.c.player_name, bcp_sub.c.total_bcp)
        .order_by(desc(bcp_sub.c.total_bcp))
        .limit(10)
        .all()
    )

    #  ───────────────────────────────────────────
    #  Determine winning games among our selection
    #  ───────────────────────────────────────────
    winning_game_ids = [
        g.id
        for g in Game.query.filter(
            Game.id.in_(game_ids), Game.result.ilike("win")
        ).all()
    ]

    # ─── 4B) Hard Hat Winners (only in wins) ──────────────────────────
    # 1) Sum each player’s BCP in each winning game
    player_bcp = (
        db.session.query(
            BlueCollarStats.player_id.label("player_id"),
            BlueCollarStats.game_id.label("game_id"),
            (
                func.coalesce(func.sum(BlueCollarStats.def_reb), 0)
                + func.coalesce(func.sum(BlueCollarStats.off_reb), 0)
                + func.coalesce(func.sum(BlueCollarStats.misc), 0)
                + func.coalesce(func.sum(BlueCollarStats.deflection), 0)
                + func.coalesce(func.sum(BlueCollarStats.steal), 0)
                + func.coalesce(func.sum(BlueCollarStats.block), 0)
                + func.coalesce(func.sum(BlueCollarStats.floor_dive), 0)
                + func.coalesce(func.sum(BlueCollarStats.charge_taken), 0)
                + func.coalesce(func.sum(BlueCollarStats.reb_tip), 0)
            ).label("bcp"),
        )
        .filter(BlueCollarStats.game_id.in_(winning_game_ids))
        .group_by(BlueCollarStats.game_id, BlueCollarStats.player_id)
        .subquery()
    )

    # 2) Find each game’s max BCP among winners
    max_bcp_sub = (
        db.session.query(
            player_bcp.c.game_id, func.max(player_bcp.c.bcp).label("max_bcp")
        )
        .group_by(player_bcp.c.game_id)
        .subquery()
    )

    # 3) Count how many times each player hit that max in a winning game
    hard_hats = (
        db.session.query(PlayerStats.player_name, func.count().label("hard_hat_count"))
        .join(player_bcp, player_bcp.c.player_id == PlayerStats.id)
        .join(
            max_bcp_sub,
            and_(
                player_bcp.c.game_id == max_bcp_sub.c.game_id,
                player_bcp.c.bcp == max_bcp_sub.c.max_bcp,
            ),
        )
        # only count games where someone actually scored >0 BCP
        .filter(max_bcp_sub.c.max_bcp > 0)
        .group_by(PlayerStats.player_name)
        .order_by(desc("hard_hat_count"))
        .limit(10)
        .all()
    )

    # ─── 4C) 3FG% Leaders ──────────────────────────────
    q3 = (
        PlayerStats.query.with_entities(
            PlayerStats.player_name,
            func.sum(PlayerStats.fg3_makes).label("fg3m"),
            func.sum(PlayerStats.fg3_attempts).label("fg3a"),
            (
                func.sum(PlayerStats.fg3_makes)
                / func.nullif(func.sum(PlayerStats.fg3_attempts), 0)
                * 100
            ).label("fg3_pct"),
        )
        .filter(PlayerStats.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
    )
    if min_3fg:
        # only players whose **total** 3FG attempts ≥ threshold
        q3 = q3.having(func.sum(PlayerStats.fg3_attempts) >= min_3fg)
    fg3_leaders = q3.order_by(desc("fg3_pct")).limit(10).all()

    # ─── 4D) ATR% Leaders ──────────────────────────────
    qa = (
        PlayerStats.query.with_entities(
            PlayerStats.player_name,
            func.sum(PlayerStats.atr_makes).label("atrm"),
            func.sum(PlayerStats.atr_attempts).label("atra"),
            (
                func.sum(PlayerStats.atr_makes)
                / func.nullif(func.sum(PlayerStats.atr_attempts), 0)
                * 100
            ).label("atr_pct"),
        )
        .filter(PlayerStats.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
    )
    if min_atr:
        qa = qa.having(func.sum(PlayerStats.atr_attempts) >= min_atr)
    atr_leaders = qa.order_by(desc("atr_pct")).limit(10).all()

    # 4E) Possessions per BCP

    # a) bcp_sub is already defined above.

    # b) Count each player’s total possessions **across all games** by name
    pps_sub = (
        db.session.query(
            PlayerStats.player_name.label("player_name"),
            func.count(PlayerPossession.id).label("possessions"),
        )
        .join(PlayerStats, PlayerPossession.player_id == PlayerStats.id)
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(Possession.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
        .subquery()
    )

    # c) Join them and compute Poss/BCP
    players_q = (
        db.session.query(
            bcp_sub.c.player_name,
            bcp_sub.c.total_bcp,
            func.coalesce(pps_sub.c.possessions, 0).label("possessions"),
            (
                func.coalesce(pps_sub.c.possessions, 0)
                / func.nullif(bcp_sub.c.total_bcp, 0)
            ).label("poss_per_bcp"),
        )
        # start from the BCP subquery
        .select_from(bcp_sub)
        # then bring in possessions by name
        .outerjoin(pps_sub, pps_sub.c.player_name == bcp_sub.c.player_name).filter(
            # only include players who actually appear in our games
            bcp_sub.c.player_name
            != None
        )
    )

    # d) If sorting by efficiency, only include players with ≥100 possessions
    if sort_by == "efficiency":
        players_q = players_q.filter(pps_sub.c.possessions >= 100).order_by(
            "poss_per_bcp"
        )
    else:
        players_q = players_q.order_by(desc(bcp_sub.c.total_bcp))

    # e) Grab top 10 and shape for template
    top10 = players_q.limit(10).all()
    bcp_leaders = [
        (
            r.player_name,
            float(r.total_bcp),
            int(r.possessions),
            None if r.poss_per_bcp is None else round(r.poss_per_bcp, 2),
        )
        for r in top10
    ]

    # ── Summary cards data ────────────────────────────
    games = Game.query.filter(Game.id.in_(game_ids)).all()
    wins = sum(1 for g in games if g.result.lower() == "win")
    losses = sum(1 for g in games if g.result.lower() == "loss")
    record = f"{wins}–{losses}"

    # 2) Avg. BCP per game over those same games (USE weighted total_blue_collar)
    team_records = TeamStats.query.filter(
        TeamStats.is_opponent == False, TeamStats.game_id.in_(game_ids)
    ).all()
    team_total_bcp = sum(r.total_blue_collar for r in team_records)
    avg_bcp = round(team_total_bcp / len(team_records), 1) if team_records else 0

    # 3) Avg. 3FG% this season (skip any None values)
    pct3s = [row.fg3_pct for row in fg3_leaders]
    valid_pct3 = [p for p in pct3s if isinstance(p, (int, float))]
    if valid_pct3:
        avg_fg3 = f"{sum(valid_pct3) / len(valid_pct3):.1f}%"
    else:
        avg_fg3 = "0%"

    # 4) Avg. Team Points Per Game
    team_stats = TeamStats.query.filter(
        TeamStats.game_id.in_(game_ids), TeamStats.is_opponent == False
    ).all()
    if team_stats:
        total_points = sum(ts.total_points for ts in team_stats)
        avg_ppg = round(total_points / len(team_stats), 1)
    else:
        avg_ppg = 0

    summary = {
        "record": record,
        "avg_bcp": avg_bcp,
        "avg_fg3": avg_fg3,
        "avg_ppg": avg_ppg,
    }

    return render_template(
        "home.html",
        bcp_leaders=bcp_leaders,
        hard_hats=hard_hats,
        fg3_leaders=fg3_leaders,
        atr_leaders=atr_leaders,
        filter_opt=filter_opt,
        view_opt=view_opt,
        active_page="home",
        summary=summary,
    )


@public_bp.route("/practice_home", methods=["GET"])
@login_required
def practice_homepage(active_page="practice_home"):
    """Leaderboard-style homepage for practice statistics."""
    season_id = get_current_season_id()
    if not season_id:
        return render_template(
            "practice_home.html",
            dunks=[],
            bcp_leaders=[],
            atr_leaders=[],
            fg3_leaders=[],
            overall_records=[],
            sprint_wins=[],
            sprint_losses=[],
            active_page=active_page,
        )

    practice_ids = [p.id for p in Practice.query.filter_by(season_id=season_id).all()]
    if not practice_ids:
        return render_template(
            "practice_home.html",
            dunks=[],
            bcp_leaders=[],
            atr_leaders=[],
            fg3_leaders=[],
            overall_records=[],
            sprint_wins=[],
            sprint_losses=[],
            active_page=active_page,
        )

    # ─── Dunks Get You Paid ────────────────────────────────────────────
    dunk_counts = defaultdict(int)
    stats = PlayerStats.query.filter(PlayerStats.practice_id.in_(practice_ids)).all()
    for rec in stats:
        if not rec.shot_type_details:
            continue
        details = (
            json.loads(rec.shot_type_details)
            if isinstance(rec.shot_type_details, str)
            else rec.shot_type_details
        )
        for shot in details:
            if shot.get("result") != "made":
                continue
            if shot.get("atr_type") == "Dunk" or shot.get("2fg_type") == "Dunk":
                dunk_counts[rec.player_name] += 1
    dunks = sorted(dunk_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    # ─── Blue Collar Point Totals and Wins ─────────────────────────────
    bcp_totals = {
        name: float(total)
        for name, total in (
            db.session.query(
                Roster.player_name,
                func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0),
            )
            .join(Roster, BlueCollarStats.player_id == Roster.id)
            .filter(BlueCollarStats.practice_id.in_(practice_ids))
            .group_by(Roster.player_name)
            .all()
        )
    }

    win_counts = defaultdict(int)
    practices = Practice.query.filter(Practice.id.in_(practice_ids)).all()
    for pr in practices:
        rows = (
            db.session.query(Roster.player_name, BlueCollarStats.total_blue_collar)
            .join(Roster, BlueCollarStats.player_id == Roster.id)
            .filter(BlueCollarStats.practice_id == pr.id)
            .all()
        )
        if not rows:
            continue
        max_bcp = max(r.total_blue_collar for r in rows)
        if max_bcp <= 0:
            continue
        for r in rows:
            if r.total_blue_collar == max_bcp:
                win_counts[r.player_name] += 1

    bcp_leaders = [
        (name, bcp_totals.get(name, 0.0), win_counts.get(name, 0))
        for name in bcp_totals.keys()
    ]
    bcp_leaders.sort(key=lambda x: x[1], reverse=True)
    bcp_leaders = bcp_leaders[:10]

    # ─── ATR% Leaders ──────────────────────────────────────────────────
    qa = (
        PlayerStats.query.with_entities(
            PlayerStats.player_name,
            func.sum(PlayerStats.atr_makes).label("atrm"),
            func.sum(PlayerStats.atr_attempts).label("atra"),
            (
                func.sum(PlayerStats.atr_makes)
                / func.nullif(func.sum(PlayerStats.atr_attempts), 0)
                * 100
            ).label("atr_pct"),
        )
        .filter(PlayerStats.practice_id.in_(practice_ids))
        .group_by(PlayerStats.player_name)
        .having(func.sum(PlayerStats.atr_attempts) >= 10)
        .order_by(desc("atr_pct"))
        .limit(5)
    )
    atr_leaders = qa.all()

    # ─── 3FG% Leaders ──────────────────────────────────────────────────
    q3 = (
        PlayerStats.query.with_entities(
            PlayerStats.player_name,
            func.sum(PlayerStats.fg3_makes).label("fg3m"),
            func.sum(PlayerStats.fg3_attempts).label("fg3a"),
            (
                func.sum(PlayerStats.fg3_makes)
                / func.nullif(func.sum(PlayerStats.fg3_attempts), 0)
                * 100
            ).label("fg3_pct"),
        )
        .filter(PlayerStats.practice_id.in_(practice_ids))
        .group_by(PlayerStats.player_name)
        .having(func.sum(PlayerStats.fg3_attempts) >= 10)
        .order_by(desc("fg3_pct"))
        .limit(5)
    )
    fg3_leaders = q3.all()

    # ─── Overall Practice Record ─────────────────────────────────────────
    records_q = (
        db.session.query(
            PlayerStats.player_name,
            func.coalesce(func.sum(PlayerStats.practice_wins), 0).label("wins"),
            func.coalesce(func.sum(PlayerStats.practice_losses), 0).label("losses"),
        )
        .filter(PlayerStats.practice_id.in_(practice_ids))
        .group_by(PlayerStats.player_name)
    )
    records_raw = records_q.all()
    overall_records = []
    for r in records_raw:
        total = (r.wins or 0) + (r.losses or 0)
        pct = (r.wins / total * 100) if total else 0
        overall_records.append(
            (
                r.player_name,
                f"{int(r.wins)}-{int(r.losses)}",
                pct,
            )
        )
    overall_records.sort(key=lambda x: x[2], reverse=True)
    overall_records = [(name, rec, f"{pct:.1f}%") for name, rec, pct in overall_records]

    # ─── Sprint Wins ────────────────────────────────────────────────────
    sprint_wins_q = (
        db.session.query(
            PlayerStats.player_name,
            func.coalesce(func.sum(PlayerStats.sprint_wins), 0).label("wins"),
        )
        .filter(PlayerStats.practice_id.in_(practice_ids))
        .group_by(PlayerStats.player_name)
        .order_by(desc("wins"))
    )
    sprint_wins = [(r.player_name, int(r.wins)) for r in sprint_wins_q.all()]

    # ─── Sprint Losses ──────────────────────────────────────────────────
    sprint_losses_q = (
        db.session.query(
            PlayerStats.player_name,
            func.coalesce(func.sum(PlayerStats.sprint_losses), 0).label("losses"),
        )
        .filter(PlayerStats.practice_id.in_(practice_ids))
        .group_by(PlayerStats.player_name)
        .order_by("losses")
    )
    sprint_losses = [(r.player_name, int(r.losses)) for r in sprint_losses_q.all()]

    return render_template(
        "practice_home.html",
        dunks=dunks,
        bcp_leaders=bcp_leaders,
        atr_leaders=atr_leaders,
        fg3_leaders=fg3_leaders,
        overall_records=overall_records,
        sprint_wins=sprint_wins,
        sprint_losses=sprint_losses,
        active_page=active_page,
    )


@public_bp.route("/home", methods=["GET"])
@login_required
def homepage():
    """Alias for practice_homepage as the main home view."""
    return practice_homepage(active_page="home")
