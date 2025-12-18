# basketball_analytics/public/routes.py

from flask import Blueprint, request, render_template, redirect, url_for, flash, jsonify, abort
from flask_login import login_required, current_user
from markupsafe import Markup
from sqlalchemy import func, desc, and_, case, or_
from utils.db_helpers import array_agg_or_group_concat
from utils.skill_config import shot_map, label_map
from datetime import date, timedelta
from collections import defaultdict
import json
from types import SimpleNamespace
from stats_config import LEADERBOARD_STATS
from admin.routes import (
    GAME_TYPE_OPTIONS,
    DEFAULT_GAME_TYPE_SELECTION,
    collect_practice_labels,
    compute_filtered_blue,
    compute_filtered_totals,
    compute_leaderboard,
    _split_leaderboard_rows_for_template,
    get_practice_dual_context,
)
from models.database import (
    db,
    BlueCollarStats,
    PlayerStats,
    Game,
    GameTypeTag,
    Season,
    TeamStats,
    PlayerPossession,
    Possession,
    Practice,
    Roster,
    PnRStats,
    SkillEntry,
    UploadedFile,
    SkillEntry,
)
from services.nba_stats import get_yesterdays_summer_stats, PLAYERS
from app.utils.table_cells import pct, ratio, num, dt_iso


public_bp = Blueprint(
    "public",
    __name__,
    template_folder="templates/public",  # points at public/templates/public/
)

@public_bp.context_processor
def inject_last_stats_update():
    """Provide the file_date of the last parsed file."""
    last_file = (
        UploadedFile.query
        .filter(UploadedFile.last_parsed.isnot(None))
        .order_by(UploadedFile.last_parsed.desc())
        .first()
    )
    if last_file and last_file.file_date:
        def ordinal(n):
            return "%d%s" % (
                n,
                "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"),
            )
        fd = last_file.file_date
        formatted = f"Through {fd.strftime('%B')} {ordinal(fd.day)}"
    else:
        formatted = "Never"
    return {"last_stats_update": formatted}

@public_bp.before_request
def public_bp_before_request():
    # Always allow the root redirect
    if request.endpoint == 'public.root' or request.endpoint.startswith('static'):
        return
    if not current_user.is_authenticated:
        return redirect(url_for('admin.login'))


# ───────────────────────────────────────────────
#  Helper functions
# ───────────────────────────────────────────────


def get_current_season_id():
    """Return the ID of the most recent season (by start_date)."""
    latest = Season.query.order_by(Season.start_date.desc()).first()
    return latest.id if latest else None


def _parse_selected_game_types(args):
    raw_types = args.getlist("game_type")
    if not raw_types:
        single = (args.get("game_type") or "").strip()
        if single:
            raw_types = [single]

    selected_types: list[str] = []
    for value in raw_types:
        match = next(
            (option for option in GAME_TYPE_OPTIONS if option.lower() == value.lower()),
            None,
        )
        if match and match not in selected_types:
            selected_types.append(match)

    if selected_types:
        return selected_types
    return list(DEFAULT_GAME_TYPE_SELECTION)


def _is_win(result: str | None) -> bool:
    sanitized = (result or "").strip().lower()
    return sanitized.startswith("w")


def _is_loss(result: str | None) -> bool:
    sanitized = (result or "").strip().lower()
    return sanitized.startswith("l")


def _compute_wins_and_losses(
    game_ids: list[int],
    games: list[Game],
    filter_opt: str,
    selected_game_types: list[str],
    selected_season_id: int | None,
) -> tuple[list[int], list[int]]:
    if not game_ids:
        return [], []

    team_scores = (
        db.session.query(
            TeamStats.game_id,
            TeamStats.is_opponent,
            TeamStats.total_points,
            TeamStats.wins,
            TeamStats.losses,
        )
        .filter(TeamStats.game_id.in_(game_ids))
        .all()
    )

    scores_by_game: dict[int, dict[str, int | None]] = {}
    win_loss_flags: dict[int, str] = {}
    for game_id, is_opponent, total_points, wins, losses in team_scores:
        scores_by_game.setdefault(game_id, {"us": None, "opp": None})[
            "opp" if is_opponent else "us"
        ] = total_points

        if not is_opponent:
            if wins and wins > 0:
                win_loss_flags[game_id] = "win"
            elif losses and losses > 0:
                win_loss_flags[game_id] = "loss"

    winning_game_ids: list[int] = []
    losing_game_ids: list[int] = []
    for g in games:
        if _is_win(g.result):
            winning_game_ids.append(g.id)
            continue
        if _is_loss(g.result):
            losing_game_ids.append(g.id)
            continue

        flagged_result = win_loss_flags.get(g.id)
        if flagged_result == "win":
            winning_game_ids.append(g.id)
            continue
        if flagged_result == "loss":
            losing_game_ids.append(g.id)
            continue

        score_line = scores_by_game.get(g.id, {})
        us_pts, opp_pts = score_line.get("us"), score_line.get("opp")
        if us_pts is None or opp_pts is None:
            continue
        if us_pts > opp_pts:
            winning_game_ids.append(g.id)
        elif us_pts < opp_pts:
            losing_game_ids.append(g.id)

    if not winning_game_ids and filter_opt == "season":
        fallback_ids = get_all_game_ids_for_season(selected_game_types, selected_season_id)
        if fallback_ids:
            fallback_games = Game.query.filter(Game.id.in_(fallback_ids)).all()
            for g in fallback_games:
                if _is_win(g.result):
                    winning_game_ids.append(g.id)
                elif _is_loss(g.result):
                    losing_game_ids.append(g.id)

    return winning_game_ids, losing_game_ids


def get_all_game_ids_for_season(selected_game_types=None, season_id=None):
    """Return a list of all game IDs in the chosen season (defaults to current)."""
    if season_id is None:
        season_id = get_current_season_id()
    if not season_id:
        return []

    if selected_game_types is None:
        selected_game_types = DEFAULT_GAME_TYPE_SELECTION

    query = Game.query.filter_by(season_id=season_id)
    if selected_game_types:
        query = query.filter(
            Game.type_tags.any(GameTypeTag.tag.in_(selected_game_types))
        )

    return [g.id for g in query.all()]


def get_last_n_game_ids(n, selected_game_types=None, season_id=None):
    """Return the IDs of the last n games by date in the given season."""
    if season_id is None:
        season_id = get_current_season_id()
    if not season_id:
        return []

    if selected_game_types is None:
        selected_game_types = DEFAULT_GAME_TYPE_SELECTION

    query = Game.query.filter_by(season_id=season_id).order_by(Game.game_date.desc())
    if selected_game_types:
        query = query.filter(
            Game.type_tags.any(GameTypeTag.tag.in_(selected_game_types))
        )
    return [g.id for g in query.limit(n).all()]


def _player_cell(name, can_link=True):
    """Return a render_table-compatible cell for a player name."""

    if can_link:
        url = url_for('admin.player_detail', player_name=name)
        return {
            'display': Markup(
                f'<a href="{url}" class="text-blue-600 hover:underline">{name}</a>'
            ),
            'data_value': name,
        }
    return {'display': name, 'data_value': name}


# ───────────────────────────────────────────────
#  Root → Login redirect
# ───────────────────────────────────────────────


@public_bp.route("/", methods=["GET"])
def root():
    """Landing page - login if unauthenticated, cover if logged in."""
    if current_user.is_authenticated:
        return render_template("cover.html")
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
    selected_game_types = _parse_selected_game_types(request.args)

    current_season_id = get_current_season_id()
    seasons = Season.query.order_by(Season.start_date.desc()).all()
    season_ids = {s.id for s in seasons}
    requested_season_id = request.args.get("season_id", type=int)
    selected_season_id = (
        requested_season_id if requested_season_id in season_ids else current_season_id
    )

    # 2) Pick games to include
    if filter_opt == "last5":
        game_ids = get_last_n_game_ids(5, selected_game_types, selected_season_id)
    else:
        # both 'season' and 'true_data' use the full season
        game_ids = get_all_game_ids_for_season(selected_game_types, selected_season_id)

    games = Game.query.filter(Game.id.in_(game_ids)).all()

    # 3) Attempt‐thresholds: only apply for season & last5
    min_3fg = None if filter_opt == "true_data" else 10
    min_atr = None if filter_opt == "true_data" else 10

    # ─── 4A) Blue Collar Points Leaders ──────────────
    bcp_sub = (
        db.session.query(
            Roster.player_name.label("player_name"),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label(
                "total_bcp"
            ),
        )
        .join(BlueCollarStats, BlueCollarStats.player_id == Roster.id)
        .filter(
            BlueCollarStats.game_id.in_(game_ids),
            Roster.season_id == selected_season_id,
            BlueCollarStats.season_id == selected_season_id,
        )
        .group_by(Roster.player_name)
        .subquery()
    )

    bcp_leaders = (
        db.session.query(bcp_sub.c.player_name, bcp_sub.c.total_bcp)
        .order_by(desc(bcp_sub.c.total_bcp))
        .all()
    )

    #  ───────────────────────────────────────────
    #  Determine winning games among our selection
    #  ───────────────────────────────────────────
    winning_game_ids, losing_game_ids = _compute_wins_and_losses(
        game_ids, games, filter_opt, selected_game_types, selected_season_id
    )

    # ─── 4B) Hard Hat Winners (only in wins) ──────────────────────────
    # 1) Sum each player’s BCP in each winning game
    player_bcp = (
        db.session.query(
            BlueCollarStats.player_id.label("player_id"),
            BlueCollarStats.game_id.label("game_id"),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label(
                "bcp"
            ),
        )
        .join(Roster, Roster.id == BlueCollarStats.player_id)
        .filter(
            BlueCollarStats.game_id.in_(winning_game_ids),
            BlueCollarStats.season_id == selected_season_id,
            Roster.season_id == selected_season_id,
        )
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
    hard_hat_winners = (
        db.session.query(
            player_bcp.c.game_id.label("game_id"),
            Roster.player_name.label("player_name"),
        )
        .join(Roster, player_bcp.c.player_id == Roster.id)
        .join(
            max_bcp_sub,
            and_(
                player_bcp.c.game_id == max_bcp_sub.c.game_id,
                player_bcp.c.bcp == max_bcp_sub.c.max_bcp,
            ),
        )
        # only count games where someone actually scored >0 BCP
        .filter(
            player_bcp.c.game_id.in_(winning_game_ids),
            max_bcp_sub.c.max_bcp > 0,
            Roster.season_id == selected_season_id,
        )
        .all()
    )

    hard_hat_counts: dict[str, int] = defaultdict(int)
    for winner in hard_hat_winners:
        hard_hat_counts[winner.player_name] += 1

    hard_hats = sorted(
        hard_hat_counts.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    # ─── 4C) 3FG% Leaders ──────────────────────────────
    q3 = (
        db.session.query(
            Roster.player_name.label("player_name"),
            func.sum(PlayerStats.fg3_makes).label("fg3m"),
            func.sum(PlayerStats.fg3_attempts).label("fg3a"),
            (
                func.sum(PlayerStats.fg3_makes)
                / func.nullif(func.sum(PlayerStats.fg3_attempts), 0)
                * 100
            ).label("fg3_pct"),
        )
        .join(PlayerStats, PlayerStats.player_name == Roster.player_name)
        .filter(
            PlayerStats.game_id.in_(game_ids),
            PlayerStats.season_id == selected_season_id,
            Roster.season_id == selected_season_id,
        )
        .group_by(Roster.player_name)
    )
    if min_3fg:
        # only players whose **total** 3FG attempts ≥ threshold
        q3 = q3.having(func.sum(PlayerStats.fg3_attempts) >= min_3fg)
    fg3_leaders = q3.order_by(desc("fg3_pct")).all()

    # ─── 4D) ATR% Leaders ──────────────────────────────
    qa = (
        db.session.query(
            Roster.player_name.label("player_name"),
            func.sum(PlayerStats.atr_makes).label("atrm"),
            func.sum(PlayerStats.atr_attempts).label("atra"),
            (
                func.sum(PlayerStats.atr_makes)
                / func.nullif(func.sum(PlayerStats.atr_attempts), 0)
                * 100
            ).label("atr_pct"),
        )
        .join(PlayerStats, PlayerStats.player_name == Roster.player_name)
        .filter(
            PlayerStats.game_id.in_(game_ids),
            PlayerStats.season_id == selected_season_id,
            Roster.season_id == selected_season_id,
        )
        .group_by(Roster.player_name)
    )
    if min_atr:
        qa = qa.having(func.sum(PlayerStats.atr_attempts) >= min_atr)
    atr_leaders = qa.order_by(desc("atr_pct")).all()

    # 4E) Possessions per BCP

    # a) bcp_sub is already defined above.

    # b) Count each player’s total possessions **across all games** by name
    pps_sub = (
        db.session.query(
            Roster.player_name.label("player_name"),
            func.count(PlayerPossession.id).label("possessions"),
        )
        .join(Roster, PlayerPossession.player_id == Roster.id)
        .join(Possession, PlayerPossession.possession_id == Possession.id)
        .filter(
            Possession.game_id.in_(game_ids),
            Roster.season_id == selected_season_id,
            Possession.season_id == selected_season_id,
        )
        .group_by(Roster.player_name)
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

    # e) Grab rows for the entire roster
    all_rows = players_q.all()
    bcp_leaders = [
        (
            r.player_name,
            float(r.total_bcp),
            int(r.possessions),
            None if r.poss_per_bcp is None else round(r.poss_per_bcp, 2),
        )
        for r in all_rows
    ]

    # ── Summary cards data ────────────────────────────
    wins = len(winning_game_ids)
    losses = len(losing_game_ids)
    record = f"{wins}–{losses}"

    # 2) Avg. BCP per game over those same games (USE weighted total_blue_collar)
    team_stats = TeamStats.query.filter(
        TeamStats.is_opponent == False, TeamStats.game_id.in_(game_ids)
    ).all()
    team_total_bcp = sum(r.total_blue_collar or 0 for r in team_stats)
    avg_bcp = round(team_total_bcp / len(team_stats), 1) if team_stats else 0

    team_fg3_makes = sum(ts.total_fg3_makes or 0 for ts in team_stats)
    team_fg3_attempts = sum(ts.total_fg3_attempts or 0 for ts in team_stats)
    team_fg3_pct = (
        team_fg3_makes / team_fg3_attempts if team_fg3_attempts else None
    )

    # 3) Season-long team 3FG% using total makes/attempts
    if team_fg3_pct is not None:
        avg_fg3 = f"{team_fg3_pct * 100:.1f}%"
    else:
        avg_fg3 = "0%"

    # 4) Avg. Team Points Per Game
    if team_stats:
        total_points = sum(ts.total_points or 0 for ts in team_stats)
        avg_ppg = round(total_points / len(team_stats), 1)
    else:
        avg_ppg = 0
    fg3_totals = {
        "player": "Team Totals",
        "fg": f"{int(team_fg3_makes)}/{int(team_fg3_attempts)}",
        "fg_pct": f"{team_fg3_pct * 100:.1f}%" if team_fg3_pct is not None else "0.0%",
    }

    summary = {
        "record": record,
        "avg_bcp": avg_bcp,
        "avg_fg3": avg_fg3,
        "avg_ppg": avg_ppg,
    }

    can_link = current_user.is_authenticated and (
        current_user.is_admin or not current_user.is_player
    )

    bcp_rows = []
    for player, total_bcp, possessions, poss_per_bcp in bcp_leaders:
        if poss_per_bcp is None:
            poss_display = "—"
            poss_value = ""
        else:
            poss_display = f"{poss_per_bcp:.2f}"
            poss_value = f"{float(poss_per_bcp):.6f}"
        bcp_rows.append(
            {
                "player": _player_cell(player, can_link),
                "player_sort": player,
                "total_bcp": num(round(total_bcp, 1)),
                "possessions": num(possessions),
                "poss_per_bcp": {"display": poss_display, "data_value": poss_value},
            }
        )

    hard_hat_rows = [
        {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "hard_hats": num(count),
        }
        for player, count in hard_hats
    ]

    fg3_rows = []
    for r in fg3_leaders:
        makes = int(r.fg3m or 0)
        attempts = int(r.fg3a or 0)
        fg3_rows.append(
            {
                "player": _player_cell(r.player_name, can_link),
                "player_sort": r.player_name,
                "fg": ratio(makes, attempts, show_pct=False),
                "fg_pct": pct((r.fg3_pct / 100) if r.fg3_pct is not None else None),
            }
        )

    atr_rows = []
    for r in atr_leaders:
        makes = int(r.atrm or 0)
        attempts = int(r.atra or 0)
        atr_rows.append(
            {
                "player": _player_cell(r.player_name, can_link),
                "player_sort": r.player_name,
                "fg": ratio(makes, attempts, show_pct=False),
                "fg_pct": pct((r.atr_pct / 100) if r.atr_pct is not None else None),
            }
        )

    return render_template(
        "home.html",
        bcp_rows=bcp_rows,
        hard_hat_rows=hard_hat_rows,
        fg3_rows=fg3_rows,
        fg3_totals=fg3_totals,
        atr_rows=atr_rows,
        filter_opt=filter_opt,
        view_opt=view_opt,
        sort_by=sort_by,
        game_type_options=GAME_TYPE_OPTIONS,
        selected_game_types=selected_game_types,
        active_page="home",
        summary=summary,
        seasons=seasons,
        selected_season_id=selected_season_id,
        current_season_id=current_season_id,
    )


@public_bp.route("/hard_hats", methods=["GET"])
@login_required
def hard_hat_detail():
    filter_opt = request.args.get("filter", "season")
    selected_game_types = _parse_selected_game_types(request.args)

    current_season_id = get_current_season_id()
    seasons = Season.query.order_by(Season.start_date.desc()).all()
    season_ids = {s.id for s in seasons}
    requested_season_id = request.args.get("season_id", type=int)
    selected_season_id = (
        requested_season_id if requested_season_id in season_ids else current_season_id
    )

    if not selected_season_id:
        return render_template(
            "hard_hats.html",
            hard_hat_rows=[],
            filter_opt=filter_opt,
            game_type_options=GAME_TYPE_OPTIONS,
            selected_game_types=selected_game_types,
            seasons=seasons,
            selected_season_id=selected_season_id,
            current_season_id=current_season_id,
            active_page="home",
        )

    if filter_opt == "last5":
        game_ids = get_last_n_game_ids(5, selected_game_types, selected_season_id)
    else:
        game_ids = get_all_game_ids_for_season(selected_game_types, selected_season_id)

    games = (
        Game.query.filter(Game.id.in_(game_ids))
        .order_by(Game.game_date.desc())
        .all()
        if game_ids
        else []
    )

    winning_game_ids, _ = _compute_wins_and_losses(
        game_ids, games, filter_opt, selected_game_types, selected_season_id
    )

    hard_hat_rows: list[dict[str, object]] = []
    if winning_game_ids:
        player_bcp = (
            db.session.query(
                BlueCollarStats.player_id.label("player_id"),
                BlueCollarStats.game_id.label("game_id"),
                func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label(
                    "bcp"
                ),
            )
            .join(Roster, Roster.id == BlueCollarStats.player_id)
            .filter(
                BlueCollarStats.game_id.in_(winning_game_ids),
                BlueCollarStats.season_id == selected_season_id,
                Roster.season_id == selected_season_id,
            )
            .group_by(BlueCollarStats.game_id, BlueCollarStats.player_id)
            .subquery()
        )

        max_bcp_sub = (
            db.session.query(
                player_bcp.c.game_id, func.max(player_bcp.c.bcp).label("max_bcp")
            )
            .group_by(player_bcp.c.game_id)
            .subquery()
        )

        winners_query = (
            db.session.query(
                player_bcp.c.game_id.label("game_id"),
                Roster.player_name.label("player_name"),
                player_bcp.c.bcp.label("bcp"),
            )
            .join(Roster, player_bcp.c.player_id == Roster.id)
            .join(
                max_bcp_sub,
                and_(
                    player_bcp.c.game_id == max_bcp_sub.c.game_id,
                    player_bcp.c.bcp == max_bcp_sub.c.max_bcp,
                ),
            )
            .filter(
                player_bcp.c.game_id.in_(winning_game_ids),
                max_bcp_sub.c.max_bcp > 0,
                Roster.season_id == selected_season_id,
            )
            .all()
        )

        winners_by_game: dict[int, list[str]] = defaultdict(list)
        bcp_by_game: dict[int, float] = {}
        for row in winners_query:
            winners_by_game[row.game_id].append(row.player_name)
            bcp_by_game[row.game_id] = float(row.bcp or 0)

        winning_games = Game.query.filter(Game.id.in_(winning_game_ids)).all()
        game_lookup = {g.id: g for g in winning_games}

        def _sort_key(gid: int):
            game = game_lookup.get(gid)
            return (
                game.game_date if game and game.game_date else date.min,
                gid,
            )

        for gid in sorted(winners_by_game.keys(), key=_sort_key, reverse=True):
            game = game_lookup.get(gid)
            date_display = (
                game.game_date.strftime("%b %d, %Y")
                if game and game.game_date
                else "Date TBD"
            )
            date_sort = (
                game.game_date.strftime("%Y%m%d") if game and game.game_date else "0"
            )
            opponent = game.opponent_name if game and game.opponent_name else "Unknown"

            game_url = url_for("admin.game_stats", game_id=gid) if game else None
            game_label = f"Game {gid}"
            game_display = (
                Markup(
                    f'<a href="{game_url}" class="text-blue-600 hover:underline">{game_label}</a>'
                )
                if game_url
                else game_label
            )

            winner_links = [_player_cell(name)["display"] for name in winners_by_game[gid]]
            winners_display = (
                Markup(", ".join(str(link) for link in winner_links))
                if winner_links
                else "—"
            )

            hard_hat_rows.append(
                {
                    "game": {"display": game_display, "data_value": gid},
                    "date": {"display": date_display, "data_value": date_sort},
                    "date_sort": date_sort,
                    "opponent": opponent,
                    "winner": {"display": winners_display, "data_value": ", ".join(winners_by_game[gid])},
                    "winner_sort": ", ".join(winners_by_game[gid]),
                    "bcp": num(bcp_by_game.get(gid, 0)),
                }
            )

    return render_template(
        "hard_hats.html",
        hard_hat_rows=hard_hat_rows,
        filter_opt=filter_opt,
        game_type_options=GAME_TYPE_OPTIONS,
        selected_game_types=selected_game_types,
        seasons=seasons,
        selected_season_id=selected_season_id,
        current_season_id=current_season_id,
        active_page="home",
    )


@public_bp.route("/practice_home", methods=["GET"])
@login_required
def practice_homepage(active_page="practice_home"):
    """Leaderboard-style homepage for practice statistics."""
    season_id = get_current_season_id()
    if not season_id:
        empty_totals = {"player": "Team Totals", "fg": "0/0", "pct": "0.0%"}
        return render_template(
            "practice_home.html",
            dunks=[],
            bcp_leaders=[],
            atr_leaders=[],
            fg3_leaders=[],
            pps_leaders=[],
            overall_records=[],
            sprint_wins=[],
            sprint_losses=[],
            fg3_totals=empty_totals,
            active_page=active_page,
            label_options=collect_practice_labels([]),
            selected_labels=[],
            start_date=request.args.get("start_date", ""),
            end_date=request.args.get("end_date", ""),
            show_poss_per_bcp=False,
        )

    start_date_param = request.args.get("start_date")
    end_date_param   = request.args.get("end_date")
    practice_q = Practice.query.filter_by(season_id=season_id)
    if start_date_param:
        try:
            start_dt = date.fromisoformat(start_date_param)
            practice_q = practice_q.filter(Practice.date >= start_dt)
        except ValueError:
            pass
    if end_date_param:
        try:
            end_dt = date.fromisoformat(end_date_param)
            practice_q = practice_q.filter(Practice.date <= end_dt)
        except ValueError:
            pass
    practice_ids = [p.id for p in practice_q.all()]
    if not practice_ids:
        empty_totals = {"player": "Team Totals", "fg": "0/0", "pct": "0.0%"}
        return render_template(
            "practice_home.html",
            dunks=[],
            bcp_leaders=[],
            atr_leaders=[],
            fg3_leaders=[],
            pps_leaders=[],
            overall_records=[],
            sprint_wins=[],
            sprint_losses=[],
            fg3_totals=empty_totals,
            active_page=active_page,
            label_options=collect_practice_labels([]),
            selected_labels=[],
            start_date=start_date_param or '',
            end_date=end_date_param or '',
            show_poss_per_bcp=False,
        )

    stats = PlayerStats.query.filter(PlayerStats.practice_id.in_(practice_ids)).all()

    label_options = collect_practice_labels(stats)
    selected_labels = [
        lbl for lbl in request.args.getlist("label") if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}
    target_drill_labels = {"4V4 DRILLS", "5V5 DRILLS"}
    show_poss_per_bcp = False
    possessions_by_player = defaultdict(int)
    if label_set and (label_set & target_drill_labels):
        show_poss_per_bcp = True
        possession_query = (
            db.session.query(
                Roster.player_name,
                func.count(PlayerPossession.id),
            )
            .join(Roster, PlayerPossession.player_id == Roster.id)
            .join(Possession, Possession.id == PlayerPossession.possession_id)
            .filter(Roster.season_id == season_id)
            .filter(Possession.practice_id.in_(practice_ids))
        )
        clauses = [Possession.drill_labels.ilike(f"%{lbl}%") for lbl in label_set]
        if clauses:
            possession_query = possession_query.filter(or_(*clauses))
        for name, count in possession_query.group_by(Roster.player_name).all():
            possessions_by_player[name] = count
    fg3_total_makes = 0
    fg3_total_attempts = 0

    # ─── Dunks Get You Paid ────────────────────────────────────────────
    dunk_counts = defaultdict(int)
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
            labels = {
                lbl.strip().upper()
                for lbl in shot.get("drill_labels", [])
                if isinstance(lbl, str) and lbl.strip()
            }
            if label_set and not (labels & label_set):
                continue
            if shot.get("atr_type") == "Dunk" or shot.get("2fg_type") == "Dunk":
                dunk_counts[rec.player_name] += 1
    dunks = sorted(dunk_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    if label_set:
        stats_by_player = defaultdict(list)
        stats_by_practice = defaultdict(lambda: defaultdict(list))
        for rec in stats:
            stats_by_player[rec.player_name].append(rec)
            stats_by_practice[rec.practice_id][rec.player_name].append(rec)

        bcp_totals = defaultdict(float)
        win_counts = defaultdict(int)
        for pr_id, players in stats_by_practice.items():
            max_bcp = 0
            winners = []
            for player, rows in players.items():
                blue = compute_filtered_blue(rows, label_set)
                total = blue.total_blue_collar
                bcp_totals[player] += total
                if total > max_bcp:
                    max_bcp = total
                    winners = [player]
                elif total == max_bcp:
                    winners.append(player)
            if max_bcp > 0:
                for p in winners:
                    win_counts[p] += 1

        bcp_entries = []
        for player in bcp_totals:
            total = bcp_totals[player]
            wins = win_counts.get(player, 0)
            possessions = possessions_by_player[player]
            ratio_value = (possessions / total) if (show_poss_per_bcp and total) else None
            bcp_entries.append((player, total, wins, possessions, ratio_value))
        bcp_entries.sort(key=lambda x: x[1], reverse=True)
        bcp_leaders = bcp_entries[:10]

        atr_rows = []
        fg3_rows = []
        records = []
        sprint_wins = []
        sprint_losses = []
        pps_rows = []
        for player, rows in stats_by_player.items():
            totals = compute_filtered_totals(rows, label_set)
            fg3_total_makes += totals.fg3_makes or 0
            fg3_total_attempts += totals.fg3_attempts or 0
            if totals.atr_attempts >= 10:
                atr_pct = (
                    round(totals.atr_makes / totals.atr_attempts * 100, 1)
                    if totals.atr_attempts
                    else None
                )
                atr_rows.append(SimpleNamespace(player_name=player, atrm=totals.atr_makes, atra=totals.atr_attempts, atr_pct=atr_pct))
            if totals.fg3_attempts >= 10:
                fg3_pct = (
                    round(totals.fg3_makes / totals.fg3_attempts * 100, 1)
                    if totals.fg3_attempts
                    else None
                )
                fg3_rows.append(SimpleNamespace(player_name=player, fg3m=totals.fg3_makes, fg3a=totals.fg3_attempts, fg3_pct=fg3_pct))

            wins = losses = sw = sl = 0
            for rec in rows:
                if not rec.stat_details:
                    continue
                details = json.loads(rec.stat_details) if isinstance(rec.stat_details, str) else rec.stat_details
                for ev in details:
                    lbls = {lbl.strip().upper() for lbl in ev.get('drill_labels', []) if isinstance(lbl, str) and lbl.strip()}
                    if label_set and not (lbls & label_set):
                        continue
                    if ev.get('event') == 'win':
                        wins += 1
                    elif ev.get('event') == 'loss':
                        losses += 1
                    elif ev.get('event') == 'sprint_wins':
                        sw += 1
                    elif ev.get('event') == 'sprint_losses':
                        sl += 1

            win_pct_val = (wins / (wins + losses) * 100) if (wins + losses) else 0
            records.append((player, f"{int(wins)}-{int(losses)}", win_pct_val))
            sprint_wins.append((player, sw))
            sprint_losses.append((player, sl))
            pps_rows.append((player, totals.points_per_shot))

        atr_leaders = sorted(
            atr_rows,
            key=lambda r: r.atr_pct if r.atr_pct is not None else -1,
            reverse=True,
        )
        fg3_leaders = sorted(
            fg3_rows,
            key=lambda r: r.fg3_pct if r.fg3_pct is not None else -1,
            reverse=True,
        )
        overall_records = sorted(records, key=lambda x: x[2], reverse=True)
        sprint_wins.sort(key=lambda x: x[1], reverse=True)
        sprint_losses.sort(key=lambda x: x[1])
        pps_leaders = sorted(pps_rows, key=lambda x: x[1], reverse=True)
    else:
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

        bcp_entries = []
        for name in bcp_totals.keys():
            total = bcp_totals.get(name, 0.0)
            wins = win_counts.get(name, 0)
            possessions = possessions_by_player[name]
            ratio_value = (possessions / total) if (show_poss_per_bcp and total) else None
            bcp_entries.append((name, total, wins, possessions, ratio_value))
        bcp_entries.sort(key=lambda x: x[1], reverse=True)
        bcp_leaders = bcp_entries[:10]

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
        )
        fg3_leaders = q3.all()

        totals_query = db.session.query(
            func.coalesce(func.sum(PlayerStats.fg3_makes), 0),
            func.coalesce(func.sum(PlayerStats.fg3_attempts), 0),
        ).filter(PlayerStats.practice_id.in_(practice_ids))
        total_makes, total_attempts = totals_query.one()
        fg3_total_makes = total_makes or 0
        fg3_total_attempts = total_attempts or 0

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
            win_pct_val = (r.wins / total * 100) if total else 0
            overall_records.append((r.player_name, f"{int(r.wins)}-{int(r.losses)}", win_pct_val))
        overall_records.sort(key=lambda x: x[2], reverse=True)

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

        # ─── PPS Leaders (entire roster) ─────────────────────────────────────
        pps_rows = (
            db.session.query(
                PlayerStats.player_name.label("player_name"),
                func.coalesce(func.sum(PlayerStats.atr_makes), 0).label("atrm"),
                func.coalesce(func.sum(PlayerStats.fg2_makes), 0).label("fg2m"),
                func.coalesce(func.sum(PlayerStats.fg3_makes), 0).label("fg3m"),
                func.coalesce(func.sum(PlayerStats.atr_attempts), 0).label("atra"),
                func.coalesce(func.sum(PlayerStats.fg2_attempts), 0).label("fg2a"),
                func.coalesce(func.sum(PlayerStats.fg3_attempts), 0).label("fg3a"),
            )
            .filter(PlayerStats.practice_id.in_(practice_ids))
            .group_by(PlayerStats.player_name)
            .all()
        )

        stats_map = {r.player_name: r for r in pps_rows}

        roster_names = [r.player_name for r in Roster.query.filter_by(season_id=season_id).all()]

        pps_leaders = []
        for name in roster_names:
            row = stats_map.get(name)
            if row:
                attempts = (row.atra or 0) + (row.fg2a or 0) + (row.fg3a or 0)
                makes = (row.atrm or 0) + (row.fg2m or 0) + 1.5 * (row.fg3m or 0)
                pps = round((makes * 2 / attempts), 2) if attempts else 0.0
            else:
                pps = 0.0
            pps_leaders.append((name, pps))

        pps_leaders.sort(key=lambda x: x[1], reverse=True)

    fg3_total_pct = (
        fg3_total_makes / fg3_total_attempts if fg3_total_attempts else None
    )
    fg3_totals = {
        "player": "Team Totals",
        "fg": f"{int(fg3_total_makes)}/{int(fg3_total_attempts)}",
        "pct": f"{fg3_total_pct * 100:.1f}%" if fg3_total_pct is not None else "0.0%",
    }

    can_link = current_user.is_authenticated and (
        current_user.is_admin or not current_user.is_player
    )

    def decimal_cell(value, places=1):
        if value is None:
            return {"display": "-", "data_value": ""}
        display = f"{value:.{places}f}"
        return {"display": display, "data_value": f"{float(value):.6f}"}

    dunk_rows = [
        {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "dunks": num(count),
        }
        for player, count in dunks
    ]

    bcp_rows = []
    for player, total, wins, _possessions, poss_per_bcp in bcp_leaders:
        row = {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "bcp": decimal_cell(total, places=1),
            "wins": num(wins),
        }
        if show_poss_per_bcp:
            row["poss_per_bcp"] = decimal_cell(poss_per_bcp, places=2)
        bcp_rows.append(row)

    atr_rows = [
        {
            "player": _player_cell(p.player_name, can_link),
            "player_sort": p.player_name,
            "fg": ratio(p.atrm or 0, p.atra or 0, show_pct=False),
            "pct": pct((p.atr_pct / 100) if p.atr_pct is not None else None),
        }
        for p in atr_leaders
    ]

    fg3_rows = [
        {
            "player": _player_cell(p.player_name, can_link),
            "player_sort": p.player_name,
            "fg": ratio(p.fg3m or 0, p.fg3a or 0, show_pct=False),
            "pct": pct((p.fg3_pct / 100) if p.fg3_pct is not None else None),
        }
        for p in fg3_leaders
    ]

    pps_rows = [
        {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "pps": decimal_cell(pps, places=2),
        }
        for player, pps in pps_leaders
    ]

    overall_record_rows = [
        {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "record": {"display": record, "data_value": record},
            "win_pct": pct((pct_val / 100) if pct_val is not None else None),
        }
        for player, record, pct_val in overall_records
    ]

    sprint_win_rows = [
        {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "wins": num(wins),
        }
        for player, wins in sprint_wins
    ]

    sprint_loss_rows = [
        {
            "player": _player_cell(player, can_link),
            "player_sort": player,
            "losses": num(losses),
        }
        for player, losses in sprint_losses
    ]

    return render_template(
        "practice_home.html",
        dunk_rows=dunk_rows,
        bcp_rows=bcp_rows,
        atr_rows=atr_rows,
        fg3_rows=fg3_rows,
        fg3_totals=fg3_totals,
        pps_rows=pps_rows,
        overall_record_rows=overall_record_rows,
        sprint_win_rows=sprint_win_rows,
        sprint_loss_rows=sprint_loss_rows,
        active_page=active_page,
        label_options=label_options,
        selected_labels=selected_labels,
        start_date=start_date_param or '',
        end_date=end_date_param or '',
        show_poss_per_bcp=show_poss_per_bcp,
    )


@public_bp.route("/home", methods=["GET"])
@login_required
def homepage():
    """Redirect the legacy home route to the game homepage."""
    return redirect(url_for("public.game_homepage"))


@public_bp.route('/api/direct_pnr_for_player/<int:player_id>', methods=['GET'])
@login_required
def direct_pnr_for_player(player_id):
    """Return aggregated Direct PnR statistics for a given player."""
    pnrs = PnRStats.query.filter_by(player_id=player_id).all()

    total = len(pnrs)
    pnrs_as_bh = sum(1 for p in pnrs if p.role == 'BH')
    pnrs_as_screener = sum(1 for p in pnrs if p.role == 'Screener')
    adv_plus = sum(1 for p in pnrs if p.advantage_created == 'Adv+')

    direct = [p for p in pnrs if p.direct]
    direct_count = len(direct)
    direct_points = sum(p.points_scored or 0 for p in direct)
    direct_turnovers = sum(1 for p in direct if p.turnover_occurred)
    direct_assists = sum(1 for p in direct if p.assist_occurred)

    pct_adv_plus = adv_plus / total if total else 0
    direct_points_per = direct_points / direct_count if direct_count else 0

    return jsonify({
        'player_id': player_id,
        'total_pnrs': total,
        'pnrs_as_bh': pnrs_as_bh,
        'pnrs_as_screener': pnrs_as_screener,
        'pct_adv_plus': round(pct_adv_plus, 3) if total else 0,
        'direct_pnr_points_per': round(direct_points_per, 3) if direct_count else 0,
        'direct_pnr_turnovers': direct_turnovers,
        'direct_pnr_assists': direct_assists,
    })


@public_bp.route('/leaderboard')
@login_required
def season_leaderboard():
    stat_key = request.args.get('stat') or request.args.get('base_stat')
    if not stat_key:
        stat_key = LEADERBOARD_STATS[0]['key']
    sid = get_current_season_id()
    q = PlayerStats.query.filter(PlayerStats.season_id == sid)
    stats_list = q.all()
    label_options = collect_practice_labels(stats_list)
    selected_labels = [lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options]
    label_set = {lbl.upper() for lbl in selected_labels}

    cfg, rows, team_totals = compute_leaderboard(stat_key, sid, label_set=label_set if label_set else None)
    practice_dual_ctx = (
        get_practice_dual_context(cfg['key'], sid, label_set=label_set if label_set else None)
        if cfg
        else None
    )
    season_rows_for_split = (
        practice_dual_ctx.get('season_rows') if practice_dual_ctx else rows
    )
    season_totals_for_split = (
        practice_dual_ctx.get('season_team_totals') if practice_dual_ctx else team_totals
    )
    split_context = (
        _split_leaderboard_rows_for_template(
            cfg['key'],
            season_rows_for_split,
            season_totals_for_split,
            last_rows=practice_dual_ctx.get('last_rows') if practice_dual_ctx else None,
            last_team_totals=practice_dual_ctx.get('last_team_totals') if practice_dual_ctx else None,
            last_practice_date=practice_dual_ctx.get('last_practice_date') if practice_dual_ctx else None,
        )
        if cfg
        else {}
    )

    return render_template(
        'leaderboard.html',
        stats_config=LEADERBOARD_STATS,
        selected=cfg,
        rows=rows,
        team_totals=team_totals,
        label_options=label_options,
        selected_labels=selected_labels,
        **split_context,
    )

@public_bp.route('/skill_dev')
@login_required
def skill_dev():
    if not current_user.is_player:
        flash('You do not have permission to view that page.', 'error')
        return redirect(url_for('public.practice_homepage'))
    roster = Roster.query.filter_by(player_name=current_user.player_name).first_or_404()
    entries = (
        SkillEntry.query
        .filter_by(player_id=roster.id, skill_name='NBA 100')
        .order_by(SkillEntry.date.desc())
        .all()
    )

    nba100_rows = []
    for entry in entries:
        iso_date = entry.date.isoformat() if entry.date else None
        pretty_date = entry.date.strftime('%b %d, %Y') if entry.date else ''
        nba100_rows.append(
            {
                'date': dt_iso(iso_date, pretty_date),
                'makes': num(entry.value),
            }
        )

    return render_template('skill_dev.html', nba100_rows=nba100_rows)


@public_bp.route('/nba100', methods=['POST'])
@login_required
def add_nba100_entry():
    if not current_user.is_player:
        abort(403)
    roster = Roster.query.filter_by(player_name=current_user.player_name).first_or_404()
    form_date = request.form.get('date')
    makes_str = request.form.get('makes', '0')
    try:
        target_date = date.fromisoformat(form_date)
    except (TypeError, ValueError):
        flash('Invalid date for NBA 100 entry.', 'error')
        return redirect(url_for('public.skill_dev'))
    try:
        makes = int(makes_str)
        if makes < 0 or makes > 100:
            raise ValueError()
    except ValueError:
        flash('"Makes" must be an integer between 0 and 100.', 'error')
        return redirect(url_for('public.skill_dev'))
    new_entry = SkillEntry(
        player_id=roster.id,
        date=target_date,
        skill_name='NBA 100',
        value=makes,
        shot_class=None,
        subcategory=None,
        makes=0,
        attempts=0,
    )
    db.session.add(new_entry)
    db.session.commit()
    flash(f'NBA 100 entry saved: {makes}/100 on {target_date.isoformat()}.', 'success')
    return redirect(url_for('public.skill_dev'))


@public_bp.route('/summer_stats')
@login_required
def summer_stats():
    stats = get_yesterdays_summer_stats(PLAYERS)
    date_str = (date.today() - timedelta(days=1)).strftime("%B %d, %Y")
    return render_template('summer_stats.html', stats=stats, date_str=date_str)
