# basketball_analytics/public/routes.py

from flask import Blueprint, request, render_template, redirect, url_for
from flask_login import login_required
from sqlalchemy import func, desc, and_, case
from models.database import db, BlueCollarStats, PlayerStats, Game, Season, TeamStats, PlayerPossession, Possession


public_bp = Blueprint(
    'public',
    __name__,
    template_folder='templates/public'  # points at public/templates/public/
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
    return [
        g.id for g in
        Game.query
            .order_by(Game.game_date.desc())
            .limit(n)
            .all()
    ]


# ───────────────────────────────────────────────
#  Root → Login redirect
# ───────────────────────────────────────────────

@public_bp.route('/', methods=['GET'])
def root():
    # always send new visitors to the login page
    return redirect(url_for('admin.login'))


# ───────────────────────────────────────────────
#  Homepage with Leaderboards
# ───────────────────────────────────────────────

@public_bp.route('/home', methods=['GET'])
@login_required
def homepage():
    # 1) Read filter options from query string
    filter_opt = request.args.get('filter', 'season')   # 'season', 'last5', 'true_data'
    view_opt   = request.args.get('view',   'season')   # reserved for future use
    # Read sort choice from query string (default to total BCP)
    sort_by = request.args.get('sort', 'bcp')  # 'bcp' or 'efficiency'


    # 2) Pick games to include
    if filter_opt == 'last5':
        game_ids = get_last_n_game_ids(5)
    else:
        # both 'season' and 'true_data' use the full season
        game_ids = get_all_game_ids_for_current_season()

    # 3) Attempt‐thresholds: only apply for season & last5
    min_3fg = None if filter_opt == 'true_data' else 10
    min_atr = None if filter_opt == 'true_data' else 10


    # ─── 4A) Blue Collar Points Leaders ──────────────
    bcp_sub = (
        db.session.query(
            PlayerStats.player_name.label('player_name'),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0)
                .label('total_bcp')
        )
        .join(BlueCollarStats, BlueCollarStats.player_id == PlayerStats.id)
        .filter(BlueCollarStats.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
        .subquery()
    )

    bcp_leaders = (
        db.session.query(
            bcp_sub.c.player_name,
            bcp_sub.c.total_bcp
        )
        .order_by(desc(bcp_sub.c.total_bcp))
        .limit(10)
        .all()
    )

    #  ───────────────────────────────────────────
    #  Determine winning games among our selection
    #  ───────────────────────────────────────────
    winning_game_ids = [
        g.id for g in Game.query
                        .filter(
                            Game.id.in_(game_ids),
                            Game.result.ilike('win')
                        )
                        .all()
    ]

    # ─── 4B) Hard Hat Winners (only in wins) ──────────────────────────
    # 1) Sum each player’s BCP in each winning game
    player_bcp = (
        db.session.query(
            BlueCollarStats.player_id.label('player_id'),
            BlueCollarStats.game_id.label('game_id'),
            (
              func.coalesce(func.sum(BlueCollarStats.def_reb),     0) +
              func.coalesce(func.sum(BlueCollarStats.off_reb),     0) +
              func.coalesce(func.sum(BlueCollarStats.misc),        0) +
              func.coalesce(func.sum(BlueCollarStats.deflection),  0) +
              func.coalesce(func.sum(BlueCollarStats.steal),       0) +
              func.coalesce(func.sum(BlueCollarStats.block),       0) +
              func.coalesce(func.sum(BlueCollarStats.floor_dive),  0) +
              func.coalesce(func.sum(BlueCollarStats.charge_taken),0) +
              func.coalesce(func.sum(BlueCollarStats.reb_tip),     0)
            ).label('bcp')
        )
        .filter(BlueCollarStats.game_id.in_(winning_game_ids))
        .group_by(BlueCollarStats.game_id, BlueCollarStats.player_id)
        .subquery()
    )

    # 2) Find each game’s max BCP among winners
    max_bcp_sub = (
        db.session.query(
            player_bcp.c.game_id,
            func.max(player_bcp.c.bcp).label('max_bcp')
        )
        .group_by(player_bcp.c.game_id)
        .subquery()
    )

    # 3) Count how many times each player hit that max in a winning game
    hard_hats = (
         db.session.query(
             PlayerStats.player_name,
             func.count().label('hard_hat_count')
         )
         .join(player_bcp, player_bcp.c.player_id == PlayerStats.id)
         .join(max_bcp_sub, and_(
             player_bcp.c.game_id == max_bcp_sub.c.game_id,
             player_bcp.c.bcp     == max_bcp_sub.c.max_bcp
         ))
        # only count games where someone actually scored >0 BCP
        .filter(max_bcp_sub.c.max_bcp > 0)
         .group_by(PlayerStats.player_name)
         .order_by(desc('hard_hat_count'))
         .limit(10)
         .all()
     )

    # ─── 4C) 3FG% Leaders ──────────────────────────────
    q3 = (
        PlayerStats.query
        .with_entities(
            PlayerStats.player_name,
            func.sum(PlayerStats.fg3_makes).label('fg3m'),
            func.sum(PlayerStats.fg3_attempts).label('fg3a'),
            (
                func.sum(PlayerStats.fg3_makes)
                / func.nullif(func.sum(PlayerStats.fg3_attempts), 0)
                * 100
            ).label('fg3_pct')
        )
        .filter(PlayerStats.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
    )
    if min_3fg:
        # only players whose **total** 3FG attempts ≥ threshold
        q3 = q3.having(func.sum(PlayerStats.fg3_attempts) >= min_3fg)
    fg3_leaders = q3.order_by(desc('fg3_pct')).limit(10).all()


    # ─── 4D) ATR% Leaders ──────────────────────────────
    qa = (
        PlayerStats.query
        .with_entities(
            PlayerStats.player_name,
            func.sum(PlayerStats.atr_makes).label('atrm'),
            func.sum(PlayerStats.atr_attempts).label('atra'),
            (
                func.sum(PlayerStats.atr_makes)
                / func.nullif(func.sum(PlayerStats.atr_attempts), 0)
                * 100
            ).label('atr_pct')
        )
        .filter(PlayerStats.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
    )
    if min_atr:
        qa = qa.having(func.sum(PlayerStats.atr_attempts) >= min_atr)
    atr_leaders = qa.order_by(desc('atr_pct')).limit(10).all()


    # 4E) Possessions per BCP

    # a) bcp_sub is already defined above.

    # b) Count each player’s total possessions **across all games** by name
    pps_sub = (
        db.session.query(
            PlayerStats.player_name.label('player_name'),
            func.count(PlayerPossession.id).label('possessions')
        )
        .join(PlayerStats, PlayerPossession.player_id == PlayerStats.id)
        .join(Possession,   PlayerPossession.possession_id == Possession.id)
        .filter(Possession.game_id.in_(game_ids))
        .group_by(PlayerStats.player_name)
        .subquery()
    )

    # c) Join them and compute Poss/BCP
    players_q = (
        db.session.query(
            bcp_sub.c.player_name,
            bcp_sub.c.total_bcp,
            func.coalesce(pps_sub.c.possessions, 0).label('possessions'),
            (
                func.coalesce(pps_sub.c.possessions, 0)
                / func.nullif(bcp_sub.c.total_bcp, 0)
            ).label('poss_per_bcp')
        )
        # start from the BCP subquery
        .select_from(bcp_sub)
        # then bring in possessions by name
        .outerjoin(
            pps_sub,
            pps_sub.c.player_name == bcp_sub.c.player_name
        )
        .filter(
            # only include players who actually appear in our games
            bcp_sub.c.player_name != None
        )
    )

    # d) If sorting by efficiency, only include players with ≥100 possessions
    if sort_by == 'efficiency':
        players_q = (
            players_q
            .filter(pps_sub.c.possessions >= 100)
            .order_by('poss_per_bcp')
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
            None if r.poss_per_bcp is None else round(r.poss_per_bcp, 2)
        )
        for r in top10
    ]



       # ── Summary cards data ────────────────────────────
    games  = Game.query.filter(Game.id.in_(game_ids)).all()
    wins   = sum(1 for g in games if g.result.lower() == 'win')
    losses = sum(1 for g in games if g.result.lower() == 'loss')
    record = f"{wins}–{losses}"

    # 2) Avg. BCP per game over those same games (USE weighted total_blue_collar)
    team_records = TeamStats.query\
        .filter(TeamStats.is_opponent==False,
                TeamStats.game_id.in_(game_ids))\
        .all()
    team_total_bcp = sum(r.total_blue_collar for r in team_records)
    avg_bcp        = round(team_total_bcp / len(team_records), 1) if team_records else 0

    # 3) Avg. 3FG% this season (skip any None values)
    pct3s      = [row.fg3_pct for row in fg3_leaders]
    valid_pct3 = [p for p in pct3s if isinstance(p, (int, float))]
    if valid_pct3:
        avg_fg3 = f"{sum(valid_pct3) / len(valid_pct3):.1f}%"
    else:
        avg_fg3 = "0%"

    # 4) Avg. Team Points Per Game
    team_stats = (
        TeamStats.query
         .filter(
             TeamStats.game_id.in_(game_ids),
             TeamStats.is_opponent == False
         )
         .all()
    )
    if team_stats:
        total_points = sum(ts.total_points for ts in team_stats)
        avg_ppg      = round(total_points / len(team_stats), 1)
    else:
        avg_ppg = 0

    summary = {
        'record':  record,
        'avg_bcp': avg_bcp,
        'avg_fg3': avg_fg3,
        'avg_ppg': avg_ppg
    }

    return render_template(
        'home.html',
        bcp_leaders=bcp_leaders,
        hard_hats=hard_hats,
        fg3_leaders=fg3_leaders,
        atr_leaders=atr_leaders,
        filter_opt=filter_opt,
        view_opt=view_opt,
        active_page='home',
        summary=summary
    )
