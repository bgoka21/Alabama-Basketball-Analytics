import os
import pandas as pd
from flask import render_template, jsonify, request, current_app, make_response, abort, redirect, url_for, flash
from werkzeug.utils import secure_filename
from app import app, db, PDFKIT_CONFIG, PDF_OPTIONS
from sqlalchemy import func, or_
from models import Possession, PossessionPlayer, ShotDetail
from models.database import PlayerDraftStock
from admin.routes import (
    collect_practice_labels,
    compute_filtered_totals,
    compute_filtered_blue,
    aggregate_stats,
    compute_team_shot_details,
)
from models.database import PlayerStats, Practice, BlueCollarStats
from datetime import date
from types import SimpleNamespace
from flask_login import login_required
from utils.auth import admin_required
from utils.leaderboard_helpers import (
    get_on_off_summary,
    get_turnover_rates_onfloor,
    get_rebound_rates_onfloor,
)
from utils.label_filters import apply_possession_label_filter
import pdfkit
from public.routes import game_homepage, season_leaderboard
from admin.routes import player_detail
from clients.synergy_client import SynergyDataCoreClient, SynergyAPI
from app.utils.table_cells import num, pct



@app.route('/draft-impact')
def draft_impact_page():
    """Render the page showing draft stock visuals."""
    return render_template('draft_impact.html')


def _get_synergy_client() -> SynergyDataCoreClient:
    """Instantiate a DataCore client using app config."""
    return SynergyDataCoreClient(
        current_app.config['SYNERGY_CLIENT_ID'],
        current_app.config['SYNERGY_CLIENT_SECRET'],
    )


def render_pdf_from_html(html, name):
    pdf = pdfkit.from_string(html, False, options=PDF_OPTIONS, configuration=PDFKIT_CONFIG)
    resp = make_response(pdf)
    resp.headers['Content-Type'] = 'application/pdf'
    resp.headers['Content-Disposition'] = f'attachment; filename="{name}.pdf"'
    return resp


def get_shot_data(shot_type):
    """Aggregate makes and attempts for the given shot type."""
    if shot_type == 'atr':
        makes_col = PlayerStats.atr_makes
        att_col = PlayerStats.atr_attempts
    elif shot_type == '2fg':
        makes_col = PlayerStats.fg2_makes
        att_col = PlayerStats.fg2_attempts
    else:  # 3fg
        makes_col = PlayerStats.fg3_makes
        att_col = PlayerStats.fg3_attempts

    makes, attempts = db.session.query(
        func.coalesce(func.sum(makes_col), 0),
        func.coalesce(func.sum(att_col), 0)
    ).one()
    pct = (makes / attempts * 100) if attempts else 0
    return SimpleNamespace(makes=makes, attempts=attempts, pct=pct)


@app.template_global()
def render_shot_section(shot_type, data):
    """Render a single shot-type section."""
    return render_template('_shot_section.html', shot_type=shot_type, data=data)


@app.route('/pdf/home')
def pdf_home():
    html = game_homepage()
    return render_pdf_from_html(html, 'home')


@app.route('/pdf/leaderboard')
def pdf_leaderboard():
    html = season_leaderboard()
    return render_pdf_from_html(html, 'leaderboard')


@app.route('/pdf/player/<int:player_id>')
def pdf_player(player_id):
    from models.database import Roster
    player = Roster.query.get_or_404(player_id)
    html = player_detail(player.player_name)
    return render_pdf_from_html(html, f'player_{player_id}')


@app.route('/api/competitions', methods=['GET'])
def api_competitions():
    """Return the list of available competitions from Synergy."""
    client = _get_synergy_client()
    competitions = client.get_competitions()
    return jsonify(competitions)


@app.route('/api/stats', methods=['GET'])
def api_stats():
    """Return recent games with player stats for a competition."""
    competition_id = request.args.get('competition_id')
    if not competition_id:
        return jsonify({'error': 'competition_id required'}), 400

    client = _get_synergy_client()
    games = client.get_recent_games_with_stats(competition_id)
    return jsonify(games)


@app.route('/api/player_stats', methods=['GET'])
def api_player_stats():
    """Return cumulative Synergy stats for a given player name."""
    player_name = request.args.get('player_name')
    if not player_name:
        return jsonify({'error': 'player_name required'}), 400

    synergy_api = SynergyAPI()
    player_id = synergy_api.find_player_id(player_name)
    if not player_id:
        return jsonify({'error': 'player not found'}), 404

    stats = synergy_api.get_player_stats(player_id)
    return jsonify(stats)



@app.route('/practice/team_totals')
@login_required
def practice_team_totals():
    """Show aggregated practice totals with date and drill label filters."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    start_dt = end_dt = None
    if start_date:
        try:
            start_dt = date.fromisoformat(start_date)
        except ValueError:
            start_date = ''
    if end_date:
        try:
            end_dt = date.fromisoformat(end_date)
        except ValueError:
            end_date = ''

    # >>> SESSION RANGE INTEGRATION START
    from utils.filters import apply_session_range

    start_dt, end_dt, selected_session = apply_session_range(request.args, start_dt, end_dt)
    # If a session is active, it should override manual dates in the actual query constraints below.
    # >>> SESSION RANGE INTEGRATION END

    q = PlayerStats.query.filter(PlayerStats.practice_id != None)
    if start_dt or end_dt:
        q = q.join(Practice, PlayerStats.practice_id == Practice.id)
        if start_dt:
            q = q.filter(Practice.date >= start_dt)
        if end_dt:
            q = q.filter(Practice.date <= end_dt)

    stats = q.all()

    label_options = collect_practice_labels(stats)
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    if label_set:
        totals = compute_filtered_totals(stats, label_set)
        blue_totals = compute_filtered_blue(stats, label_set)
    else:
        totals = aggregate_stats(stats)
        bc_query = db.session.query(
            func.coalesce(func.sum(BlueCollarStats.def_reb), 0).label('def_reb'),
            func.coalesce(func.sum(BlueCollarStats.off_reb), 0).label('off_reb'),
            func.coalesce(func.sum(BlueCollarStats.misc), 0).label('misc'),
            func.coalesce(func.sum(BlueCollarStats.deflection), 0).label('deflection'),
            func.coalesce(func.sum(BlueCollarStats.steal), 0).label('steal'),
            func.coalesce(func.sum(BlueCollarStats.block), 0).label('block'),
            func.coalesce(func.sum(BlueCollarStats.floor_dive), 0).label('floor_dive'),
            func.coalesce(func.sum(BlueCollarStats.charge_taken), 0).label('charge_taken'),
            func.coalesce(func.sum(BlueCollarStats.reb_tip), 0).label('reb_tip'),
            func.coalesce(func.sum(BlueCollarStats.total_blue_collar), 0).label('total_blue_collar'),
        ).filter(BlueCollarStats.practice_id != None)
        if start_dt or end_dt:
            bc_query = bc_query.join(Practice, BlueCollarStats.practice_id == Practice.id)
            if start_dt:
                bc_query = bc_query.filter(Practice.date >= start_dt)
            if end_dt:
                bc_query = bc_query.filter(Practice.date <= end_dt)
        bc = bc_query.one()
        blue_totals = SimpleNamespace(
            def_reb=bc.def_reb,
            off_reb=bc.off_reb,
            misc=bc.misc,
            deflection=bc.deflection,
            steal=bc.steal,
            block=bc.block,
            floor_dive=bc.floor_dive,
            charge_taken=bc.charge_taken,
            reb_tip=bc.reb_tip,
            total_blue_collar=bc.total_blue_collar,
        )

    pt_query = db.session.query(
        func.coalesce(Possession.paint_touches, '').label('pt'),
        func.coalesce(func.sum(Possession.points_scored), 0).label('points'),
        func.count(Possession.id).label('poss'),
    ).filter(Possession.practice_id != None)
    if start_dt or end_dt:
        pt_query = pt_query.join(Practice, Possession.practice_id == Practice.id)
        if start_dt:
            pt_query = pt_query.filter(Practice.date >= start_dt)
        if end_dt:
            pt_query = pt_query.filter(Practice.date <= end_dt)
    pt_rows = pt_query.group_by(Possession.paint_touches).all()
    buckets = {0: {'pts': 0, 'poss': 0}, 1: {'pts': 0, 'poss': 0}, 2: {'pts': 0, 'poss': 0}, 3: {'pts': 0, 'poss': 0}}
    for r in pt_rows:
        try:
            val = int(float(str(r.pt).strip() or '0'))
        except ValueError:
            continue
        key = 3 if val >= 3 else val
        buckets[key]['pts'] += r.points
        buckets[key]['poss'] += r.poss
    paint_ppp = SimpleNamespace(
        zero=round(buckets[0]['pts'] / buckets[0]['poss'], 2) if buckets[0]['poss'] else 0.0,
        one=round(buckets[1]['pts'] / buckets[1]['poss'], 2) if buckets[1]['poss'] else 0.0,
        two=round(buckets[2]['pts'] / buckets[2]['poss'], 2) if buckets[2]['poss'] else 0.0,
        three=round(buckets[3]['pts'] / buckets[3]['poss'], 2) if buckets[3]['poss'] else 0.0,
    )

    return render_template(
        'admin/team_totals.html',
        totals=totals,
        blue_totals=blue_totals,
        paint_ppp=paint_ppp,
        label_options=label_options,
        selected_labels=selected_labels,
        start_date=start_date or '',
        end_date=end_date or '',
        seasons=[],
        selected_season=None,
        active_page='team_totals',
        # >>> TEMPLATE CONTEXT SESSION START
        selected_session=selected_session if 'selected_session' in locals() else request.args.get('session') or 'All',
        sessions=['Summer 1','Summer 2','Fall','Official Practice','All'],
        # <<< TEMPLATE CONTEXT SESSION END
    )


@app.route('/shot-type/<string:shot_type>')
def shot_type_report(shot_type):
    """Printable report for a single shot type."""
    valid = {'atr': 'ATR', '2fg': '2FG', '3fg': '3FG'}
    if shot_type not in valid:
        abort(404)
    data = get_shot_data(shot_type)
    title = valid[shot_type] + ' Shot Type Report'
    return render_template('shot_type.html', shot_type=shot_type, title=title, data=data)


@app.route('/player/<player_name>')
def player_view(player_name):
    """Public-facing player page with on-court offensive metrics."""
    from models.database import Roster
    player = Roster.query.filter_by(player_name=player_name).first_or_404()

    label_options = collect_practice_labels([])
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    # 1. On-court offensive possessions & points
    offense_sides = ('Offense',)
    helper_labels = list(label_set) if label_set else None
    summary = get_on_off_summary(
        player_id=player.id,
        labels=helper_labels,
    )
    turnover_rates = get_turnover_rates_onfloor(
        player_id=player.id,
        labels=helper_labels,
    )
    rebound_rates = get_rebound_rates_onfloor(
        player_id=player.id,
        labels=helper_labels,
    )

    ON_poss = summary.offensive_possessions_on
    PPP_ON = summary.ppp_on_offense or 0.0
    PPP_OFF = summary.ppp_off_offense or 0.0

    # helper to count shot/event details on-court
    def count_event(ev_type):
        q = (
            db.session.query(func.count(ShotDetail.id))
            .join(Possession, ShotDetail.possession_id == Possession.id)
            .join(PossessionPlayer, Possession.id == PossessionPlayer.possession_id)
            .filter(
                PossessionPlayer.player_id == player.id,
                Possession.possession_side.in_(offense_sides),
                ShotDetail.event_type == ev_type,
            )
        )
        if label_set:
            q = apply_possession_label_filter(q, label_set)
        return q.scalar() or 0

    # 5. Shooting splits
    FGM2_ON = count_event('ATR+') + count_event('2FG+')
    FGM3_ON = count_event('3FG+')
    FGA_ON  = sum(count_event(e) for e in ['ATR+','ATR-','2FG+','2FG-','3FG+','3FG-'])
    EFG_ON  = (FGM2_ON + 1.5 * FGM3_ON) / FGA_ON if FGA_ON else 0
    ATR_pct = count_event('ATR+') / (count_event('ATR+') + count_event('ATR-')) if (count_event('ATR+') + count_event('ATR-')) else 0
    FG2_pct = count_event('2FG+') / (count_event('2FG+') + count_event('2FG-')) if (count_event('2FG+') + count_event('2FG-')) else 0
    FG3_pct = count_event('3FG+') / (count_event('3FG+') + count_event('3FG-')) if (count_event('3FG+') + count_event('3FG-')) else 0

    # 6. Rate metrics
    turnover_pct = turnover_rates.get('team_turnover_rate_on') or 0.0
    turnover_rate     = (turnover_pct / 100) if ON_poss else 0
    off_reb_pct = rebound_rates.get('off_reb_rate_on') or 0.0
    off_reb_rate      = (off_reb_pct / 100) if ON_poss else 0
    fouls_drawn_rate  = count_event('Fouled') / ON_poss if ON_poss else 0

    player_summary_rows = [
        {"stat": "Possessions", "value": num(ON_poss)},
        {"stat": "PPP (On-court)", "value": num(round(PPP_ON, 2))},
        {"stat": "PPP (Off-court)", "value": num(round(PPP_OFF, 2))},
        {"stat": "eFG%", "value": pct(EFG_ON)},
        {"stat": "ATR%", "value": pct(ATR_pct)},
        {"stat": "2FG%", "value": pct(FG2_pct)},
        {"stat": "3FG%", "value": pct(FG3_pct)},
        {"stat": "Turnover Rate", "value": pct(turnover_rate)},
        {"stat": "Off-Reb Rate", "value": pct(off_reb_rate)},
        {"stat": "Fouls Drawn Rate", "value": pct(fouls_drawn_rate)},
    ]

    # 7. Shot type breakdown for mobile tables
    stats_records = PlayerStats.query.filter_by(player_name=player.player_name).all()
    raw_totals, shot_summaries = compute_team_shot_details(stats_records, label_set)
    shot_type_categories = []
    for key, label in [('atr', 'ATR'), ('fg2', '2FG'), ('fg3', '3FG')]:
        summary = shot_summaries.get(key)
        if not summary:
            continue
        tot_att = summary.total.attempts or 0
        def fmt(ctx):
            return SimpleNamespace(
                fga=ctx.attempts,
                fg_pct=f"{(ctx.fg_pct*100 if ctx.fg_pct <= 1 else ctx.fg_pct):.1f}%",
                pps=f"{ctx.pps:.2f}",
                freq_pct=f"{(ctx.attempts / tot_att * 100) if tot_att else 0:.1f}%",
            )
        shot_type_categories.append(
            SimpleNamespace(
                name=label,
                total=fmt(summary.total),
                transition=fmt(summary.transition),
                half_court=fmt(summary.halfcourt),
            )
        )

    # 8. Pass into template context
    return render_template(
        'player_view.html',
        player=player,
        offensive_possessions = ON_poss,
        ppp_on               = round(PPP_ON,2),
        ppp_off              = round(PPP_OFF,2),
        efg_on               = round(EFG_ON*100,1),
        atr_pct              = round(ATR_pct*100,1),
        two_fg_pct           = round(FG2_pct*100,1),
        three_fg_pct         = round(FG3_pct*100,1),
        turnover_rate        = round(turnover_rate*100,1),
        off_reb_rate         = round(off_reb_rate*100,1),
        fouls_drawn_rate     = round(fouls_drawn_rate*100,1),
        shot_type_categories = shot_type_categories,
        shot_type_totals     = raw_totals,
        shot_summaries       = shot_summaries,
        player_summary_rows  = player_summary_rows,
    )




# —– 2. Head-to-Head NET API —–
@app.route('/api/draft/net')
def draft_net():
    rival = request.args.get('school')
    al_net = db.session.query(func.sum(PlayerDraftStock.net)).filter_by(team='Alabama').scalar() or 0
    rival_net = 0
    if rival:
        rival_net = (
            db.session.query(func.sum(PlayerDraftStock.net)).filter_by(team=rival).scalar() or 0
        )
    return jsonify({'alabama_net': int(al_net), 'rival_net': int(rival_net)})


def _dev_tables_enabled() -> bool:
    """Return True when dev-only routes should be registered."""
    return app.debug or os.environ.get('FLASK_ENV') == 'development'


if _dev_tables_enabled():

    @app.route('/dev/tables-smoke')
    def dev_tables_smoke():
        """Render the unified tables smoke test sandbox."""
        return render_template('dev/tables_smoketest.html')


