from flask import render_template, jsonify, request, current_app, make_response, abort
from app import app, PDFKIT_CONFIG, PDF_OPTIONS
from yourapp import db
from admin.routes import (
    collect_practice_labels,
    compute_filtered_totals,
    compute_filtered_blue,
    aggregate_stats,
)
from models.database import PlayerStats, Practice, BlueCollarStats
from datetime import date
from sqlalchemy import func
from types import SimpleNamespace
from flask_login import login_required
import pdfkit
from public.routes import game_homepage, season_leaderboard
from admin.routes import player_detail
from models.recruit import Recruit
from clients.synergy_client import SynergyDataCoreClient, SynergyAPI


@app.route('/recruits')
def recruits_view():
    recs = Recruit.query.order_by(Recruit.last_updated.desc()).all()
    return render_template('recruits.html', recruits=recs)


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


@app.route('/api/players', methods=['GET'])
def api_players():
    q = request.args.get('query', '').strip()
    if not q:
        return jsonify([])
    results = Recruit.query.filter(Recruit.name.ilike(f'%{q}%')).limit(10).all()
    return jsonify([{'id': r.id, 'name': r.name} for r in results])


# ------------------------------------------------------------------
#  Simple page to query Synergy player stats
# ------------------------------------------------------------------

@app.route('/recruits/synergy_player')
def synergy_player_page():
    """Render the player stats search page."""
    return render_template('synergy_player.html')


@app.route('/recruits/synergy_stats')
def synergy_stats_page():
    """Render the competition stats page."""
    return render_template('synergy_stats.html')


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

    return render_template(
        'admin/team_totals.html',
        totals=totals,
        blue_totals=blue_totals,
        label_options=label_options,
        selected_labels=selected_labels,
        start_date=start_date or '',
        end_date=end_date or '',
        seasons=[],
        selected_season=None,
        active_page='team_totals',
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
