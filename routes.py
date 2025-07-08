from flask import render_template, jsonify, request, current_app, make_response
from app import app, PDFKIT_CONFIG, PDF_OPTIONS
from yourapp import db
from yourapp.stats_utils import get_practice_team_totals
from admin.routes import collect_practice_labels, compute_filtered_totals
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
    """Show aggregated practice totals with optional drill-label filtering."""
    stats = get_practice_team_totals(db.session, raw=True)
    label_options = collect_practice_labels(stats)
    selected_labels = [
        lbl for lbl in request.args.getlist('label') if lbl.upper() in label_options
    ]
    label_set = {lbl.upper() for lbl in selected_labels}

    totals = compute_filtered_totals(stats, label_set) if label_set else compute_filtered_totals(stats, set())
    return render_template(
        'practice_team_totals.html',
        team_totals=vars(totals),
        label_options=label_options,
        selected_labels=selected_labels,
    )
