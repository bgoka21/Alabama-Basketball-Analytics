from flask import render_template, jsonify, request, current_app
from app import app
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
