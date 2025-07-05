from flask import render_template, jsonify, request, current_app
from app import app
from models.recruit import Recruit
from clients.synergy_client import SynergyDataCoreClient


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
