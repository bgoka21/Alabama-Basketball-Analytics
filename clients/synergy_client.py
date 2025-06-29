import requests
from flask import current_app

class SynergyAPI:
    def __init__(self):
        self.api_key = current_app.config['SYNERGY_API_KEY']
        self.base = 'https://api.synergybasketball.com/v1'

    def find_player_id(self, name: str) -> str:
        resp = requests.get(
            f"{self.base}/players",
            params={'search': name},
            headers={'access-token': self.api_key}
        )
        resp.raise_for_status()
        players = resp.json().get('players', [])
        return players[0]['id'] if players else None

    def get_player_stats(self, player_id: str) -> dict:
        resp = requests.get(
            f"{self.base}/players/{player_id}/stats",
            headers={'access-token': self.api_key}
        )
        resp.raise_for_status()
        return resp.json() or {}
