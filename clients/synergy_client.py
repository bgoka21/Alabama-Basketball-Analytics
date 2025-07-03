import requests
from flask import current_app
from requests import RequestException

class SynergyAPI:
    """Simple wrapper around the Synergy Basketball API."""

    def __init__(self, timeout: float = 10.0):
        self.base = 'https://api.synergybasketball.com/v1'
        self.timeout = timeout

    @property
    def api_key(self):
        return current_app.config['SYNERGY_API_KEY']

    def find_player_id(self, name: str) -> str:
        try:
            resp = requests.get(
                f"{self.base}/players",
                params={'search': name},
                headers={'access-token': self.api_key},
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except RequestException as exc:
            current_app.logger.warning(
                "Synergy player lookup failed: %s", exc
            )
            return None

        players = resp.json().get('players', [])
        return players[0]['id'] if players else None

    def get_player_stats(self, player_id: str) -> dict:
        try:
            resp = requests.get(
                f"{self.base}/players/{player_id}/stats",
                headers={'access-token': self.api_key},
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except RequestException as exc:
            current_app.logger.warning(
                "Synergy player stats fetch failed: %s", exc
            )
            return {}

        return resp.json() or {}
