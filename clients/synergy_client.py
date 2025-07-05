import requests
from flask import current_app
from requests import RequestException
from datetime import datetime, timedelta

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


class SynergyDataCoreClient:
    """Client for Synergy Sports DataCore API using OAuth2."""

    token_url = "https://auth.synergysports.com/oauth/token"
    api_base = "https://datacore.synergysports.com/v1"

    def __init__(self, client_id: str, client_secret: str, timeout: float = 10.0):
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self._access_token = None
        self._token_expiry = datetime.min

    # ------------------------------------------------------------------
    #  OAuth token handling
    # ------------------------------------------------------------------
    def _get_token(self) -> str:
        if self._access_token and datetime.utcnow() < self._token_expiry:
            return self._access_token

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        try:
            resp = requests.post(self.token_url, data=data, timeout=self.timeout)
            resp.raise_for_status()
        except RequestException as exc:
            current_app.logger.warning("Synergy token fetch failed: %s", exc)
            raise

        payload = resp.json()
        self._access_token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 3600))
        self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in)
        return self._access_token

    def _headers(self) -> dict:
        token = self._get_token()
        return {"Authorization": f"Bearer {token}"}

    # ------------------------------------------------------------------
    #  API methods
    # ------------------------------------------------------------------
    def get_competitions(self) -> list:
        """Return a list of available competitions."""
        try:
            resp = requests.get(
                f"{self.api_base}/competitions",
                headers=self._headers(),
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except RequestException as exc:
            current_app.logger.warning("Synergy competitions fetch failed: %s", exc)
            return []

        return resp.json().get("competitions", [])

    def get_recent_games(self, competition_id: str, limit: int = 5) -> list:
        """Return recent completed games for a competition."""
        try:
            resp = requests.get(
                f"{self.api_base}/competitions/{competition_id}/games",
                params={"status": "final", "limit": limit},
                headers=self._headers(),
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except RequestException as exc:
            current_app.logger.warning("Synergy games fetch failed: %s", exc)
            return []

        return resp.json().get("games", [])

    def get_game_player_stats(self, game_id: str) -> list:
        """Return player stats for a given game."""
        try:
            resp = requests.get(
                f"{self.api_base}/games/{game_id}/stats",
                headers=self._headers(),
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except RequestException as exc:
            current_app.logger.warning("Synergy game stats fetch failed: %s", exc)
            return []

        return resp.json().get("players", [])

    def get_recent_games_with_stats(self, competition_id: str, limit: int = 5) -> list:
        """Convenience helper returning recent games and player stats."""
        games = self.get_recent_games(competition_id, limit=limit)
        results = []
        for g in games:
            stats = self.get_game_player_stats(g.get("id"))
            players = [
                {
                    "name": p.get("name"),
                    "points": p.get("points"),
                    "rebounds": p.get("rebounds"),
                    "assists": p.get("assists"),
                }
                for p in stats
            ]
            results.append(
                {
                    "game_id": g.get("id"),
                    "date": g.get("date"),
                    "home_team": g.get("home_team"),
                    "away_team": g.get("away_team"),
                    "players": players,
                }
            )
        return results
