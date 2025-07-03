import os
import requests


class SynergyClient:
    """Minimal wrapper for the Synergy Sports API."""

    def __init__(self, api_key: str | None = None, timeout: float = 10.0):
        self.api_key = api_key or os.getenv("SYNERGY_API_KEY") or "0vBg4oX7mqNx"
        self.base = "https://api.synergysportstech.com/v1"
        self.timeout = timeout

    def _get(self, path: str, **kwargs):
        url = f"{self.base}{path}"
        headers = {"Authorization": f"Bearer {self.api_key}"}
        resp = requests.get(url, headers=headers, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp.json() or {}

    def search_players(self, query: str):
        data = self._get("/players", params={"search": query})
        return data.get("players", [])

    # backwards compatibility
    search = search_players

    def get_player_stats(self, player_id: str):
        data = self._get(f"/players/{player_id}/stats")
        return {
            "off_rating": data.get("offensiveRating"),
            "def_rating": data.get("defensiveRating"),
            "minutes_played": data.get("minutesPlayed"),
            "three_fg_pct": data.get("threePointPct"),
            "ft_pct": data.get("freeThrowPct"),
            "assists": data.get("assists"),
            "turnovers": data.get("turnovers"),
            "ast_to_to_ratio": data.get("assistTurnoverRatio"),
        }
