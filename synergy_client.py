import os
import requests
from requests.exceptions import HTTPError


class SynergyClient:
    """Minimal wrapper for the Synergy Sports API."""

    def __init__(self, api_key: str | None = None):
        # Read the API key from the environment if not provided
        self.api_key = api_key or os.getenv("SYNERGY_API_KEY")

    def _get(self, path, params=None):
        headers = {"Authorization": f"Bearer {self.api_key}"}
        resp = requests.get(
            f"https://api.synergysportstech.com/v1{path}",
            headers=headers,
            params=params,
            timeout=5,
        )
        resp.raise_for_status()
        return resp.json()

    def search_players(self, query: str):
        from requests.exceptions import HTTPError
        endpoints = [
            ("/players", {"search": query}),
            ("/players/search", {"search": query}),
            ("/players/search", {"query": query}),
            ("/players", {"q": query}),
            ("/players/search", {"q": query}),
        ]
        for path, params in endpoints:
            try:
                data = self._get(path, params)
                return data.get("players", [])
            except HTTPError as e:
                # if it's a 404, try the next endpoint
                if e.response is not None and e.response.status_code == 404:
                    continue
                # otherwise re-raise
                raise
        # if none succeed, return empty list
        return []

    # backwards compatibility
    search = search_players

    def get_player_stats(self, player_id: str):
        data = self._get(f"/players/{player_id}/stats")
        stats = data.get("stats", {})
        return {
            "off_rating": stats.get("off_rating"),
            "def_rating": stats.get("def_rating"),
            "minutes_played": stats.get("minutes"),
            "three_fg_pct": stats.get("fg3_pct"),
            "ft_pct": stats.get("ft_pct"),
            "assists": stats.get("assists_per_game"),
            "turnovers": stats.get("turnovers_per_game"),
            "ast_to_to_ratio": stats.get("assist_turnover_ratio"),
        }
