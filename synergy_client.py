class SynergyClient:
    """Simple stub for Synergy search used in tests."""

    def search(self, query: str):
        # In a real implementation this would query the Synergy API.
        # For now return a static list filtered by the query string.
        players = [
            {'id': '123', 'name': 'John Doe'},
            {'id': '456', 'name': 'Jane Smith'},
        ]
        q = query.lower()
        return [p for p in players if q in p['name'].lower()]

    def get_player_stats(self, player_id: str):
        # TODO: replace stub with real API call
        return {
            'off_rating': 90.0,
            'def_rating': 85.0,
            'minutes_played': 120.0,
            'three_fg_pct': 38.5,
            'ft_pct': 75.0,
            'assists': 4.1,
            'turnovers': 2.3,
            'ast_to_to_ratio': 1.78
        }
