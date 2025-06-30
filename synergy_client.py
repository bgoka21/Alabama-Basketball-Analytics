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
