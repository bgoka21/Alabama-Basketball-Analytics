import requests
from unittest.mock import patch

from services.nba_stats import (
    get_scoreboard_json,
    get_game_summary,
    get_yesterdays_summer_stats,
)


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        return self._payload


def test_get_scoreboard_json_handles_error():
    with patch('services.nba_stats.requests.get', side_effect=requests.RequestException):
        assert get_scoreboard_json('20240101') == {}


def test_get_game_summary_handles_error():
    with patch('services.nba_stats.requests.get', side_effect=requests.RequestException):
        assert get_game_summary('123') == {}


def test_get_yesterdays_summer_stats_skips_failed_games():
    scoreboard = {'events': [{'id': '1'}]}
    # First, simulate summary request failure
    def mock_get(url, timeout=10):
        if 'scoreboard' in url:
            return DummyResponse(scoreboard)
        raise requests.RequestException

    with patch('services.nba_stats.requests.get', side_effect=mock_get):
        stats = get_yesterdays_summer_stats(['Foo'])
        assert stats == {}

    # Next, simulate scoreboard failure
    with patch('services.nba_stats.requests.get', side_effect=requests.RequestException):
        assert get_yesterdays_summer_stats(['Foo']) == {}

