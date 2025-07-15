import pytest
import requests
from unittest.mock import patch

from services.nba_stats import (
    get_scoreboard_html,
    get_game_summary,
    get_yesterdays_summer_stats,
)


def test_get_scoreboard_html_handles_error():
    with patch('services.nba_stats.requests.get', side_effect=requests.RequestException):
        with pytest.raises(requests.RequestException):
            get_scoreboard_html('20240101')


def test_get_game_summary_handles_error():
    with patch('services.nba_stats.requests.get', side_effect=requests.RequestException):
        with pytest.raises(requests.RequestException):
            get_game_summary('123')


def test_get_yesterdays_summer_stats_propagates_errors():
    # scoreboard failure propagates
    with patch('services.nba_stats.get_scoreboard_html', side_effect=requests.RequestException):
        with pytest.raises(requests.RequestException):
            get_yesterdays_summer_stats(['Foo'])

    # summary failure propagates
    sample_html = '<a href="/game/_/gameId/1/foo">Game</a>'
    with patch('services.nba_stats.get_scoreboard_html', return_value=sample_html), \
         patch('services.nba_stats.get_game_summary', side_effect=requests.RequestException):
        with pytest.raises(requests.RequestException):
            get_yesterdays_summer_stats(['Foo'])
