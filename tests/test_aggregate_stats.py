import pytest
from types import SimpleNamespace

from admin.routes import aggregate_stats


def test_aggregate_stats_freq_percentages():
    stats = [
        SimpleNamespace(points=0, assists=0, turnovers=0,
                        atr_attempts=2, atr_makes=1,
                        fg2_attempts=2, fg2_makes=1,
                        fg3_attempts=1, fg3_makes=0,
                        fta=0, ftm=0, second_assists=0, pot_assists=0),
        SimpleNamespace(points=0, assists=0, turnovers=0,
                        atr_attempts=1, atr_makes=0,
                        fg2_attempts=2, fg2_makes=1,
                        fg3_attempts=3, fg3_makes=2,
                        fta=0, ftm=0, second_assists=0, pot_assists=0),
    ]
    agg = aggregate_stats(stats)
    assert agg.atr_freq_pct == 27.3
    assert agg.fg3_freq_pct == 36.4
