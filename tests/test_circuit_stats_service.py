import pytest
from flask import Flask
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.database import db
from models.recruit import Recruit
from models.eybl import UnifiedStats
from services.circuit_stats import get_circuit_stats_for_recruit, get_latest_circuit_stat
from services.eybl_ingest import normalize_and_merge
import pandas as pd


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['TESTING'] = True
    db.init_app(app)
    with app.app_context():
        db.create_all()
    yield app
    with app.app_context():
        db.drop_all()


def _create_stats(recruit_id):
    rows = [
        UnifiedStats(recruit_id=recruit_id, circuit='EYBL', season_year=2023, ppg=10,
                     ingested_at=datetime(2023,5,1)),
        UnifiedStats(recruit_id=recruit_id, circuit='EYBL', season_year=2024, ppg=12,
                     ingested_at=datetime(2024,5,1)),
        UnifiedStats(recruit_id=recruit_id, circuit='UA', season_year=2024, ppg=15,
                     ingested_at=datetime(2024,6,1)),
    ]
    db.session.add_all(rows)
    db.session.commit()


def test_get_circuit_stats_for_recruit(app):
    with app.app_context():
        r = Recruit(name='Test')
        db.session.add(r)
        db.session.commit()
        _create_stats(r.id)
        stats = get_circuit_stats_for_recruit(r.id)
        assert [s['circuit'] for s in stats] == ['UA', 'EYBL', 'EYBL']
        stats_eybl = get_circuit_stats_for_recruit(r.id, circuits=['EYBL'])
        assert len(stats_eybl) == 2
        stats_2024 = get_circuit_stats_for_recruit(r.id, season_year=2024)
        assert all(s['season_year'] == 2024 for s in stats_2024)


def test_get_latest_circuit_stat(app):
    with app.app_context():
        r = Recruit(name='Test2')
        db.session.add(r)
        db.session.commit()
        _create_stats(r.id)
        latest = get_latest_circuit_stat(r.id, 'EYBL')
        assert latest['season_year'] == 2024
        assert latest['ppg'] == 12


def test_normalize_and_merge_no_pnr():
    overall_df = pd.DataFrame({
        'Player': ['A'],
        'Team': ['X'],
        'GP': [10],
        'PPG': [15],
    })
    assists_df = pd.DataFrame({
        'Player': ['A'],
        'Team': ['X'],
        'AST/G': [5],
        'Ast/TO': [2],
    })
    merged = normalize_and_merge(overall_df, assists_df, pd.DataFrame(), None,
                                 circuit='EYBL', season_year=2024, season_type='AAU')
    assert list(merged.columns).count('pnr_poss') == 1
    assert merged['pnr_poss'].isna().all()
    assert merged['pnr_ppp'].isna().all()
    assert merged['pnr_to_pct'].isna().all()
    assert merged['pnr_score_pct'].isna().all()


def test_normalize_and_merge_with_pnr():
    overall_df = pd.DataFrame({
        'Player': ['A'],
        'Team': ['X'],
        'GP': [10],
        'PPG': [15],
    })
    assists_df = pd.DataFrame({
        'Player': ['A'],
        'Team': ['X'],
        'AST/G': [5],
        'Ast/TO': [2],
    })
    pnr_df = pd.DataFrame({
        'Player': ['A'],
        'Team': ['X'],
        'Poss': [20],
        'PPP': [0.9],
        'TO%': ['15.9%'],
        'Score%': ['45'],
    })
    merged = normalize_and_merge(overall_df, assists_df, pd.DataFrame(), pnr_df,
                                 circuit='EYBL', season_year=2024, season_type='AAU')
    row = merged.iloc[0]
    assert row.pnr_poss == 20
    assert row.pnr_ppp == 0.9
    assert 0 < row.pnr_to_pct < 1
    assert 0 < row.pnr_score_pct < 1
