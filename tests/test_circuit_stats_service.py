import pytest
from flask import Flask, render_template
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from models.database import db
from models.recruit import Recruit
from models.eybl import UnifiedStats
from models.user import User  # ensure users table exists for FK
from services.circuit_stats import get_circuit_stats_for_recruit, get_latest_circuit_stat
from services.eybl_ingest import normalize_and_merge
import pandas as pd
from types import SimpleNamespace
from flask import url_for as flask_url_for


@pytest.fixture
def app():
    templates = os.path.join(os.path.dirname(__file__), '..', 'templates')
    app = Flask(__name__, template_folder=templates)
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


def test_pnr_with_blanks():
    overall_df = pd.DataFrame({
        'Player': ['A', 'B'],
        'Team': ['X', 'Y'],
        'GP': [10, 10],
        'PPG': [15, 14],
    })
    assists_df = pd.DataFrame({
        'Player': ['A', 'B'],
        'Team': ['X', 'Y'],
        'AST/G': [5, 6],
        'Ast/TO': [2, 2],
    })
    pnr_df = pd.DataFrame({
        'Player': ['A', 'B'],
        'Team': ['X', 'Y'],
        'Poss': ['', '12'],
        'PPP': [0.8, 0.9],
        'TO%': ['15.9%', '15.9%'],
        'Score%': ['0.367', '0.367'],
    })
    merged = normalize_and_merge(overall_df, assists_df, pd.DataFrame(), pnr_df,
                                 circuit='EYBL', season_year=2024, season_type='AAU')
    assert merged['pnr_poss'].tolist() == [None, 12]
    assert merged['pnr_to_pct'].tolist() == [pytest.approx(0.159, rel=1e-3), pytest.approx(0.159, rel=1e-3)]
    assert merged['pnr_score_pct'].tolist() == [pytest.approx(0.367, rel=1e-3), pytest.approx(0.367, rel=1e-3)]


def test_preview_without_pnr(app):
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
    assert merged[['pnr_poss', 'pnr_ppp', 'pnr_to_pct', 'pnr_score_pct']].isna().all().all()
    with app.test_request_context():
        app.jinja_env.globals['current_user'] = SimpleNamespace(is_player=False)
        app.jinja_env.globals['view_exists'] = lambda *args, **kwargs: False
        app.jinja_env.globals['url_for'] = lambda endpoint, **values: (
            flask_url_for(endpoint, **values)
            if endpoint == 'static'
            else '#'
        )
        render_template(
            'admin/eybl_import_preview.html',
            circuit='EYBL',
            season_year=2024,
            season_type='AAU',
            total_rows=len(merged),
            counts={k: merged[k].notna().sum() for k in ['ppg','ast','tov','fg_pct','ppp','pnr_poss','pnr_ppp','pnr_to_pct','pnr_score_pct']},
            verified=0,
            pending=0,
            anomalies=[],
            rows=merged.to_dict(orient='records'),
            batch_dir='.',
            pnr_available=False,
        )
