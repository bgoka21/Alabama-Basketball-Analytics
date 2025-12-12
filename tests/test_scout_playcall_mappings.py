from pathlib import Path

from models.scout import (
    ScoutGame,
    ScoutPlaycallMapping,
    ScoutPossession,
    ScoutTeam,
    normalize_playcall,
)
from scout.parsers.scout_playcalls import store_scout_playcalls
from scout.routes import _save_playcall_mapping
from models.database import db


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(content, encoding='utf-8')
    return path


def test_store_scout_playcalls_applies_saved_mapping(tmp_path, app):
    csv_content = """Instance Number,Playcall,Series,Family,Shot
1,Set A,Uploaded Series,Uploaded Family,3
"""
    csv_path = _write_csv(tmp_path / 'playcalls.csv', csv_content)

    with app.app_context():
        team = ScoutTeam(name='Mapping Team')
        db.session.add(team)
        db.session.flush()

        game = ScoutGame(scout_team_id=team.id)
        db.session.add(game)

        mapping = ScoutPlaycallMapping.from_playcall('Set A')
        mapping.canonical_series = 'Mapped Series'
        mapping.canonical_family = 'Mapped Family'
        db.session.commit()

        created_count = store_scout_playcalls(str(csv_path), game)
        assert created_count == 1

        possession = ScoutPossession.query.filter_by(scout_game_id=game.id).first()
        assert possession.series == 'Mapped Series'
        assert possession.family == 'Mapped Family'
        assert possession.playcall == 'Set A'


def test_save_playcall_mapping_backfills_all_possessions(app):
    with app.app_context():
        team = ScoutTeam(name='Backfill Team')
        db.session.add(team)
        db.session.flush()

        game_one = ScoutGame(scout_team_id=team.id)
        game_two = ScoutGame(scout_team_id=team.id)
        db.session.add_all([game_one, game_two])
        db.session.flush()

        possession_one = ScoutPossession(
            scout_game_id=game_one.id,
            instance_number='1',
            playcall='Play X',
            series='Old',
            family='Legacy',
            bucket='STANDARD',
            points=0,
        )
        possession_two = ScoutPossession(
            scout_game_id=game_two.id,
            instance_number='2',
            playcall='play x',
            series='Old',
            family='Legacy',
            bucket='STANDARD',
            points=0,
        )
        db.session.add_all([possession_one, possession_two])
        db.session.commit()

        updated = _save_playcall_mapping(
            'Play X',
            'New Series',
            'New Family',
            {game_one.id},
            apply_globally=True,
        )
        db.session.commit()

        assert updated == 2

        mapping = ScoutPlaycallMapping.query.filter_by(
            playcall_key=normalize_playcall('Play X')
        ).first()
        assert mapping is not None
        assert mapping.canonical_series == 'New Series'
        assert mapping.canonical_family == 'New Family'

        refreshed = ScoutPossession.query.order_by(ScoutPossession.instance_number).all()
        assert {pos.series for pos in refreshed} == {'New Series'}
        assert {pos.family for pos in refreshed} == {'New Family'}
