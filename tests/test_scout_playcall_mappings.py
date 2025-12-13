from pathlib import Path

from models.scout import ScoutGame, ScoutPlaycallMapping, ScoutPossession, ScoutTeam
from scout.parsers.scout_playcalls import store_scout_playcalls
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
