from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import inspect, text

from scout.parsers.scout_playcalls import parse_playcalls_csv
from scout.schema import ensure_scout_possession_schema


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_parse_playcalls_allows_missing_shot_column(tmp_path):
    csv_content = """Instance Number,Playcall
1,Set A
2,Transition Push
"""
    csv_path = _write_csv(tmp_path / "playcalls.csv", csv_content)

    possessions = parse_playcalls_csv(str(csv_path))

    assert len(possessions) == 1
    assert possessions[0]["instance_number"] == "1"
    assert possessions[0]["playcall"] == "Set A"
    assert possessions[0]["bucket"] == "STANDARD"
    assert possessions[0]["points"] == 0


def test_ensure_schema_adds_missing_columns():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE scout_possessions (
                    id INTEGER PRIMARY KEY,
                    scout_game_id INTEGER,
                    instance_number TEXT,
                    playcall TEXT,
                    bucket TEXT,
                    points INTEGER
                )
                """
            )
        )

    columns_before = {col["name"] for col in inspect(engine).get_columns("scout_possessions")}
    assert "family" not in columns_before
    assert "series" not in columns_before

    ensure_scout_possession_schema(engine)

    columns_after = {col["name"] for col in inspect(engine).get_columns("scout_possessions")}
    assert "family" in columns_after
    assert "series" in columns_after
