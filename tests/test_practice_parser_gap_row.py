from datetime import date

import pandas as pd

from models.database import db, Season, Practice, Roster, PlayerStats
from parse_practice_csv import parse_practice_csv


def test_gap_row_records_offense_and_defense(app, tmp_path):
    csv_path = tmp_path / "gap_row.csv"
    practice_date = date(2024, 10, 4)

    df = pd.DataFrame(
        [
            {
                "Row": "Crimson",
                "DRILL TYPE": "",
                "TEAM": "",
                "POSSESSION START": "",
                "POSSESSION TYPE": "",
                "PAINT TOUCHES": "",
                "SHOT CLOCK": "",
                "SHOT CLOCK PT": "",
                "CRIMSON PLAYER POSSESSIONS": "",
                "WHITE PLAYER POSSESSIONS": "",
                "Shot Location": "",
                "Label": "",
                "#0 LaBaron Philon": "Gap +, ATR+, 3FG-",
            }
        ]
    )
    df.to_csv(csv_path, index=False)

    with app.app_context():
        season = Season(id=1, season_name="2024-25", start_date=practice_date)
        db.session.add(season)
        practice = Practice(
            id=1,
            season_id=season.id,
            date=practice_date,
            category="Official Practice",
        )
        db.session.add(practice)
        db.session.add(Roster(id=1, season_id=season.id, player_name="#0 LaBaron Philon"))
        db.session.commit()

        parse_practice_csv(
            str(csv_path),
            season_id=season.id,
            category="Official Practice",
            file_date=practice_date,
        )

        stats = PlayerStats.query.filter_by(player_name="#0 LaBaron Philon").one()

        assert stats.collision_gap_positive == 1
        assert stats.pass_contest_positive == 0
        assert stats.atr_attempts == 1
        assert stats.atr_makes == 1
        assert stats.fg3_attempts == 1
        assert stats.fg3_makes == 0
        assert stats.points == 2
