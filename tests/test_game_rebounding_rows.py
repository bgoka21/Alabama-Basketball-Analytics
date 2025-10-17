import csv
import os

from app import create_app
from models.database import db, PlayerStats, Season
from test_parse import parse_csv


def test_game_rebounding_opportunity_rows(tmp_path):
    csv_path = tmp_path / "game_rebounding.csv"
    rows = [
        [
            "Row",
            "PLAYER POSSESSIONS",
            "OPP STATS",
            "POSSESSION START",
            "POSSESSION TYPE",
            "PAINT TOUCHES",
            "SHOT CLOCK",
            "SHOT CLOCK PT",
            "TEAM",
            "#1 A",
            "#2 B",
        ],
        ["Offense", "", "", "", "", "", "", "", "Team", "", ""],
        ["Defense", "", "", "", "", "", "", "", "Team", "", ""],
        [
            "Offense Rebounding Opportunities",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "Off +, BM +",
            "Off -, BM -",
        ],
        [
            "Defense Rebounding Opportunities",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "Def +",
            "Def -, Given Up",
        ],
    ]
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    os.makedirs("instance", exist_ok=True)
    db_path = os.path.join("instance", "database.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    app = create_app()
    with app.app_context():
        db.drop_all()
        db.create_all()
        db.session.add(Season(id=1, season_name="2024"))
        db.session.commit()

        parse_csv(str(csv_path), game_id=None, season_id=1)

        player_a = PlayerStats.query.filter_by(player_name="#1 A").one()
        assert player_a.crash_positive == 1
        assert player_a.crash_missed == 0
        assert player_a.back_man_positive == 1
        assert player_a.back_man_missed == 0
        assert player_a.box_out_positive == 1
        assert player_a.box_out_missed == 0
        assert player_a.off_reb_given_up == 0

        player_b = PlayerStats.query.filter_by(player_name="#2 B").one()
        assert player_b.crash_positive == 0
        assert player_b.crash_missed == 1
        assert player_b.back_man_positive == 0
        assert player_b.back_man_missed == 1
        assert player_b.box_out_positive == 0
        assert player_b.box_out_missed == 1
        assert player_b.off_reb_given_up == 1
