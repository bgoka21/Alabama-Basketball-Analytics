from datetime import date

import pytest

from models.database import (
    BlueCollarStats,
    Game,
    PlayerStats,
    Practice,
    Roster,
    Season,
    db,
)
from services.correlation import (
    MetricDefinition,
    MetricSource,
    StudyDefinition,
    StudyScope,
    run_studies,
)


def _create_roster(season: Season, names: list[str]) -> dict[str, Roster]:
    roster_entries = {}
    for name in names:
        entry = Roster(season_id=season.id, player_name=name)
        db.session.add(entry)
        roster_entries[name] = entry
    db.session.commit()
    return roster_entries


def _add_practice(
    season: Season,
    practice_date: date,
    category: str,
    stats: dict[str, dict[str, int]],
    blue: dict[str, dict[str, int]],
    roster_lookup: dict[str, Roster],
) -> Practice:
    practice = Practice(season_id=season.id, date=practice_date, category=category)
    db.session.add(practice)
    db.session.flush()

    for player, values in stats.items():
        entry = PlayerStats(
            season_id=season.id,
            practice_id=practice.id,
            player_name=player,
            **values,
        )
        db.session.add(entry)

    for player, values in blue.items():
        roster = roster_lookup[player]
        entry = BlueCollarStats(
            season_id=season.id,
            practice_id=practice.id,
            player_id=roster.id,
            **values,
        )
        db.session.add(entry)

    db.session.commit()
    return practice


def _add_game(
    season: Season,
    game_date: date,
    opponent: str,
    stats: dict[str, dict[str, int]],
) -> Game:
    game = Game(
        season_id=season.id,
        game_date=game_date,
        opponent_name=opponent,
        home_or_away="home",
        result="W",
    )
    db.session.add(game)
    db.session.flush()

    for player, values in stats.items():
        entry = PlayerStats(
            season_id=season.id,
            game_id=game.id,
            player_name=player,
            **values,
        )
        db.session.add(entry)

    db.session.commit()
    return game


@pytest.fixture
def season(app):
    with app.app_context():
        season = Season(season_name="2024-25")
        db.session.add(season)
        db.session.commit()
        yield season


@pytest.fixture
def practice_and_game_data(app, season):
    with app.app_context():
        roster_lookup = _create_roster(season, ["Alice", "Bob"])

        base_stats_1 = {
            "Alice": {
                "fg3_makes": 3,
                "fg3_attempts": 6,
                "fg2_makes": 4,
                "fg2_attempts": 8,
                "atr_makes": 0,
                "atr_attempts": 0,
                "points": 14,
                "assists": 5,
                "turnovers": 2,
                "second_assists": 1,
                "pot_assists": 2,
                "ftm": 2,
                "fta": 2,
                "crash_positive": 2,
                "crash_missed": 1,
                "back_man_positive": 1,
                "back_man_missed": 1,
                "box_out_positive": 2,
                "box_out_missed": 1,
                "off_reb_given_up": 1,
            },
            "Bob": {
                "fg3_makes": 1,
                "fg3_attempts": 5,
                "fg2_makes": 3,
                "fg2_attempts": 7,
                "atr_makes": 0,
                "atr_attempts": 0,
                "points": 11,
                "assists": 3,
                "turnovers": 3,
                "second_assists": 0,
                "pot_assists": 1,
                "ftm": 1,
                "fta": 2,
                "crash_positive": 1,
                "crash_missed": 2,
                "back_man_positive": 1,
                "back_man_missed": 2,
                "box_out_positive": 1,
                "box_out_missed": 2,
                "off_reb_given_up": 2,
            },
        }

        blue_stats_1 = {
            "Alice": {
                "total_blue_collar": 6,
                "deflection": 3,
                "charge_taken": 1,
                "floor_dive": 1,
                "reb_tip": 2,
                "misc": 1,
                "steal": 2,
                "block": 1,
                "off_reb": 3,
                "def_reb": 4,
            },
            "Bob": {
                "total_blue_collar": 4,
                "deflection": 2,
                "charge_taken": 0,
                "floor_dive": 1,
                "reb_tip": 1,
                "misc": 2,
                "steal": 1,
                "block": 0,
                "off_reb": 2,
                "def_reb": 3,
            },
        }

        _add_practice(
            season,
            date(2024, 10, 1),
            "Fall",
            base_stats_1,
            blue_stats_1,
            roster_lookup,
        )

        base_stats_2 = {
            "Alice": {
                "fg3_makes": 1,
                "fg3_attempts": 4,
                "fg2_makes": 2,
                "fg2_attempts": 5,
                "atr_makes": 0,
                "atr_attempts": 0,
                "points": 9,
                "assists": 4,
                "turnovers": 1,
                "second_assists": 1,
                "pot_assists": 1,
                "ftm": 1,
                "fta": 2,
                "crash_positive": 1,
                "crash_missed": 1,
                "back_man_positive": 1,
                "back_man_missed": 0,
                "box_out_positive": 1,
                "box_out_missed": 1,
                "off_reb_given_up": 0,
            },
            "Bob": {
                "fg3_makes": 0,
                "fg3_attempts": 5,
                "fg2_makes": 2,
                "fg2_attempts": 6,
                "atr_makes": 0,
                "atr_attempts": 0,
                "points": 8,
                "assists": 2,
                "turnovers": 2,
                "second_assists": 0,
                "pot_assists": 1,
                "ftm": 0,
                "fta": 1,
                "crash_positive": 1,
                "crash_missed": 2,
                "back_man_positive": 0,
                "back_man_missed": 1,
                "box_out_positive": 1,
                "box_out_missed": 1,
                "off_reb_given_up": 1,
            },
        }

        blue_stats_2 = {
            "Alice": {
                "total_blue_collar": 5,
                "deflection": 2,
                "charge_taken": 0,
                "floor_dive": 1,
                "reb_tip": 1,
                "misc": 1,
                "steal": 1,
                "block": 1,
                "off_reb": 2,
                "def_reb": 5,
            },
            "Bob": {
                "total_blue_collar": 3,
                "deflection": 1,
                "charge_taken": 0,
                "floor_dive": 0,
                "reb_tip": 1,
                "misc": 1,
                "steal": 1,
                "block": 0,
                "off_reb": 1,
                "def_reb": 2,
            },
        }

        _add_practice(
            season,
            date(2024, 10, 3),
            "Fall",
            base_stats_2,
            blue_stats_2,
            roster_lookup,
        )

        _add_game(
            season,
            date(2024, 11, 5),
            "First Opponent",
            {
                "Alice": {
                    "fg3_makes": 2,
                    "fg3_attempts": 5,
                    "fg2_makes": 5,
                    "fg2_attempts": 9,
                    "atr_makes": 0,
                    "atr_attempts": 0,
                    "points": 18,
                    "assists": 6,
                    "turnovers": 3,
                    "ftm": 2,
                    "fta": 3,
                },
                "Bob": {
                    "fg3_makes": 1,
                    "fg3_attempts": 4,
                    "fg2_makes": 4,
                    "fg2_attempts": 8,
                    "atr_makes": 0,
                    "atr_attempts": 0,
                    "points": 12,
                    "assists": 4,
                    "turnovers": 2,
                    "ftm": 1,
                    "fta": 2,
                },
            },
        )

        _add_game(
            season,
            date(2024, 11, 12),
            "Second Opponent",
            {
                "Alice": {
                    "fg3_makes": 1,
                    "fg3_attempts": 3,
                    "fg2_makes": 4,
                    "fg2_attempts": 7,
                    "atr_makes": 0,
                    "atr_attempts": 0,
                    "points": 12,
                    "assists": 5,
                    "turnovers": 2,
                    "ftm": 3,
                    "fta": 4,
                },
                "Bob": {
                    "fg3_makes": 0,
                    "fg3_attempts": 3,
                    "fg2_makes": 3,
                    "fg2_attempts": 6,
                    "atr_makes": 0,
                    "atr_attempts": 0,
                    "points": 6,
                    "assists": 3,
                    "turnovers": 3,
                    "ftm": 2,
                    "fta": 2,
                },
            },
        )

        return {
            "alice_id": roster_lookup["Alice"].id,
            "bob_id": roster_lookup["Bob"].id,
        }


def test_run_studies_mixed_sources(app, season, practice_and_game_data):
    with app.app_context():
        scope = StudyScope(season_id=season.id)
        studies = [
            StudyDefinition(
                identifier="practice_fg3_vs_game_points",
                x=MetricDefinition(MetricSource.PRACTICE, "shooting_fg3_pct"),
                y=MetricDefinition(MetricSource.GAME, "points"),
            )
        ]

        payload = run_studies(studies, scope)
        assert "studies" in payload
        result = payload["studies"][0]

        assert result["samples"] == 2
        assert pytest.approx(result["pearson"], abs=1e-12) == 1.0
        assert pytest.approx(result["spearman"], abs=1e-12) == 1.0

        scatter = {point["player"]: point for point in result["scatter"]}
        assert set(scatter.keys()) == {"Alice", "Bob"}
        assert scatter["Alice"]["x"] == pytest.approx(40.0)
        assert scatter["Alice"]["y"] == pytest.approx(30.0)
        assert scatter["Bob"]["x"] == pytest.approx(10.0)
        assert scatter["Bob"]["y"] == pytest.approx(18.0)


def test_single_player_returns_no_correlation(app, season, practice_and_game_data):
    with app.app_context():
        alice_id = practice_and_game_data["alice_id"]

        scope = StudyScope(season_id=season.id, roster_ids=[alice_id])
        studies = [
            StudyDefinition(
                identifier="single-player",
                x=MetricDefinition(MetricSource.PRACTICE, "play_ast"),
                y=MetricDefinition(MetricSource.PRACTICE, "play_to"),
            )
        ]

        payload = run_studies(studies, scope)
        result = payload["studies"][0]

        assert result["samples"] == 1
        assert result["pearson"] is None
        assert result["spearman"] is None
        assert len(result["scatter"]) == 1


def test_empty_dataset_returns_zero_samples(app, season):
    with app.app_context():
        new_season = Season(season_name="2025-26")
        db.session.add(new_season)
        db.session.commit()

        scope = StudyScope(season_id=new_season.id)
        studies = [
            StudyDefinition(
                identifier="empty",
                x=MetricDefinition(MetricSource.PRACTICE, "play_ast"),
                y=MetricDefinition(MetricSource.GAME, "points"),
            )
        ]

        payload = run_studies(studies, scope)
        result = payload["studies"][0]
        assert result["samples"] == 0
        assert result["scatter"] == []


def test_unknown_metric_raises_value_error(app, season):
    with app.app_context():
        scope = StudyScope(season_id=season.id)
        studies = [
            StudyDefinition(
                identifier="invalid",
                x=MetricDefinition(MetricSource.PRACTICE, "not-a-real-metric"),
                y=MetricDefinition(MetricSource.GAME, "points"),
            )
        ]

        with pytest.raises(ValueError):
            run_studies(studies, scope)
