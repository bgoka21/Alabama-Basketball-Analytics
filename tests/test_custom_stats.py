from datetime import date
import csv

import pytest
from bs4 import BeautifulSoup

from admin.routes import _build_game_table_dataset
from models.database import (
    db,
    Season,
    Practice,
    Game,
    PlayerStats,
    BlueCollarStats,
    Roster,
    Possession,
    PlayerPossession,
    ShotDetail,
)


@pytest.fixture
def sample_custom_stats(app):
    with app.app_context():
        season = Season(id=1, season_name='Test 2024', start_date=date(2024, 1, 1))
        db.session.add(season)

        roster = Roster(id=1, season_id=1, player_name='#1 Tester')
        db.session.add(roster)

        practices = [
            Practice(id=1, season_id=1, date=date(2024, 1, 2), category='Scrimmage'),
            Practice(id=2, season_id=1, date=date(2024, 1, 3), category='Scrimmage'),
        ]
        db.session.add_all(practices)

        games = [
            Game(
                id=1,
                season_id=1,
                game_date=date(2024, 2, 1),
                opponent_name='Opponent A',
                home_or_away='home',
            ),
            Game(
                id=2,
                season_id=1,
                game_date=date(2024, 2, 2),
                opponent_name='Opponent B',
                home_or_away='away',
            ),
        ]
        db.session.add_all(games)

        possessions = [
            Possession(id=101, game_id=1, season_id=1, time_segment='Offense', possession_side='Crimson'),
            Possession(id=102, game_id=1, season_id=1, time_segment='Defense', possession_side='Crimson'),
            Possession(id=201, game_id=2, season_id=1, time_segment='Offense', possession_side='Crimson'),
            Possession(id=202, game_id=2, season_id=1, time_segment='Defense', possession_side='Crimson'),
        ]
        db.session.add_all(possessions)

        player_possessions = [
            PlayerPossession(possession_id=pid, player_id=1)
            for pid in (101, 102, 201, 202)
        ]
        db.session.add_all(player_possessions)

        shot_details = []
        offense_events_game1 = [
            '2FG-',
            '2FG-',
            '3FG-',
            '3FG-',
            'ATR-',
            'Off Reb',
            'TEAM Off Reb',
        ]
        defense_events_game1 = [
            '2FG-',
            '2FG-',
            '3FG-',
            '3FG-',
            'ATR-',
            '2FG-',
            'Def Reb',
            'Def Reb',
            'TEAM Def Reb',
            'TEAM Def Reb',
        ]
        offense_events_game2 = [
            '2FG-',
            '2FG-',
            '3FG-',
            '3FG-',
            'ATR-',
            'Off Reb',
            'Off Reb',
            'TEAM Off Reb',
        ]
        defense_events_game2 = [
            '2FG-',
            '2FG-',
            '3FG-',
            '3FG-',
            'ATR-',
            '2FG-',
            'Def Reb',
            'Def Reb',
            'TEAM Def Reb',
            'TEAM Def Reb',
        ]

        for event in offense_events_game1:
            shot_details.append(ShotDetail(possession_id=101, event_type=event))
        for event in defense_events_game1:
            shot_details.append(ShotDetail(possession_id=102, event_type=event))
        for event in offense_events_game2:
            shot_details.append(ShotDetail(possession_id=201, event_type=event))
        for event in defense_events_game2:
            shot_details.append(ShotDetail(possession_id=202, event_type=event))

        db.session.add_all(shot_details)

        db.session.add_all(
            [
                PlayerStats(
                    practice_id=1,
                    season_id=1,
                    player_name='#1 Tester',
                    points=10,
                    assists=4,
                    turnovers=2,
                    fg2_attempts=4,
                    fg2_makes=2,
                    fg3_attempts=4,
                    fg3_makes=2,
                    atr_attempts=2,
                    atr_makes=1,
                    ftm=2,
                    fta=4,
                    crash_positive=2,
                    crash_missed=2,
                    back_man_positive=1,
                    back_man_missed=1,
                    box_out_positive=1,
                    box_out_missed=1,
                    foul_by=1,
                ),
                PlayerStats(
                    practice_id=2,
                    season_id=1,
                    player_name='#1 Tester',
                    points=20,
                    assists=6,
                    turnovers=4,
                    fg2_attempts=6,
                    fg2_makes=3,
                    fg3_attempts=3,
                    fg3_makes=1,
                    atr_attempts=3,
                    atr_makes=2,
                    ftm=4,
                    fta=6,
                    crash_positive=3,
                    crash_missed=1,
                    back_man_positive=2,
                    back_man_missed=1,
                    box_out_positive=2,
                    box_out_missed=1,
                    foul_by=2,
                ),
                PlayerStats(
                    game_id=1,
                    season_id=1,
                    player_name='#1 Tester',
                    points=12,
                    assists=5,
                    turnovers=3,
                    fg2_attempts=5,
                    fg2_makes=3,
                    fg3_attempts=4,
                    fg3_makes=2,
                    atr_attempts=2,
                    atr_makes=1,
                    ftm=3,
                    fta=4,
                ),
                PlayerStats(
                    game_id=2,
                    season_id=1,
                    player_name='#1 Tester',
                    points=16,
                    assists=7,
                    turnovers=3,
                    fg2_attempts=6,
                    fg2_makes=4,
                    fg3_attempts=5,
                    fg3_makes=2,
                    atr_attempts=3,
                    atr_makes=2,
                    ftm=5,
                    fta=6,
                ),
            ]
        )

        db.session.add_all(
            [
                BlueCollarStats(
                    practice_id=1,
                    season_id=1,
                    player_id=1,
                    deflection=1,
                    charge_taken=1,
                    floor_dive=0,
                    reb_tip=1,
                    misc=0,
                    steal=1,
                    block=0,
                    off_reb=2,
                    def_reb=3,
                    total_blue_collar=5,
                ),
                BlueCollarStats(
                    practice_id=2,
                    season_id=1,
                    player_id=1,
                    deflection=2,
                    charge_taken=0,
                    floor_dive=1,
                    reb_tip=1,
                    misc=1,
                    steal=1,
                    block=1,
                    off_reb=3,
                    def_reb=4,
                    total_blue_collar=7,
                ),
                BlueCollarStats(
                    game_id=1,
                    season_id=1,
                    player_id=1,
                    deflection=1,
                    charge_taken=0,
                    floor_dive=0,
                    reb_tip=1,
                    misc=0,
                    steal=1,
                    block=1,
                    off_reb=2,
                    def_reb=4,
                    total_blue_collar=6,
                ),
                BlueCollarStats(
                    game_id=2,
                    season_id=1,
                    player_id=1,
                    deflection=2,
                    charge_taken=1,
                    floor_dive=1,
                    reb_tip=1,
                    misc=1,
                    steal=0,
                    block=0,
                    off_reb=3,
                    def_reb=5,
                    total_blue_collar=8,
                ),
            ]
        )

        db.session.commit()

    yield


def test_game_field_catalog_endpoint_returns_grouped_options(client, sample_custom_stats):
    response = client.get('/admin/api/game/fields')
    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert not any(label.startswith('Practice •') for label in payload.keys())
    assert any('Game Leaderboard' in label for label in payload.keys())
    assert any('pts' in [field['key'] for field in fields] for fields in payload.values())


def test_custom_stats_table_partial_handles_practice_and_game(client, sample_custom_stats):
    practice_resp = client.post(
        '/admin/custom-stats/table-partial',
        json={
            'player_ids': [1],
            'fields': ['pts', 'play_to'],
            'mode': 'per_practice',
            'source': 'practice',
        },
    )
    assert practice_resp.status_code == 200
    practice_html = practice_resp.data.decode('utf-8')
    assert 'Custom practice stats' in practice_html
    practice_soup = BeautifulSoup(practice_html, 'html.parser')
    practice_row = practice_soup.select_one('tbody tr')
    assert practice_row is not None
    assert practice_row.select_one('td[data-key="pts"]').get_text(strip=True) == '15'
    assert practice_row.select_one('td[data-key="play_to"]').get_text(strip=True) == '3'

    game_resp = client.post(
        '/admin/custom-stats/table-partial',
        json={
            'player_ids': [1],
            'fields': ['pts', 'play_to'],
            'mode': 'per_game',
            'source': 'game',
        },
    )
    assert game_resp.status_code == 200
    game_html = game_resp.data.decode('utf-8')
    assert 'Custom game stats' in game_html
    game_soup = BeautifulSoup(game_html, 'html.parser')
    game_row = game_soup.select_one('tbody tr')
    assert game_row is not None
    assert game_row.select_one('td[data-key="pts"]').get_text(strip=True) == '14'
    assert game_row.select_one('td[data-key="play_to"]').get_text(strip=True) == '3'


def test_custom_game_table_formats_on_floor_rebound_rates(client, sample_custom_stats):
    with client.application.app_context():
        dataset = _build_game_table_dataset(
            {
                'player_ids': [1],
                'fields': [
                    'on_floor_indiv_oreb_pct',
                    'on_floor_team_oreb_pct',
                    'on_floor_indiv_dreb_pct',
                    'on_floor_team_dreb_pct',
                ],
                'mode': 'total',
                'source': 'game',
            }
        )

    row = dataset['rows'][0]

    assert row['on_floor_indiv_oreb_pct']['data_value'] == pytest.approx(50.0)
    assert row['on_floor_team_oreb_pct']['display'] == '50.0%'
    assert row['on_floor_indiv_dreb_pct']['data_value'] == pytest.approx(75.0)
    assert row['on_floor_team_dreb_pct']['display'] == '66.7%'


def test_custom_stats_csv_export_includes_source(client, sample_custom_stats):
    practice_resp = client.post(
        '/admin/custom-stats/export/csv',
        json={
            'player_ids': [1],
            'fields': ['pts'],
            'mode': 'per_practice',
            'source': 'practice',
        },
    )
    assert practice_resp.status_code == 200
    disposition = practice_resp.headers.get('Content-Disposition', '')
    assert 'custom_stats_practice_' in disposition
    practice_rows = list(csv.reader(practice_resp.data.decode('utf-8').splitlines()))
    assert practice_rows[0][0] == 'Player (Practice)'
    assert practice_rows[1][0] == '#1 Tester'
    assert practice_rows[1][1] == '15'

    game_resp = client.post(
        '/admin/custom-stats/export/csv',
        json={
            'player_ids': [1],
            'fields': ['pts', 'play_to'],
            'mode': 'per_game',
            'source': 'game',
        },
    )
    assert game_resp.status_code == 200
    disposition_game = game_resp.headers.get('Content-Disposition', '')
    assert 'custom_stats_game_' in disposition_game
    game_rows = list(csv.reader(game_resp.data.decode('utf-8').splitlines()))
    assert game_rows[0][0] == 'Player (Game)'
    assert game_rows[1][0] == '#1 Tester'
    assert game_rows[1][1] == '14'
    assert game_rows[1][2] == '3'


def test_game_table_counts_off_possessions_for_dnp(app):
    with app.app_context():
        season = Season(id=1, season_name='DNP Season', start_date=date(2024, 1, 1))
        db.session.add(season)

        roster = Roster(id=10, season_id=1, player_name='#10 DNP')
        db.session.add(roster)

        game = Game(
            id=1,
            season_id=1,
            game_date=date(2024, 2, 1),
            opponent_name='Opponent DNP',
            home_or_away='home',
        )
        db.session.add(game)

        possession = Possession(
            id=1,
            game_id=1,
            season_id=1,
            time_segment='Offense',
            possession_side='Crimson',
            points_scored=2,
        )
        db.session.add(possession)

        db.session.commit()

        dataset = _build_game_table_dataset(
            {
                'player_ids': [roster.id],
                'fields': ['adv_ppp_off_offense', 'adv_off_possession_pct'],
                'mode': 'total',
            }
        )

        assert dataset['rows'], 'Expected a dataset row for the roster entry'
        row = dataset['rows'][0]

        assert row['adv_ppp_off_offense']['data_value'] == 2.0
        assert row['adv_ppp_off_offense']['display'] == '2.00'
        assert row['adv_off_possession_pct']['data_value'] == 0
        assert row['adv_off_possession_pct']['display'] == '0.0%'
