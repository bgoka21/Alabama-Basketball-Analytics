from types import SimpleNamespace

from admin.routes import _build_practice_cells


def test_adv_offensive_leverage_calculation():
    onoff = SimpleNamespace(
        offensive_possessions_on=10,
        defensive_possessions_on=12,
        offensive_possessions_off=8,
        defensive_possessions_off=9,
        ppp_on_offense=1.25,
        ppp_off_offense=1.00,
        ppp_on_defense=0.9,
        ppp_off_defense=1.1,
    )

    extras = {
        'good_shot_sum': 0,
        'good_shot_count': 0,
        'oreb_pct_sum': 0,
        'oreb_pct_count': 0,
    }

    cells = _build_practice_cells(
        totals={},
        blue={},
        extras=extras,
        session_count=1,
        mode='totals',
        onoff=onoff,
        to_rates={},
        reb_rates={},
    )

    assert cells['adv_ppp_on_offense']['data_value'] == 1.25
    assert cells['adv_ppp_off_offense']['data_value'] == 1.0
    assert cells['adv_offensive_leverage']['data_value'] == 0.25


def test_adv_ppp_fields_none_when_no_possessions():
    onoff = SimpleNamespace(
        offensive_possessions_on=0,
        defensive_possessions_on=0,
        offensive_possessions_off=0,
        defensive_possessions_off=0,
        ppp_on_offense=2.0,
        ppp_off_offense=1.5,
        ppp_on_defense=1.1,
        ppp_off_defense=1.0,
    )

    extras = {
        'good_shot_sum': 0,
        'good_shot_count': 0,
        'oreb_pct_sum': 0,
        'oreb_pct_count': 0,
    }

    cells = _build_practice_cells(
        totals={},
        blue={},
        extras=extras,
        session_count=1,
        mode='totals',
        onoff=onoff,
        to_rates={},
        reb_rates={},
    )

    for field in (
        'adv_ppp_on_offense',
        'adv_ppp_off_offense',
        'adv_ppp_on_defense',
        'adv_ppp_off_defense',
        'adv_offensive_leverage',
        'adv_defensive_leverage',
    ):
        assert cells[field]['display'] == '—'
        assert cells[field]['data_value'] == 0.0
