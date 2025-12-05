"""Debug helper for COOE/PPP ON-OFF values for LaBaron Philon."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import create_app
from models.database import Game, Roster
from utils.player_stats_helpers.cooe import get_game_on_off_stats


TRUTH: Dict[str, Dict[str, float]] = {
    "North_Dakota": {
        "poss_on": 60,
        "poss_off": 15,
        "pts_on": 80,
        "pts_off": 11,
        "ppp_on": 1.3333,
        "ppp_off": 0.7333,
        "cooe": 0.6,
    },
    "St_Johns": {
        "poss_on": 64,
        "poss_off": 19,
        "pts_on": 80,
        "pts_off": 23,
        "ppp_on": 1.25,
        "ppp_off": 1.2105,
        "cooe": 0.04,
    },
}


def _find_player() -> Optional[Roster]:
    return (
        Roster.query.filter(Roster.player_name.ilike("%LaBaron Philon%"))
        .order_by(Roster.id)
        .first()
    )


def _find_game_by_fragment(fragment: str) -> Optional[Game]:
    return (
        Game.query.filter(Game.csv_filename.contains(fragment))
        .order_by(Game.id)
        .first()
    )


def _fmt(value: Optional[float]) -> str:
    if value is None:
        return "None"
    return f"{value:.4f}" if isinstance(value, float) else str(value)


def _print_truth_and_app(label: str, game: Game, player: Roster):
    truth = TRUTH[label]
    stats = get_game_on_off_stats([game.id], player.id)

    app_vals = {
        "poss_on": stats.offensive_possessions_on or 0,
        "poss_off": stats.offensive_possessions_off or 0,
        "pts_on": stats.points_on_offense or 0.0,
        "pts_off": stats.points_off_offense or 0.0,
        "ppp_on": stats.ppp_on_offense,
        "ppp_off": stats.ppp_off_offense,
        "cooe": stats.adv_offensive_leverage,
    }

    diffs = {
        key: (app_vals[key] - truth[key]) if app_vals[key] is not None else None
        for key in truth
    }

    print(f"=== {game.csv_filename} – LaBaron Philon ===")

    print("\nTRUTH (Sportscode OFF POSS EFF):")
    print(f"  Off poss ON   = {truth['poss_on']}")
    print(f"  Off poss OFF  = {truth['poss_off']}")
    print(f"  Points ON     = {truth['pts_on']}")
    print(f"  Points OFF    = {truth['pts_off']}")
    print(f"  PPP ON        = {_fmt(truth['ppp_on'])}")
    print(f"  PPP OFF       = {_fmt(truth['ppp_off'])}")
    print(f"  COOE          = {_fmt(truth['cooe'])}")

    print("\nAPP (DB + cooe.py):")
    print(f"  Off poss ON   = {app_vals['poss_on']}")
    print(f"  Off poss OFF  = {app_vals['poss_off']}")
    print(f"  Points ON     = {app_vals['pts_on']}")
    print(f"  Points OFF    = {app_vals['pts_off']}")
    print(f"  PPP ON        = {_fmt(app_vals['ppp_on'])}")
    print(f"  PPP OFF       = {_fmt(app_vals['ppp_off'])}")
    print(f"  COOE          = {_fmt(app_vals['cooe'])}")

    print("\nDIFF (APP – TRUTH):")
    print(f"  poss ON   = {diffs['poss_on']}")
    print(f"  poss OFF  = {diffs['poss_off']}")
    print(f"  pts ON    = {diffs['pts_on']}")
    print(f"  pts OFF   = {diffs['pts_off']}")
    print(f"  PPP ON    = {_fmt(diffs['ppp_on'])}")
    print(f"  PPP OFF   = {_fmt(diffs['ppp_off'])}")
    print(f"  COOE      = {_fmt(diffs['cooe'])}")
    print("")


def main():
    app = create_app()
    with app.app_context():
        games = {
            "North_Dakota": _find_game_by_fragment("North_Dakota"),
            "St_Johns": _find_game_by_fragment("St_Johns"),
        }

        print("Game records:")
        for label, game in games.items():
            if game:
                print(
                    f"  [{label}] id={game.id}, date={game.game_date}, opponent={game.opponent_name}, file={game.csv_filename}"
                )
            else:
                print(f"  [{label}] not found")

        player = _find_player()
        if not player:
            print("LaBaron Philon not found in roster")
            return

        print(f"\nPlayer: id={player.id}, name={player.player_name}, season={player.season_id}\n")

        for label in ("North_Dakota", "St_Johns"):
            game = games.get(label)
            if not game:
                print(f"Skipping {label}: game not loaded")
                continue
            _print_truth_and_app(label, game, player)


if __name__ == "__main__":
    main()
