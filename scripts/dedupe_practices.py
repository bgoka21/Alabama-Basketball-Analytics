"""Utility to merge duplicate Practice rows sharing the same season/date."""

from collections import defaultdict
import os
import sys

from sqlalchemy import inspect

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app import create_app, db
from app.utils.category_normalization import normalize_category
from models.database import (
    Practice,
    PlayerStats,
    BlueCollarStats,
    TeamStats,
    OpponentBlueCollarStats,
    Possession,
    PnRStats,
)


def _gather_groups(practices):
    groups = defaultdict(list)
    for practice in practices:
        groups[(practice.season_id, practice.date)].append(practice)
    return groups


def _select_survivor(practices):
    """Return the Practice row that should remain after dedupe."""
    if not practices:
        return None, None

    canonical_label = None
    for practice in practices:
        normalized = normalize_category(practice.category)
        if normalized:
            canonical_label = normalized
            break
    if canonical_label is None:
        canonical_label = normalize_category(practices[0].category or "")

    survivor = None
    for practice in practices:
        if practice.category == canonical_label:
            survivor = practice
            break
    if survivor is None:
        for practice in practices:
            if normalize_category(practice.category) == canonical_label:
                survivor = practice
                break
    if survivor is None:
        survivor = practices[0]

    return survivor, canonical_label


def _reassign_children(old_practice_id, new_practice_id):
    for model in (TeamStats, PlayerStats, BlueCollarStats, OpponentBlueCollarStats, Possession, PnRStats):
        for row in model.query.filter_by(practice_id=old_practice_id):
            row.practice_id = new_practice_id


def run():
    app = create_app()
    with app.app_context():
        inspector = inspect(db.engine)
        if "practice" not in inspector.get_table_names():
            print("Practice table not found; nothing to dedupe.")
            return

        practices = Practice.query.order_by(Practice.season_id, Practice.date, Practice.id).all()
        groups = _gather_groups(practices)

        total_merged = 0
        for key, items in groups.items():
            if len(items) <= 1:
                continue

            survivor, canonical_label = _select_survivor(items)
            if survivor is None:
                continue

            for practice in items:
                if practice.id == survivor.id:
                    continue

                _reassign_children(practice.id, survivor.id)
                db.session.flush()
                db.session.delete(practice)
                total_merged += 1

            if canonical_label and survivor.category != canonical_label:
                survivor.category = canonical_label

        db.session.commit()
        print(f"Merged {total_merged} duplicate practice rows.")


if __name__ == "__main__":
    run()
