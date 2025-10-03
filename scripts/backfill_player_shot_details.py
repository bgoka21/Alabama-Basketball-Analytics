"""Backfill structured player shot detail rows from serialized JSON blobs."""

import json
from typing import Iterable, Mapping

from sqlalchemy.orm import selectinload

from app import create_app
from models.database import PlayerStats, db
from utils.shottype import persist_player_shot_details


def _load_shots(blob) -> list[Mapping]:
    if not blob:
        return []
    data = blob
    if isinstance(blob, str):
        try:
            data = json.loads(blob)
        except ValueError:
            return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, Mapping)]
    if isinstance(data, Mapping):
        return [data]
    return []


def main() -> None:
    app = create_app()
    with app.app_context():
        stats: Iterable[PlayerStats] = PlayerStats.query.options(
            selectinload(PlayerStats.player_shot_details)
        ).all()
        updated = 0
        inserted_details = 0
        for stat in stats:
            shots = _load_shots(stat.shot_type_details)
            if not shots:
                continue
            if stat.player_shot_details:
                continue
            persist_player_shot_details(stat, shots, replace=True)
            updated += 1
            inserted_details += len(shots)
        if updated:
            db.session.commit()
        print(
            f"✅ Backfill complete. Updated {updated} PlayerStats rows and created "
            f"{inserted_details} PlayerShotDetail records."
        )


if __name__ == "__main__":
    main()
