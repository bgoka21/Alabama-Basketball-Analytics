import json
import os
from datetime import datetime
from typing import Optional

from flask import current_app

from models.database import (
    BlueCollarStats,
    Game,
    OpponentBlueCollarStats,
    PlayerPossession,
    PlayerStats,
    Possession,
    Practice,
    TeamStats,
    db,
)
from models.uploaded_file import UploadedFile


def _format_lineup_efficiencies(raw_lineups: dict) -> dict:
    formatted = {}
    for size, sides in raw_lineups.items():
        formatted[size] = {
            side: {",".join(combo): ppp for combo, ppp in side_data.items()}
            for side, side_data in sides.items()
        }
    return formatted


def reparse_uploaded_file(file: UploadedFile) -> Optional[dict]:
    """Re-parse an uploaded CSV using existing Game/Practice records.

    This helper clears previously parsed stats for the associated record,
    re-runs the appropriate parser, and refreshes the UploadedFile metadata.
    """

    upload_folder = current_app.config.get("UPLOAD_FOLDER", "")
    filepath = os.path.join(upload_folder, file.filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"File '{file.filename}' not found in upload folder '{upload_folder}'"
        )

    result: Optional[dict] = None

    if file.category == "Game":
        game = Game.query.filter_by(
            season_id=file.season_id, csv_filename=file.filename
        ).first()
        if game is None:
            raise RuntimeError(
                "No existing Game found for reparse; refusing to create a new one."
            )

        TeamStats.query.filter_by(game_id=game.id).delete()
        PlayerStats.query.filter_by(game_id=game.id).delete()
        BlueCollarStats.query.filter_by(game_id=game.id).delete()
        OpponentBlueCollarStats.query.filter_by(game_id=game.id).delete()

        possession_ids = [p.id for p in Possession.query.filter_by(game_id=game.id).all()]
        if possession_ids:
            PlayerPossession.query.filter(
                PlayerPossession.possession_id.in_(possession_ids)
            ).delete(synchronize_session=False)
        Possession.query.filter_by(game_id=game.id).delete()
        db.session.commit()

        from admin import routes as admin_routes

        result = admin_routes.parse_csv(
            filepath, game_id=None, season_id=file.season_id
        )

    elif file.category in {"Official Practice", "Fall Workouts", "Summer Workouts", "Pickup"}:
        from app.utils.category_normalization import normalize_category

        from admin import routes as admin_routes

        normalized_category = normalize_category(file.category)
        practice = Practice.query.filter_by(
            season_id=file.season_id,
            date=file.file_date,
            category=normalized_category,
        ).first()
        if practice is None:
            raise RuntimeError(
                "No existing Practice found for reparse; refusing to create a new one."
            )

        TeamStats.query.filter_by(practice_id=practice.id).delete()
        PlayerStats.query.filter_by(practice_id=practice.id).delete()
        BlueCollarStats.query.filter_by(practice_id=practice.id).delete()
        OpponentBlueCollarStats.query.filter_by(practice_id=practice.id).delete()

        possession_ids = [
            p.id for p in Possession.query.filter_by(practice_id=practice.id).all()
        ]
        if possession_ids:
            PlayerPossession.query.filter(
                PlayerPossession.possession_id.in_(possession_ids)
            ).delete(synchronize_session=False)
        Possession.query.filter_by(practice_id=practice.id).delete()
        db.session.commit()

        result = admin_routes.parse_practice_csv(
            filepath,
            season_id=file.season_id,
            category=normalized_category,
            file_date=file.file_date,
        )

    if result is None:
        return None

    raw_lineups = _format_lineup_efficiencies(result.get("lineup_efficiencies", {}))
    file.lineup_efficiencies = json.dumps(raw_lineups)

    if file.category == "Game":
        offensive_payload = {
            "possession_type": result.get("offensive_breakdown", {}),
            "periodic": result.get("periodic_offense", {}),
            "shot_clock": result.get("shot_clock_offense", {}),
            "possession_start": result.get("possession_start_offense", {}),
            "paint_touches": result.get("paint_touches_offense", {}),
            "shot_clock_pt": result.get("shot_clock_pt_offense", {}),
        }
        defensive_payload = {
            "possession_type": result.get("defensive_breakdown", {}),
            "periodic": result.get("periodic_defense", {}),
            "shot_clock": result.get("shot_clock_defense", {}),
            "possession_start": result.get("possession_start_defense", {}),
            "paint_touches": result.get("paint_touches_defense", {}),
            "shot_clock_pt": result.get("shot_clock_pt_defense", {}),
        }
    else:
        offensive_payload = result.get("offensive_breakdown")
        defensive_payload = result.get("defensive_breakdown")

        if any(key in result for key in [
            "periodic_offense",
            "shot_clock_offense",
            "possession_start_offense",
            "paint_touches_offense",
            "shot_clock_pt_offense",
        ]):
            offensive_payload = {
                "possession_type": result.get("offensive_breakdown", {}),
                "periodic": result.get("periodic_offense", {}),
                "shot_clock": result.get("shot_clock_offense", {}),
                "possession_start": result.get("possession_start_offense", {}),
                "paint_touches": result.get("paint_touches_offense", {}),
                "shot_clock_pt": result.get("shot_clock_pt_offense", {}),
            }

        if any(key in result for key in [
            "periodic_defense",
            "shot_clock_defense",
            "possession_start_defense",
            "paint_touches_defense",
            "shot_clock_pt_defense",
        ]):
            defensive_payload = {
                "possession_type": result.get("defensive_breakdown", {}),
                "periodic": result.get("periodic_defense", {}),
                "shot_clock": result.get("shot_clock_defense", {}),
                "possession_start": result.get("possession_start_defense", {}),
                "paint_touches": result.get("paint_touches_defense", {}),
                "shot_clock_pt": result.get("shot_clock_pt_defense", {}),
            }

    file.offensive_breakdown = json.dumps(offensive_payload or {})
    file.defensive_breakdown = json.dumps(defensive_payload or {})

    file.parse_status = "Parsed Successfully"
    file.last_parsed = datetime.utcnow()
    db.session.commit()

    return result
