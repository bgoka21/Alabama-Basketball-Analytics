"""Routes for the CSV Pipeline management tool."""

from datetime import date, datetime
from io import BytesIO
import os

import pandas as pd
from flask import (
    Blueprint,
    current_app,
    flash,
    render_template,
    request,
    send_file,
)
from flask_login import login_required
from werkzeug.utils import secure_filename

from utils.auth import admin_required
from app.csv_pipeline.service import (
    CsvPipelineError,
    GroupFilenames,
    GroupInputs,
    build_final_csv,
)
from models.database import Game, Season, db
from models.uploaded_file import UploadedFile
from services.reports.playcall import invalidate_playcall_report

csv_pipeline_bp = Blueprint("csv_pipeline", __name__)

PLAYCALL_COLUMNS = ["PLAYCALL", "POSITION", "SERIES", "VS", "ACTION"]


def _read_csv(file_storage, label: str) -> pd.DataFrame:
    if file_storage is None or not file_storage.filename:
        raise CsvPipelineError(f"Missing required file: {label}.")
    return pd.read_csv(file_storage)


def _final_filename(pre_combined_name: str) -> str:
    base = secure_filename(pre_combined_name) or "pre_combined.csv"
    stem, ext = os.path.splitext(base)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    suffix = ext if ext else ".csv"
    return f"final_{stem}_{timestamp}{suffix}"


def _load_form_context():
    seasons = Season.query.order_by(Season.start_date.desc(), Season.id.desc()).all()
    games = Game.query.order_by(Game.game_date.desc(), Game.id.desc()).all()
    return seasons, games


def _apply_playcall_overlay(game: Game, playcall_df: pd.DataFrame) -> None:
    upload_folder = current_app.config.get("UPLOAD_FOLDER")
    if not upload_folder:
        raise CsvPipelineError("Upload folder is not configured.")
    if not game.csv_filename:
        raise CsvPipelineError("Selected game has no CSV file attached.")

    csv_path = os.path.join(upload_folder, game.csv_filename)
    if not os.path.exists(csv_path):
        raise CsvPipelineError("Stored game CSV file not found on disk.")

    game_df = pd.read_csv(csv_path)
    if "Row" not in game_df.columns:
        raise CsvPipelineError("Stored game CSV is missing required 'Row' column.")

    offense_mask = game_df["Row"] == "Offense"
    offense_count = int(offense_mask.sum())
    if offense_count != len(playcall_df):
        raise CsvPipelineError(
            f"Playcall row count mismatch: {len(playcall_df)} rows provided, "
            f"expected {offense_count} offense rows."
        )

    for col in PLAYCALL_COLUMNS:
        if col not in playcall_df.columns:
            playcall_df[col] = ""
        if col not in game_df.columns:
            game_df[col] = ""

    overlay_values = playcall_df[PLAYCALL_COLUMNS].reset_index(drop=True)
    for col in PLAYCALL_COLUMNS:
        game_df.loc[offense_mask, col] = overlay_values[col].values

    game_df.to_csv(csv_path, index=False)
    invalidate_playcall_report(game.id)


@csv_pipeline_bp.route("/csv-pipeline", methods=["GET", "POST"])
@login_required
@admin_required
def csv_pipeline_index():
    seasons, games = _load_form_context()

    if request.method == "POST":
        errors: list[str] = []
        action = (request.form.get("action") or "build").lower()

        try:
            if action == "overlay":
                game_id = request.form.get("overlay_game_id", type=int)
                playcall_file = request.files.get("playcall_overlay")
                if not game_id:
                    raise CsvPipelineError("Select a game to apply the playcall overlay.")
                if playcall_file is None or not playcall_file.filename:
                    raise CsvPipelineError("Playcall CSV is required for the overlay.")

                game = Game.query.get(game_id)
                if not game:
                    raise CsvPipelineError("Selected game was not found.")

                playcall_df = pd.read_csv(playcall_file)
                _apply_playcall_overlay(game, playcall_df)

                flash("Playcall overlay applied successfully.", "success")
                return render_template(
                    "csv_pipeline/index.html",
                    errors=[],
                    seasons=seasons,
                    games=games,
                )

            pre_combined_file = request.files.get("pre_combined")
            pre_combined = _read_csv(pre_combined_file, "Pre-Combined CSV")

            inputs = GroupInputs(
                offense_shot_type=_read_csv(
                    request.files.get("offense_shot_type"),
                    "Offense Shot Type",
                ),
                offense_shot_creation=_read_csv(
                    request.files.get("offense_shot_creation"),
                    "Offense Shot Creation",
                ),
                offense_turnover_type=_read_csv(
                    request.files.get("offense_turnover_type"),
                    "Offense Turnover Type",
                ),
                defense_possessions=_read_csv(
                    request.files.get("defense_possessions"),
                    "Defense Possessions",
                ),
                defense_gap_help=_read_csv(
                    request.files.get("defense_gap_help"),
                    "Defense Gap Help",
                ),
                defense_shot_contest=_read_csv(
                    request.files.get("defense_shot_contest"),
                    "Defense Shot Contest",
                ),
                defense_pass_contest=_read_csv(
                    request.files.get("defense_pass_contest"),
                    "Defense Pass Contest",
                ),
                pnr_gap_help=_read_csv(
                    request.files.get("pnr_gap_help"),
                    "PnR Gap Help",
                ),
                pnr_grade=_read_csv(
                    request.files.get("pnr_grade"),
                    "PnR Grade",
                ),
                offense_rebound=_read_csv(
                    request.files.get("offense_rebound"),
                    "Offense Rebound Opportunities",
                ),
                defense_rebound=_read_csv(
                    request.files.get("defense_rebound"),
                    "Defense Rebound Opportunities",
                ),
            )

            filenames = GroupFilenames(
                offense_shot_type=request.files.get("offense_shot_type").filename,
                offense_shot_creation=request.files.get("offense_shot_creation").filename,
                offense_turnover_type=request.files.get("offense_turnover_type").filename,
                defense_possessions=request.files.get("defense_possessions").filename,
                defense_gap_help=request.files.get("defense_gap_help").filename,
                defense_shot_contest=request.files.get("defense_shot_contest").filename,
                defense_pass_contest=request.files.get("defense_pass_contest").filename,
                pnr_gap_help=request.files.get("pnr_gap_help").filename,
                pnr_grade=request.files.get("pnr_grade").filename,
                offense_rebound=request.files.get("offense_rebound").filename,
                defense_rebound=request.files.get("defense_rebound").filename,
            )

            game_id = request.form.get("game_id", type=int)
            season_id = request.form.get("season_id", type=int)
            opponent_name = (request.form.get("opponent_name") or "").strip()
            home_or_away = (request.form.get("home_or_away") or "").strip()
            game_date_str = (request.form.get("game_date") or "").strip()

            game = None
            if game_id:
                game = Game.query.get(game_id)
                if not game:
                    raise CsvPipelineError("Selected game was not found.")
                if season_id is None:
                    season_id = game.season_id
            else:
                if not season_id:
                    raise CsvPipelineError("Season is required to create a game record.")
                if not opponent_name:
                    raise CsvPipelineError("Opponent name is required to create a game record.")
                if not home_or_away:
                    raise CsvPipelineError("Home/Away selection is required.")
                if not game_date_str:
                    raise CsvPipelineError("Game date is required.")

                try:
                    parsed_date = datetime.strptime(game_date_str, "%Y-%m-%d").date()
                except ValueError as exc:
                    raise CsvPipelineError("Game date must be in YYYY-MM-DD format.") from exc

                game = Game(
                    season_id=season_id,
                    game_date=parsed_date,
                    opponent_name=opponent_name,
                    home_or_away=home_or_away,
                    result="N/A",
                )
                db.session.add(game)
                db.session.flush()

            final_df = build_final_csv(pre_combined, inputs, filenames)

            upload_folder = current_app.config.get("UPLOAD_FOLDER")
            if not upload_folder:
                raise CsvPipelineError("Upload folder is not configured.")

            os.makedirs(upload_folder, exist_ok=True)
            filename = _final_filename(pre_combined_file.filename)
            file_path = os.path.join(upload_folder, filename)

            final_df.to_csv(file_path, index=False)

            game.csv_filename = filename
            if not game.game_date:
                game.game_date = date.today()
            db.session.commit()

            uploaded_file = UploadedFile(
                filename=filename,
                parse_status="Not Parsed",
                category="Game",
                season_id=game.season_id,
                file_date=game.game_date,
            )
            db.session.add(uploaded_file)
            db.session.commit()

            csv_io = BytesIO()
            final_df.to_csv(csv_io, index=False)
            csv_io.seek(0)
            return send_file(
                csv_io,
                mimetype="text/csv",
                download_name=filename,
                as_attachment=True,
            )
        except CsvPipelineError as exc:
            errors.append(str(exc))
        except pd.errors.ParserError as exc:
            errors.append(f"CSV parse error: {exc}")

        for error in errors:
            flash(error, "error")
        return (
            render_template(
                "csv_pipeline/index.html",
                errors=errors,
                seasons=seasons,
                games=games,
            ),
            400,
        )

    return render_template(
        "csv_pipeline/index.html",
        errors=[],
        seasons=seasons,
        games=games,
    )
