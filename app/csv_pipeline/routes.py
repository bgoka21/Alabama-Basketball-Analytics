"""Routes for the CSV Pipeline management tool."""

from datetime import date, datetime
from io import BytesIO, StringIO
import os
import pandas as pd
from flask import (
    Blueprint,
    current_app,
    flash,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import login_required
from werkzeug.utils import secure_filename

from utils.auth import admin_required
from app.csv_pipeline.service import (
    CsvPipelineError,
    GroupFilenames,
    GroupInputs,
    build_final_csv,
    _ensure_row_column,
)
from models.database import Game, Season, db
from models.uploaded_file import UploadedFile
from scripts.export_xml import export_csv_to_sportscode_xml
from services.reports.playcall import invalidate_playcall_report

csv_pipeline_bp = Blueprint("csv_pipeline", __name__)

PLAYCALL_COLUMNS = ["PLAYCALL", "POSITION", "SERIES", "VS", "ACTION"]


def _read_csv(file_storage, label: str) -> pd.DataFrame:
    if file_storage is None or not file_storage.filename:
        raise CsvPipelineError(f"Missing required file: {label} is required.")
    return pd.read_csv(file_storage)


def _final_filename(pre_combined_name: str) -> str:
    base = secure_filename(pre_combined_name) or "pre_combined.csv"
    stem, ext = os.path.splitext(base)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    suffix = ext if ext else ".csv"
    return f"final_{stem}_{timestamp}{suffix}"


def _download_filename(game: Game | None) -> str:
    if not game:
        return "final.csv"
    opponent = (game.opponent_name or "game").strip().replace(" ", "_")
    game_date = game.game_date.isoformat() if game.game_date else "game"
    return f"{game_date}_{opponent}_FINAL.csv"


def _xml_download_filename(upload_filename: str | None, game_date: str | None = None) -> str:
    if game_date:
        return f"{game_date}_FINAL.xml"
    if not upload_filename:
        game_date = date.today().isoformat()
        return f"{game_date}_game_FINAL.xml"
    base = secure_filename(upload_filename)
    stem, ext = os.path.splitext(base)
    if ext.lower() == ".csv":
        return f"{stem}.xml"
    return f"{stem}_FINAL.xml"


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
            "Playcall CSV row count does not match number of Offense rows"
        )

    overlay_columns = [col for col in PLAYCALL_COLUMNS if col in playcall_df.columns]
    if not overlay_columns:
        raise CsvPipelineError(
            "Playcall CSV must include at least one of: PLAYCALL, POSITION, SERIES, VS, ACTION."
        )

    overlay_values = playcall_df[overlay_columns].reset_index(drop=True)
    for col in overlay_columns:
        if col not in game_df.columns:
            game_df[col] = ""
        game_df.loc[offense_mask, col] = overlay_values[col].values

    game_df.to_csv(csv_path, index=False)
    invalidate_playcall_report(game.id)


@csv_pipeline_bp.route("/csv-pipeline/playcall-overlay", methods=["POST"])
@login_required
@admin_required
def playcall_overlay():
    seasons, games = _load_form_context()
    errors: list[str] = []

    try:
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


@csv_pipeline_bp.route("/export-xml", methods=["POST"])
@login_required
@admin_required
def export_xml():
    errors: list[str] = []
    try:
        csv_content: str | None = None
        upload_filename: str | None = None
        final_csv_path = request.form.get("final_csv_path")
        final_csv_content = request.form.get("final_csv_content")
        game_date = request.form.get("game_date")

        if final_csv_path:
            upload_folder = current_app.config.get("UPLOAD_FOLDER")
            if not upload_folder:
                raise CsvPipelineError("Upload folder is not configured.")
            abs_folder = os.path.abspath(upload_folder)
            abs_path = os.path.abspath(final_csv_path)
            if not abs_path.startswith(f"{abs_folder}{os.sep}"):
                raise CsvPipelineError("Invalid Final CSV path provided.")
            if not os.path.exists(abs_path):
                raise CsvPipelineError("Final CSV file not found on disk.")
            with open(abs_path, "rb") as csv_file:
                csv_content = csv_file.read().decode("utf-8")
            upload_filename = os.path.basename(abs_path)
        elif final_csv_content:
            csv_content = final_csv_content
        else:
            final_csv_file = request.files.get("final_csv")
            if final_csv_file is None or not final_csv_file.filename:
                raise CsvPipelineError("Final CSV is required.")
            upload_filename = final_csv_file.filename
            csv_content = final_csv_file.read().decode("utf-8")

        final_df = pd.read_csv(StringIO(csv_content))
        _ensure_row_column(final_df, upload_filename or "Final CSV")

        xml_io = BytesIO()
        export_csv_to_sportscode_xml(StringIO(csv_content), xml_io)
        xml_io.seek(0)

        return send_file(
            xml_io,
            mimetype="application/xml",
            download_name=_xml_download_filename(upload_filename, game_date),
            as_attachment=True,
        )
    except CsvPipelineError as exc:
        errors.append(str(exc))
    except pd.errors.ParserError as exc:
        errors.append(f"CSV parse error: {exc}")
    except (ValueError, OSError) as exc:
        errors.append(str(exc))

    for error in errors:
        flash(error, "error")
    seasons, games = _load_form_context()
    return (
        render_template(
            "csv_pipeline/index.html",
            errors=errors,
            seasons=seasons,
            games=games,
        ),
        400,
    )


@csv_pipeline_bp.route("/csv-pipeline", methods=["GET", "POST"])
@login_required
@admin_required
def csv_pipeline_index():
    seasons, games = _load_form_context()

    if request.method == "POST":
        errors: list[str] = []
        try:
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
                    home_or_away=None,
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

            return render_template(
                "csv_pipeline/final_csv.html",
                game=game,
                download_name=_download_filename(game),
                download_url=url_for(
                    "csv_pipeline.download_final_csv",
                    game_id=game.id,
                ),
                final_csv_path=file_path,
                game_date=game.game_date.isoformat() if game.game_date else "",
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


@csv_pipeline_bp.route("/csv-pipeline/download-final/<int:game_id>", methods=["GET"])
@login_required
@admin_required
def download_final_csv(game_id: int):
    game = Game.query.get(game_id)
    if not game or not game.csv_filename:
        flash("Final CSV not found for the selected game.", "error")
        seasons, games = _load_form_context()
        return (
            render_template(
                "csv_pipeline/index.html",
                errors=["Final CSV not found for the selected game."],
                seasons=seasons,
                games=games,
            ),
            404,
        )

    upload_folder = current_app.config.get("UPLOAD_FOLDER")
    if not upload_folder:
        raise CsvPipelineError("Upload folder is not configured.")
    csv_path = os.path.join(upload_folder, game.csv_filename)
    if not os.path.exists(csv_path):
        flash("Final CSV file not found on disk.", "error")
        seasons, games = _load_form_context()
        return (
            render_template(
                "csv_pipeline/index.html",
                errors=["Final CSV file not found on disk."],
                seasons=seasons,
                games=games,
            ),
            404,
        )

    return send_file(
        csv_path,
        mimetype="text/csv",
        download_name=_download_filename(game),
        as_attachment=True,
    )
