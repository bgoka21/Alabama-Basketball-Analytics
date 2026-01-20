"""Routes for the CSV Pipeline management tool."""

from io import BytesIO

import pandas as pd
from flask import Blueprint, flash, render_template, request, send_file
from flask_login import login_required

from utils.auth import admin_required
from app.csv_pipeline.service import (
    CsvPipelineError,
    GroupFilenames,
    GroupInputs,
    build_final_csv,
)

csv_pipeline_bp = Blueprint("csv_pipeline", __name__)


def _read_csv(file_storage, label: str) -> pd.DataFrame:
    if file_storage is None or not file_storage.filename:
        raise CsvPipelineError(f"Missing required file: {label}.")
    return pd.read_csv(file_storage)


@csv_pipeline_bp.route("/csv-pipeline", methods=["GET", "POST"])
@login_required
@admin_required
def csv_pipeline_index():
    if request.method == "POST":
        errors: list[str] = []

        try:
            pre_combined = _read_csv(
                request.files.get("pre_combined"),
                "Pre-Combined CSV",
            )

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

            final_df = build_final_csv(pre_combined, inputs, filenames)

            csv_io = BytesIO()
            final_df.to_csv(csv_io, index=False)
            csv_io.seek(0)
            return send_file(
                csv_io,
                mimetype="text/csv",
                download_name="final.csv",
                as_attachment=True,
            )
        except CsvPipelineError as exc:
            errors.append(str(exc))
        except pd.errors.ParserError as exc:
            errors.append(f"CSV parse error: {exc}")

        for error in errors:
            flash(error, "error")
        return render_template("csv_pipeline/index.html", errors=errors), 400

    return render_template("csv_pipeline/index.html", errors=[])
