"""Routes for the CSV Pipeline management tool."""

from flask import Blueprint, render_template
from flask_login import login_required

from utils.auth import admin_required

csv_pipeline_bp = Blueprint(
    "csv_pipeline",
    __name__,
    template_folder="../../templates/csv_pipeline",
)


@csv_pipeline_bp.route("/csv-pipeline", methods=["GET"])
@login_required
@admin_required
def csv_pipeline_index():
    return render_template("csv_pipeline/index.html")
