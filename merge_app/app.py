import json
import os
from io import BytesIO

import pandas as pd
from flask import (
    Blueprint,
    render_template,
    request,
    current_app,
    flash,
    send_file,
)

# Blueprint replacing the standalone Flask app
merge_bp = Blueprint("merge", __name__, template_folder="templates")

MERGE_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "merge_config.json")


def load_merge_config():
    """Load merge configuration from ``merge_config.json``."""
    default = {"on": [], "how": "inner"}
    if os.path.exists(MERGE_CONFIG_PATH):
        try:
            with open(MERGE_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                default.update(data)
        except json.JSONDecodeError:
            current_app.logger.warning("Invalid merge_config.json; using default")
    return default


def save_merge_config(config):
    """Persist merge configuration to ``merge_config.json``."""
    with open(MERGE_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)


@merge_bp.route("/merge_csv", methods=["GET", "POST"])
def merge_csv():
    """Merge two uploaded CSV files according to the saved settings."""
    if request.method == "POST":
        file1 = request.files.get("file1")
        file2 = request.files.get("file2")
        if not file1 or not file2:
            flash("Please upload two CSV files", "error")
            return render_template("merge_csv.html")

        config = load_merge_config()
        on_columns = config.get("on") or []
        how = config.get("how", "inner")

        try:
            df1 = pd.read_csv(file1)
            df2 = pd.read_csv(file2)

            if on_columns:
                merged = pd.merge(df1, df2, on=on_columns, how=how)
            else:
                merged = pd.concat([df1, df2], axis=0, ignore_index=True)

            csv_io = BytesIO()
            merged.to_csv(csv_io, index=False)
            csv_io.seek(0)
            return send_file(
                csv_io,
                mimetype="text/csv",
                download_name="merged.csv",
                as_attachment=True,
            )
        except Exception as exc:  # pragma: no cover - unlikely in tests
            current_app.logger.exception("CSV merge failed")
            flash(f"Merge failed: {exc}", "error")

    return render_template("merge_csv.html")

@merge_bp.route("/merge_settings", methods=["GET", "POST"])
def merge_settings():
    """View or update merge configuration."""
    config = load_merge_config()
    if request.method == "POST":
        keys = request.form.get("merge_keys", "")
        how = request.form.get("how", "inner")

        config["on"] = [k.strip() for k in keys.split(",") if k.strip()]
        config["how"] = how
        save_merge_config(config)
        flash("Settings updated", "success")

    merge_keys = ", ".join(config.get("on", []))
    return render_template(
        "merge_settings.html", merge_keys=merge_keys, how=config.get("how", "inner")
    )
