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
from flask_login import login_required
from utils.auth import admin_required

# Blueprint replacing the standalone Flask app
merge_bp = Blueprint("merge", __name__, template_folder="templates")

MERGE_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "merge_config.json")


def load_merge_config():
    """Load merge configuration from ``merge_config.json``."""
    default = {"files": {}, "on": [], "how": "inner"}
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
@login_required
@admin_required
def merge_csv():
    """Merge uploaded CSV files according to the saved settings."""
    if request.method == "POST":
        files = request.files.getlist("files")
        if len(files) < 2:
            flash("Please upload at least two CSV files", "error")
            return render_template("merge_csv.html")

        config = load_merge_config()
        on_columns = config.get("on") or []
        how = config.get("how", "inner")
        file_cols = config.get("files", {})

        try:
            dataframes = []
            for idx, f in enumerate(files, 1):
                df = pd.read_csv(f)
                cols = file_cols.get(f"file{idx}")
                if cols:
                    cols_existing = [c for c in cols if c in df.columns]
                    df = df[cols_existing]
                dataframes.append(df)

            merged = dataframes[0]
            for df in dataframes[1:]:
                if on_columns:
                    merged = pd.merge(merged, df, on=on_columns, how=how)
                else:
                    merged = pd.concat([merged, df], axis=0, ignore_index=True)

            csv_io = BytesIO()
            merged.to_csv(csv_io, index=False, float_format='%.15g')
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
@login_required
@admin_required
def merge_settings():
    """View or update merge configuration."""
    config = load_merge_config()
    if request.method == "POST":
        keys = request.form.get("merge_keys", "")
        how = request.form.get("how", "inner")

        config["on"] = [k.strip() for k in keys.split(",") if k.strip()]
        config["how"] = how

        files_cfg = {}
        for key, value in request.form.items():
            if key.startswith("cols_file"):
                idx = key.replace("cols_file", "")
                cols = [c.strip() for c in value.split(",") if c.strip()]
                if cols:
                    files_cfg[f"file{idx}"] = cols
        config["files"] = files_cfg

        save_merge_config(config)
        flash("Settings updated", "success")

    file_columns = config.get("files", {})
    max_index = max([int(k.replace("file", "")) for k in file_columns.keys()] or [2])
    file_fields = {
        i: ", ".join(file_columns.get(f"file{i}", [])) for i in range(1, max_index + 1)
    }

    merge_keys = ", ".join(config.get("on", []))
    return render_template(
        "merge_settings.html",
        merge_keys=merge_keys,
        how=config.get("how", "inner"),
        file_fields=file_fields,
    )
