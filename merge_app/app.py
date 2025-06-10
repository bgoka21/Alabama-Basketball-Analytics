from flask import Blueprint, render_template, request, current_app

# Blueprint replacing the standalone Flask app
merge_bp = Blueprint("merge", __name__, template_folder="templates")

@merge_bp.route("/merge_csv", methods=["GET", "POST"])
def merge_csv():
    """Display the CSV merge tool."""
    if request.method == "POST":
        # Placeholder processing logic
        pass
    return render_template("merge_csv.html")

@merge_bp.route("/merge_settings", methods=["GET", "POST"])
def merge_settings():
    """Page for adjusting merge settings."""
    if request.method == "POST":
        pass
    return render_template("merge_settings.html")
