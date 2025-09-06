import os, time
from flask import Blueprint, request, redirect, url_for, abort, render_template, flash
from werkzeug.utils import secure_filename
from flask_login import login_required, current_user
from app import db
from app.models import Coach  # adjust import path

bp_logo = Blueprint("coach_logo", __name__)

ALLOWED = {"png","jpg","jpeg","svg"}
UPLOAD_DIR = os.path.join("static","uploads","logos")

def _allowed(fname): return "." in fname and fname.rsplit(".",1)[1].lower() in ALLOWED

@bp_logo.route("/admin/coach/<int:coach_id>/logo", methods=["GET","POST"])
@login_required
def upload_logo(coach_id):
    if not getattr(current_user, "is_admin", False):
        abort(403)
    coach = Coach.query.get_or_404(coach_id)
    if request.method == "POST":
        file = request.files.get("logo")
        if not file or file.filename == "":
            flash("Choose a file.", "warning")
            return redirect(request.url)
        if not _allowed(file.filename):
            flash("Allowed types: png, jpg, jpeg, svg", "danger")
            return redirect(request.url)

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        ext = file.filename.rsplit(".",1)[1].lower()
        fname = f"coach_{coach.id}.{ext}"
        path = os.path.join(UPLOAD_DIR, secure_filename(fname))
        file.save(path)

        # store as web path + cache bust
        rel = "/" + path.replace("\\","/")
        coach.team_logo_url = f"{rel}?v={int(time.time())}"
        db.session.commit()
        flash("Logo updated.", "success")
        return redirect(url_for("recruits.money_coach", coach_name=coach.name))
    return render_template("admin/coach_logo_upload.html", coach=coach)
