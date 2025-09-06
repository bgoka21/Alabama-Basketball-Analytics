from flask import Blueprint

recruits_bp = Blueprint('recruits', __name__, template_folder='templates')


def display_pick(value):
    try:
        num, text = value
    except Exception:
        num, text = value, None
    if text and str(text).strip():
        return str(text).strip()
    if num is not None:
        try:
            return str(int(num))
        except Exception:
            return str(num)
    return ""


recruits_bp.add_app_template_filter(display_pick, "display_pick")

# Import routes only once; if the blueprint has already been registered, the
# routes are presumed to be in place.
if not getattr(recruits_bp, '_got_registered_once', False):
    from . import routes  # noqa: E402,F401
