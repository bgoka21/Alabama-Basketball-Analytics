from flask import Blueprint

recruits_bp = Blueprint('recruits', __name__, template_folder='templates')

# Import routes only once; if the blueprint has already been registered, the
# routes are presumed to be in place.
if not getattr(recruits_bp, '_got_registered_once', False):
    from . import routes  # noqa: E402,F401
