from flask import Blueprint

scout_bp = Blueprint('scout', __name__, template_folder='../templates')

# Import routes so they are registered with the blueprint
if not getattr(scout_bp, '_got_registered_once', False):
    from . import routes  # noqa: E402,F401
