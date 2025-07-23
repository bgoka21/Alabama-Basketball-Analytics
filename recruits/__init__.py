from flask import Blueprint

recruits_bp = Blueprint('recruits', __name__, template_folder='templates')

from . import routes  # noqa: E402,F401
