from functools import wraps
from flask import abort
from flask_login import current_user, login_required

# Endpoints a logged-in player is allowed to access without being redirected.
PLAYER_ALLOWED_ENDPOINTS = {
    'public.practice_homepage',
    'public.game_homepage',
    'public.homepage',
    'public.skill_dev',
    'admin.player_detail',
    'admin.logout',
    'admin.edit_skill_entry',
    'admin.delete_skill_entry',
    'player_view',
}

def admin_required(f):
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if not current_user.is_admin:
            abort(403)
        return f(*args, **kwargs)
    return decorated
