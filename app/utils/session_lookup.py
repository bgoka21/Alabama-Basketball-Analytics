from datetime import date
from typing import Optional, Tuple

from sqlalchemy.orm import Session as SASession

from models.database import Session as DBSession


def get_session_window(
    db_session: SASession,
    season_id: int,
    session_name: str,
) -> Tuple[Optional[date], Optional[date]]:
    """Return the start and end date for ``session_name`` within ``season_id``.

    The lookup uses the :class:`models.database.Session` table so callers can
    share the same session windows used by Compare Sessions. If the session is
    not present, ``(None, None)`` is returned so the caller may skip applying a
    date filter.
    """

    if not season_id or not session_name:
        return (None, None)

    row = (
        db_session.query(DBSession)
        .filter(DBSession.season_id == season_id, DBSession.name == session_name)
        .one_or_none()
    )
    if not row:
        return (None, None)

    return (row.start_date, row.end_date)
