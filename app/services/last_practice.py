from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy import desc
from sqlalchemy.orm import Session

from app import db
from models.database import Practice


def get_last_practice(
    season_id: int,
    *,
    session: Optional[Session] = None,
) -> Optional[Practice]:
    """
    Return the most recent Practice for a given season.

    Ordering priority:
      1) Practice.date DESC
      2) Practice.created_at DESC (if available)
      3) Practice.id DESC (safety fallback)

    Returns None if no practice exists for the season.
    """
    s = session or db.session

    practice_date_column = getattr(Practice, "date", None)
    created_at_column = getattr(Practice, "created_at", None)

    query = s.query(Practice).filter(Practice.season_id == season_id)

    if practice_date_column is not None:
        query = query.filter(practice_date_column.isnot(None))

    order_clauses = []
    if practice_date_column is not None:
        order_clauses.append(desc(practice_date_column))
    if created_at_column is not None:
        order_clauses.append(desc(created_at_column))
    order_clauses.append(desc(Practice.id))

    query = query.order_by(*order_clauses)

    return query.first()


def get_last_practice_date(
    season_id: int,
    *,
    session: Optional[Session] = None,
) -> Optional[date]:
    """
    Convenience wrapper that returns the date of the most recent practice
    for the provided season ID, if one exists.
    """
    practice = get_last_practice(season_id, session=session)
    return practice.date if practice else None


def normalize_practice_date(
    practice: Practice,
    *,
    candidate_date: Optional[date] = None,
) -> Practice:
    """
    Ensure a Practice instance has a meaningful ``date`` assigned.

    - If the practice already has a date, leave it unchanged.
    - Else, if ``candidate_date`` is provided, assign it.
    - Otherwise leave the date unset and let the caller decide on defaults.
    """
    if getattr(practice, "date", None) is None and candidate_date is not None:
        practice.date = candidate_date
    return practice
