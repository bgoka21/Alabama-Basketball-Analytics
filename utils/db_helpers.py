from sqlalchemy import func
from models.database import db


def array_agg_or_group_concat(column, delimiter=';'):
    """Return an aggregation of values for a column that works in SQLite.

    Uses array_agg when available (e.g., PostgreSQL) and group_concat
    for SQLite databases.
    """
    if db.session.bind.dialect.name == 'sqlite':
        return func.group_concat(column, delimiter)
    return func.array_agg(column)
