from sqlalchemy import func
from flask import current_app
from models.database import db


def array_agg_or_group_concat(column):
    """
    Return the appropriate aggregate function for the current DB dialect.
    Uses db.engine (guaranteed initialized) instead of session.bind.
    """
    try:
        dialect_name = db.engine.dialect.name
    except Exception:
        # Fallback for older Flask-SQLAlchemy versions
        dialect_name = db.get_engine(current_app).dialect.name

    if dialect_name == 'sqlite':
        # SQLite: concat all values with a delimiter, to split in Python later
        return func.group_concat(column, '|||')
    else:
        # Postgres, etc.
        return func.array_agg(column)
