from typing import Set

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError, OperationalError

_verified_engines: set[str] = set()


def _table_columns(engine: Engine, table_name: str) -> Set[str]:
    try:
        inspector = inspect(engine)
        return {column["name"] for column in inspector.get_columns(table_name)}
    except NoSuchTableError:
        return set()


def ensure_scout_possession_schema(engine: Engine) -> Set[str]:
    """Ensure the scout_possessions table has family and series columns.

    If the columns are missing (for example when migrations have not been run), they
    are added using ALTER TABLE statements. The function returns the discovered
    column names after any alterations.
    """

    engine_key = str(engine.url)
    if engine_key in _verified_engines:
        return _table_columns(engine, "scout_possessions")

    existing_columns = _table_columns(engine, "scout_possessions")
    missing_statements: list[str] = []

    if "family" not in existing_columns:
        missing_statements.append(
            "ALTER TABLE scout_possessions ADD COLUMN family VARCHAR(255)"
        )
    if "series" not in existing_columns:
        missing_statements.append(
            "ALTER TABLE scout_possessions ADD COLUMN series VARCHAR(255)"
        )

    if missing_statements:
        try:
            with engine.begin() as connection:
                for statement in missing_statements:
                    connection.execute(text(statement))
            # Refresh column list after applying alterations.
            existing_columns = _table_columns(engine, "scout_possessions")
        except OperationalError:
            # If the underlying database cannot alter the table (or it does not
            # exist), leave the existing columns as-is. Queries will continue to
            # use whatever is available.
            return existing_columns

    _verified_engines.add(engine_key)
    return existing_columns
