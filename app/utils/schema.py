from sqlalchemy import inspect, text


def ensure_columns(engine, table_name, cols):
    """
    cols: list of tuples (name, sql_type_string), e.g. [("projected_pick","REAL")]
    Creates columns if missing. No-op if present.
    """
    insp = inspect(engine)
    existing = {c["name"] for c in insp.get_columns(table_name)}
    dialect = engine.dialect.name
    # map generic to dialect when needed
    def dtype(sql):
        if dialect in ("sqlite", "mysql"):
            return sql  # REAL/DOUBLE ok
        if dialect == "postgresql":
            return sql.replace("REAL", "DOUBLE PRECISION").replace("DOUBLE", "DOUBLE PRECISION")
        return sql
    for name, sqltype in cols:
        if name not in existing:
            engine.execute(
                text(f'ALTER TABLE "{table_name}" ADD COLUMN "{name}" {dtype(sqltype)}')
            )
