"""FastAPI dependencies."""
from sqlalchemy.ext.automap import automap_base

from .db import DBConnection, Connection


class ConnectionWithTables():
    """Connection with tables."""

    def __init__(self, connection, tables):
        """Initialize."""
        self.connection: Connection = connection
        self.tables = tables

    def execute(self, *args, **kwargs):
        """Execute query."""
        return self.connection.execute(*args, **kwargs)


async def get_db() -> ConnectionWithTables:
    """Get database connection."""
    with DBConnection() as conn:
        Base = automap_base()
        Base.prepare(conn.engine, reflect=True)  # reflect the tables
        tables = Base.metadata.tables
        yield ConnectionWithTables(conn, tables)
