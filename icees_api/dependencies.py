from .db import DBConnection


async def get_db():
    """Get database connection."""
    with DBConnection() as conn:
        yield conn
