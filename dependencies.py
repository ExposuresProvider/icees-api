import db

async def get_db():
    """Get database connection."""
    with db.DBConnection() as conn:
        yield conn
