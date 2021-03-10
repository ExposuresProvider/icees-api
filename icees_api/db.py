"""Database tools."""
from contextlib import contextmanager
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

db = os.environ.get("ICEES_DB", "sqlite")
serv_host = os.environ["ICEES_HOST"]
serv_port = os.environ["ICEES_PORT"]

engine = None

DB_PATH = Path(os.environ["DB_PATH"])


def get_db_connection():
    """Get database connection."""
    global engine
    if engine is None:
        if db == "sqlite":
            engine = create_engine(
                f"sqlite:///{DB_PATH / 'example.db'}?check_same_thread=False",
            )
        elif db == "postgres":
            engine = create_engine(
                f"postgresql+psycopg2://icees_dbuser:icees_dbpass@{serv_host}:{serv_port}/icees_database",
                pool_size=int(os.environ["POOL_SIZE"]),
                max_overflow=int(os.environ["MAX_OVERFLOW"]),
            )
        else:
            raise ValueError(f"Unsupported database '{db}'")

    return engine


@contextmanager
def DBConnection() -> Connection:
    """Database connection."""
    engine = get_db_connection()
    conn: Connection = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
