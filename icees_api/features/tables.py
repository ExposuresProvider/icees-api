"""Get SQL tables."""
from sqlalchemy.engine import Connection
from sqlalchemy.ext.automap import automap_base

from ..db import get_db_connection

Base = automap_base()

engine = get_db_connection()

# reflect the tables
Base.prepare(engine, reflect=True)

tables = Base.metadata.tables


def table_id(table):
    """Generate table id."""
    return table[0].upper() + table[1:] + "Id"


name_table = tables["name"]
cache = tables["cache"]
cohort = tables["cohort"]
