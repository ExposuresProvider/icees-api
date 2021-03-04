"""Testing utilities."""
import csv
from functools import partial, wraps
import io
import os
from pathlib import Path
import re
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base

from icees_api.app import APP
from icees_api.dependencies import get_db, ConnectionWithTables

testclient = TestClient(APP)


def generate_kgraph(shorthand):
    """Generate TRAPI-style knowledge graph.

    Example:
    PUBCHEM:2083 (( category biolink:Drug ))
    MESH:D052638 (( category biolink:ChemicalSubstance ))
    PUBCHEM:2083 -- predicate biolink:association --> MESH:D052638
    """
    node_pattern = r"(.*?) \(\( category (.*?) \)\)"
    edge_pattern = r"(.*?) -- predicate (.*?) --> (.*)"
    kgraph = {
        "nodes": dict(),
        "edges": dict(),
    }
    for line in shorthand.split("\n"):
        line = line.strip()
        node_match = re.fullmatch(node_pattern, line)
        if node_match:
            kgraph["nodes"][node_match.group(1)] = {
                "category": node_match.group(2)
            }
            continue
        edge_match = re.fullmatch(edge_pattern, line)
        if edge_match:
            kgraph["edges"][str(uuid4())] = {
                "subject": edge_match.group(1),
                "predicate": edge_match.group(2),
                "object": edge_match.group(3),
            }
            continue
    return kgraph


DATAPATH = Path(os.environ["DATA_PATH"])
db_ = os.environ["ICEES_DB"]


async def get_db_(data: str):
    """Get database connection."""
    engine = create_engine(
        f"sqlite://",
        connect_args={"check_same_thread": False},
    )
    conn = engine.connect()

    table_name = "patient"

    # remove extraneous whitespace
    data = "\n".join(line.strip() for line in data.split("\n") if line.strip())

    # get data
    with io.StringIO(data) as stream:
        reader = csv.DictReader(stream)
        columns = next(reader)
        to_db = [
            tuple(row.get(col) for col in columns)
            for row in reader
        ]

    # create table
    column_spec = ", ".join([
        f"\"{col_name}\" {type_}"
        for col_name, type_ in columns.items()
    ])
    query = f"CREATE TABLE {table_name} ({column_spec});"
    conn.execute(query)

    # add data
    if db_ == "sqlite":
        placeholders = ", ".join("?" for _ in columns)
    else:
        placeholders = ", ".join("%s" for _ in columns)
    query = "INSERT INTO {0} ({1}) VALUES ({2});".format(
        table_name,
        ", ".join(f"\"{col}\"" for col in columns),
        placeholders,
    )
    conn.execute(query, to_db)

    # create cohort table
    query = "CREATE TABLE cohort (" + (
        "cohort_id varchar(255), "
        "size int, "
        "\"table\" varchar(255), "
        "year int, "
        "features varchar(255)"
    ) + ");"
    conn.execute(query)

    # get tables
    Base = automap_base()
    Base.prepare(conn.engine, reflect=True)  # reflect the tables
    tables = Base.metadata.tables

    try:
        yield ConnectionWithTables(conn, tables)
    finally:
        conn.close()


def load_data(app, data):
    """Create decorator loading data into ICEES db."""
    def decorator(fcn):
        @wraps(fcn)
        def wrapper(*args, **kwargs):
            app.dependency_overrides[get_db] = partial(get_db_, data)
            fcn(*args, **kwargs)
            app.dependency_overrides = {}
        return wrapper
    return decorator


@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure
    varchar(255),int,varchar(255),varchar(255),int
    1,2010,0-2,0,1
    2,2010,0-2,1,2
    3,2010,0-2,>1,3
""")
def test_knowledge_graph_overlay_year_table_features():
    """Test knowledge graph overlay with year, table, and features."""
    payload = {
        "query_options": {
            "table": "patient",
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        },
        "message": {
            "knowledge_graph": generate_kgraph("""
                PUBCHEM:2083 (( category biolink:Drug ))
                MESH:D052638 (( category biolink:ChemicalSubstance ))
                PUBCHEM:2083 -- predicate biolink:association --> MESH:D052638
            """)
        }
    }
    resp = testclient.post(
        "/knowledge_graph_overlay",
        json=payload,
    )
    resp_json = resp.json()
    print(resp_json)
