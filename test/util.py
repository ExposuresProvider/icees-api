"""Testing utilities."""
import csv
from functools import partial, wraps
import io
import os
import re
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.ext.automap import automap_base

from icees_api.dependencies import get_db, ConnectionWithTables

db_ = os.environ.get("ICEES_DB", "sqlite")


def generate_kgraph(shorthand):
    """Generate TRAPI-style knowledge graph.

    Example:
    PUBCHEM:2083 (( category biolink:Drug ))
    MESH:D052638 (( category biolink:ChemicalSubstance ))
    PUBCHEM:2083 -- predicate biolink:association --> MESH:D052638
    """
    node_pattern = r"(.*?) \(\( category (.*) \)\)"
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


async def fill_db(conn: Connection, data: str, cohort_data: str):
    """Fill database with data."""
    table_name = "patient"

    # remove extraneous whitespace
    data = "\n".join(line.strip() for line in data.split("\n") if line.strip())

    # get data
    with io.StringIO(data) as stream:
        reader = csv.DictReader(stream)
        columns = next(reader)  # captures the SQL datatypes
        to_db = [
            tuple(
                value if (value:=row.get(col)) != "" else None
                for col in columns
            )
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

    if cohort_data:
        # remove extraneous whitespace
        cohort_data = "\n".join(
            line.strip()
            for line in cohort_data.split("\n")
            if line.strip()
        )

        # get cohort data
        with io.StringIO(cohort_data) as stream:
            reader = csv.DictReader(stream, escapechar="\\")
            rows = list(reader)
            columns = list(rows[0].keys())
            to_db = [
                tuple(row.values())
                for row in rows
            ]

        # add cohort data
        if db_ == "sqlite":
            placeholders = ", ".join("?" for _ in columns)
        else:
            placeholders = ", ".join("%s" for _ in columns)
        query = "INSERT INTO {0} ({1}) VALUES ({2});".format(
            "cohort",
            ", ".join(f"\"{col}\"" for col in columns),
            placeholders,
        )
        conn.execute(query, to_db)


async def get_db_(data: str, cohort_data: str):
    """Get database connection."""
    engine = create_engine(
        f"sqlite://",
        connect_args={"check_same_thread": False},
    )
    conn = engine.connect()

    await fill_db(conn, data, cohort_data)

    # get tables
    Base = automap_base()
    Base.prepare(conn.engine, reflect=True)  # reflect the tables
    tables = Base.metadata.tables

    try:
        yield ConnectionWithTables(conn, tables)
    finally:
        conn.close()


def escape_quotes(string: str) -> str:
    return string.replace("\"", "\\\"")


def load_data(app, data, cohort_data=""):
    """Create decorator loading data into ICEES db."""
    def decorator(fcn):
        @wraps(fcn)
        def wrapper(*args, **kwargs):
            app.dependency_overrides[get_db] = partial(get_db_, data, cohort_data)
            fcn(*args, **kwargs)
            app.dependency_overrides = {}
        return wrapper
    return decorator


def query(year, biolink_class):
    return {
        "query_options": {
            "table": "patient",
            "year": year,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            },
            "feature": {
                "EstResidentialDensity": {
                    "operator": "<",
                    "value": 1
                }
            },
            "maximum_p_value": 1.1
        },
        "message": {
            "query_graph": {
                "nodes": {
                    "n00": {
                        "category": "biolink:PopulationOfIndividualOrganisms"
                    },
                    "n01": {
                        "category": biolink_class
                    }
                },
                "edges": {
                    "e00": {
                        "predicate": "biolink:correlated_with",
                        "subject": "n00",
                        "object": "n01"
                    }
                }
            }
        }
    }


def do_verify_response(resp_json, results=True):
    """Perform basic formatting checks on response object.

    knode keys should be unique
    kedge keys should be unique
    equivalent_identifiers should be a list of strings
    kedges should reference real knodes
    message_code, tool_version, and datetime should exist
    result bindings should reference real kgraph elements
    """
    assert "return value" in resp_json
    return_value = resp_json["return value"]
    assert "knowledge_graph" in return_value["message"]
    knowledge_graph = return_value["message"]["knowledge_graph"]
    nodes = knowledge_graph["nodes"]
    for node in nodes:
        if "equivalent_identifiers" in node:
            equivalent_ids = node["equivalent_identifiers"]
            assert (
                isinstance(equivalent_ids, list) and
                all(isinstance(x, str) for x in equivalent_ids)
            )
    node_ids_list = list(nodes)
    node_ids = set(node_ids_list)
    assert len(node_ids_list) == len(node_ids)
    edges = knowledge_graph["edges"]
    edge_ids_list = list(edges)
    edge_ids = set(edge_ids_list)
    assert len(edge_ids_list) == len(edge_ids)
    for edge in edges.values():
        assert edge["subject"] in node_ids
        assert edge["object"] in node_ids

    assert "message_code" in resp_json["return value"]
    assert "tool_version" in resp_json["return value"]
    assert "datetime" in resp_json["return value"]

    if results:
        assert len(return_value["message"]["results"]) > 0
        assert "n_results" in return_value
        n_results = return_value["n_results"]
        assert "results" in return_value["message"]
        results = return_value["message"]["results"]
        assert n_results == len(results)
        for result in results:
            node_bindings = result["node_bindings"]
            edge_bindings = result["edge_bindings"]
            for node_binding_value in node_bindings.values():
                assert all(nbv["id"] in node_ids for nbv in node_binding_value)
            for edge_binding_value in edge_bindings.values():
                assert all(ebv["id"] in edge_ids for ebv in edge_binding_value)


def do_verify_feature_matrix_response(respjson):
    assert isinstance(respjson, dict)
    assert "chi_squared" in respjson
    assert "p_value" in respjson
    assert "columns" in respjson
    assert "rows" in respjson
    assert "feature_matrix" in respjson


def do_verify_feature_count_response(respjson):
    assert isinstance(respjson, list)
    for feature_count in respjson:
        assert "feature" in feature_count
        feature = feature_count["feature"]
        assert "feature_name" in feature
        assert "feature_qualifiers" in feature
        feature_qualifiers = feature["feature_qualifiers"]
        assert isinstance(feature_qualifiers, list)
        for feature_qualifier in feature_qualifiers:
            assert "operator" in feature_qualifier
            assert "value" in feature_qualifier
        assert "feature_matrix" in feature_count
        for stats in feature_count["feature_matrix"]:
            assert "frequency" in stats
            assert "percentage" in stats
