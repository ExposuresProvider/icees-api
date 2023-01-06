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
