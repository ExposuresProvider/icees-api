"""Database utilities."""
import argparse
import csv
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import sqlite3
import sys

import pandas as pd
import psycopg2
from sqlalchemy import Index

from .db import DBConnection
from .features import features
from .model import metadata, tables, table_id

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def createargs(args):
    create()


def create():
    """Build database schema."""
    with DBConnection() as conn:
        with conn.begin() as trans:
            metadata.create_all(conn)


def create_indices():
    """Build database indexes."""
    itrunc = 0
    def truncate(a, length=63):
        logger.info("creating index " + a)
        nonlocal itrunc
        prefix = "index" + str(itrunc)
        itrunc += 1
        return prefix + a[:63-len(prefix)]
    with DBConnection() as conn:
        with conn.begin() as trans:

            for table, table_features in features.items():
                id_col = table[0].upper() + table[1:] + "Id"
                Index(truncate(table + "_" + id_col), tables[table].c[id_col]).create(conn)
                Index(truncate(table + "_year"), tables[table].c.year).create(conn)
                cols = list(map(lambda a : a.name, table_features))
                for feature in cols:
                    Index(truncate(table + "_" + feature), tables[table].c[feature]).create(conn)
                    Index(truncate(table + "_year_" + feature), tables[table].c.year, tables[table].c[feature]).create(conn)
#                    for feature2 in cols:
#                        Index(truncate(table + "_year_" + feature + "_" + feature2), tables[table].c.year, tables[table].c[feature], tables[table].c[feature2]).create(conn)

    
def insertargs(args):
    insert(args.input_file, args.table_name)


type_dict = {
    "integer": lambda s : s.astype(pd.Int64Dtype()),
    "string": lambda s : s.astype(str, skipna=True)
}

db_ = os.environ["ICEES_DB"]
host = os.environ["ICEES_HOST"]
DATAPATH = Path(os.environ["DATA_PATH"])


@contextmanager
def db_connections():
    """Database connection context manager."""
    if db_ == "sqlite":
        con = sqlite3.connect(DATAPATH / "example.db")
    elif db_ == "postgres":
        con = psycopg2.connect(
            host=host,
            database="icees_database",
            user="icees_dbuser",
            password="icees_dbpass",
        )
    else:
        raise ValueError(f"Unsupported database '{db_}'")

    yield con

    con.commit()
    con.close()


def insert(file_path, table_name):
    """Insert data from file into table."""
    with open(file_path, "r") as f:
        columns = [
            (table_id(table_name) if x == "index" else x)
            for x in next(f).strip().split(",")
        ]

    with open(file_path, "r") as stream:
        reader = csv.DictReader(stream)
        to_db = [
            tuple(row.get(col) for col in columns)
            for row in reader
        ]

    with db_connections() as con:

        cur = con.cursor()
        if db_ == "sqlite":
            placeholders = ", ".join("?" for _ in columns)
        else:
            placeholders = ", ".join("%s" for _ in columns)
        query = "INSERT INTO {0} ({1}) VALUES ({2});".format(
            table_name,
            ", ".join(f"\"{col}\"" for col in columns),
            placeholders,
        )
        cur.executemany(query, to_db)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='ICEES DB Utilities')
    subparsers = parser.add_subparsers(help='subcommands')
    # create the parser for the "create" command
    parser_create = subparsers.add_parser('create', help='create tables')
    parser_create.set_defaults(func=createargs)
    
    # create the parser for the "insert" command
    parser_insert = subparsers.add_parser('insert', help='insert data into database')
    parser_insert.add_argument('input_file', type=str, help='csv file')
    parser_insert.add_argument('table_name', type=str, help='table name')
    parser_insert.set_defaults(func=insertargs)
    
    args = parser.parse_args(sys.argv[1:])
    args.func(args)
