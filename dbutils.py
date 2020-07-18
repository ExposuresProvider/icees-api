import pandas as pd
import sys
import argparse
import db
import psycopg2
import os
import sys
import logging
from sqlalchemy import Index

from features import model, features

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
    with db.DBConnection() as conn:
        with conn.begin() as trans:
            model.metadata.create_all(conn)

def create_indices():
    itrunc = 0
    def truncate(a, length=63):
        logger.info("creating index " + a)
        sys.stdout.flush()
        sys.stderr.flush()
        nonlocal itrunc
        prefix = "index" + str(itrunc)
        itrunc += 1
        return prefix + a[:63-len(prefix)]
    with db.DBConnection() as conn:
        with conn.begin() as trans:

            tables = model.tables
            for table, table_features in features.features.items():
                Index(truncate(table + "_year"), tables[table].c.year).create(conn)
                cols = list(map(lambda a : a[0], table_features))
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

def insert(input_file, table_name):
    pg_load_table(input_file, table_name)
    # df0 = pd.read_csv(input_file)
    # table_features = features.features_dict[table_name]
    # # dtypes = {col: ty["type"] for col, ty in table_features.items()}
    # # df = df0.astype(dtypes)
    # df = df0
    # print("processing table " + table_name)
    # for col, ty in table_features.items():
    #     df.rename(columns={"index": table_name[0].upper() + table_name[1:] + "Id"}, inplace=True)
    #     if col in df:
    #         df[col] = type_dict[ty["type"]](df[col])
    # def toDType(table):
    #     l = []
    #     for col_name, col_type, *_ in table:
    #         l.append((col_name, col_type))
    #     return dict(l)
        
    # with db.DBConnection() as conn:
    #     with conn.begin() as trans:
    #         df.to_sql(name=table_name, con=conn,if_exists="append", 
    #                   index=False, 
    #                   dtype=toDType(features.features[table_name]),
    #                   chunksize=4096)

def pg_load_table(file_path, table_name):
    dbname = os.environ["ICEES_DATABASE"]
    host = os.environ["ICEES_HOST"]
    port = os.environ["ICEES_PORT"]
    user = os.environ["ICEES_DBUSER"]
    pwd = os.environ["ICEES_DBPASS"]

    with psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=pwd) as conn:
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Loading data from {} into {}".format(file_path, table_name))

        with open(file_path, "r") as f:
            columns = ["\"" + (model.table_id(table_name) if x == "index" else x) + "\"" for x in next(f).strip().split(",")]
            cur.copy_from(f, table_name, sep=",", null="", columns=columns)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='ICEES DB Utitilies')
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
