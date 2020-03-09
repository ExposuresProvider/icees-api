import pandas as pd
import sys
import argparse
import db

from features import model, features

def createargs(args):
    create()

def create():
    with db.DBConnection() as conn:
        with conn.begin() as trans:
            model.metadata.create_all(conn)
    
def insertargs(args):
    insert(args.input_file, args.table_name)

type_dict = {
    "integer": lambda s : s.astype(pd.Int64Dtype()),
    "string": lambda s : s.astype(str, skipna=True)
}
def insert(input_file, table_name):
    df0 = pd.read_csv(input_file)
    table_features = features.features_dict[table_name]
    # dtypes = {col: ty["type"] for col, ty in table_features.items()}
    # df = df0.astype(dtypes)
    df = df0
    print("processing table " + table_name)
    for col, ty in table_features.items():
        print("processing column " + col)
        if col in df:
            df[col] = type_dict[ty["type"]](df[col])
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df.dtypes)
    def toDType(table):
#        print(table)
        l = []
        for col_name, col_type, *_ in table:
            l.append((col_name, col_type))
        return dict(l)
        
    with db.DBConnection() as conn:
        with conn.begin() as trans:
            df.to_sql(name=table_name, con=conn,if_exists="append", 
                      index=False, 
                      dtype=toDType(features.features[table_name]),
                      chunksize=4096)

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
