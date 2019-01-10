import pandas as pd
import sys
import argparse

from features import model, features

def createargs(args):
    create(args.version)

def create(version):
    with model[version].DBConnection() as conn:
        with conn.begin() as trans:
            model[version].metadata.create_all(conn)
    
def insertargs(args):
    insert(args.version, args.input_file, args.table_name)

def insert(version, input_file, table_name):
    df = pd.read_csv(input_file)
    def toDType(table):
        print(table)
        l = []
        for col_name, col_type, _, _ in table:
            l.append((col_name, col_type))
        return dict(l)
        
    with model[version].DBConnection() as conn:
        with conn.begin() as trans:
            df.to_sql(name=table_name, con=conn,if_exists="append", 
                      index=False, 
                      dtype=toDType(features[version].features[table_name]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='ICEES DB Utitilies')
    parser.add_argument('--version', required=True, type=str, help='version of the database')
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
