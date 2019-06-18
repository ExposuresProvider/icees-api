import pandas as pd
import sys
import argparse

from features import model, features

def create(args):
    conn = model[args.version].get_db_connection()
    model[args.version].metadata.create_all(conn)
    
def insert(args):
    df = pd.read_csv(args.input_file)
    conn = model[args.version].get_db_connection()
    def toDType(table):
        print(table)
        l = []
        for col_name, col_type, _, _ in table:
            l.append((col_name, col_type))
        return dict(l)
        
    for k,v in map(toDType, features[args.version].features[args.table_name]):
        print(k,v)
    df.to_sql(name=args.table_name, con=conn,if_exists="append", index_label=args.index_col, dtype=map(toDType, features[args.version].features[args.table_name]))

parser = argparse.ArgumentParser(prog='ICEES DB Utitilies')
parser.add_argument('--version', required=True, type=str, help='version of the database')
subparsers = parser.add_subparsers(help='subcommands')
# create the parser for the "create" command
parser_create = subparsers.add_parser('create', help='create tables')
parser_create.set_defaults(func=create)

# create the parser for the "insert" command
parser_insert = subparsers.add_parser('insert', help='insert data into database')
parser_insert.add_argument('input_file', type=str, help='csv file')
parser_insert.add_argument('table_name', type=str, help='table name')
parser_insert.add_argument('index_col', type=str, help='index column')
parser_insert.set_defaults(func=insert)

args = parser.parse_args(sys.argv[1:])
args.func(args)
