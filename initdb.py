import sys
import json
import os
import subprocess
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import tempfile

print("waiting for database startup")
time.sleep(10)

os.environ["ICEES_HOST"]="localhost"
os.environ["ICEES_PORT"]="5432"

import dbutils
import sample

dbuser = os.environ["ICEES_DBUSER"]
dbpass = os.environ["ICEES_DBPASS"]

conn = psycopg2.connect(dbname='postgres', user="postgres", host='localhost', password="postgres")

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()

cursor.execute("SELECT count(*) FROM pg_catalog.pg_user WHERE usename = '" + dbuser + "'")
p = cursor.fetchall()[0][0]

if p == 0:

    print("user not found initializing db")
    cursor.execute("CREATE USER " + dbuser + " with password '" + dbpass + "'")

    db = os.environ["ICEES_DATABASE"]
    cursor.execute("CREATE DATABASE " + db)
    cursor.execute("GRANT ALL ON DATABASE " + db + " to " + dbuser)
    dbutils.create()
    csvdir = "db/data/"
    for t in os.listdir(csvdir):
        table_dir = csvdir + "/" + t
        print(table_dir, t, os.stat(table_dir))
        if os.path.isdir(table_dir):
            for f in os.listdir(table_dir):
                table = table_dir + "/" + f
                print("loading " + table)
                dbutils.insert(table, t)
        else:
            print("generating data " + t)
            temp = tempfile.NamedTemporaryFile()
            try:
                sample.generate_data(t, [2010], 1000, temp.name)
                dbutils.insert(temp.name, t)
            finally:
                temp.close()
    print("db initialized")
else:
    print("db already initialized")

cursor.close()
conn.close()



