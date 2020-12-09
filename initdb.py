import sys
import json
import os
import subprocess
import time
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import tempfile
from features import features
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info("waiting for database startup")
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

    logger.info("user not found initializing db")
    cursor.execute("CREATE USER " + dbuser + " with password '" + dbpass + "'")

    db = os.environ["ICEES_DATABASE"]
    cursor.execute("CREATE DATABASE " + db)
    cursor.execute("GRANT ALL ON DATABASE " + db + " to " + dbuser)
    dbutils.create()
    csvdir = os.environ.get("DATA_PATH", "db/data/")
    for t in features.features_dict.keys():
        table_dir = csvdir + "/" + t
        if os.path.isdir(table_dir):
            logger.info(table_dir + " exists")
            for f in os.listdir(table_dir):
                table = table_dir + "/" + f
                logger.info("loading " + table)
                dbutils.insert(table, t)
        else:
            logger.info("generating data " + t)
            temp = tempfile.NamedTemporaryFile()
            try:
                sample.generate_data(t, [2010, 2011], 1000, temp.name)
                dbutils.insert(temp.name, t)
            finally:
                temp.close()
    dbutils.create_indices()
    logger.info("db initialized")
else:
    logger.info("db already initialized")

cursor.close()
conn.close()



