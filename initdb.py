import logging
import os
import tempfile

from db.dbutils import create, insert, create_indices
from db.features import features_dict
from db.sample import generate_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info("waiting for database startup")

os.environ["ICEES_HOST"] = "localhost"
os.environ["ICEES_PORT"] = "5432"
db_ = os.environ["ICEES_DB"]


# conn = sqlite3.connect('example.db')

# cursor = conn.cursor()

# cursor.execute("SELECT count(*) FROM pg_catalog.pg_user WHERE usename = '" + dbuser + "'")
# p = cursor.fetchall()[0][0]

# if p == 0:

#     logger.info("user not found initializing db")
#     cursor.execute("CREATE USER " + dbuser + " with password '" + dbpass + "'")

#     db = os.environ["ICEES_DATABASE"]
#     cursor.execute("CREATE DATABASE " + db)
#     cursor.execute("GRANT ALL ON DATABASE " + db + " to " + dbuser)

def setup():
    if db_ == "sqlite" and os.path.isfile("example.db"):
        return
    #     conn = sqlite3.connect('example.db')
    # elif db_ == "postgres":

    

    # cursor = conn.cursor()

    # cur.execute(f"DROP TABLE {table_name};")
    create()
    csvdir = os.environ.get("DATA_PATH", "db/data/")
    for t in features_dict.keys():
        table_dir = csvdir + "/" + t
        if os.path.isdir(table_dir):
            logger.info(table_dir + " exists")
            for f in os.listdir(table_dir):
                table = table_dir + "/" + f
                logger.info("loading " + table)
                insert(table, t)
        else:
            logger.info("generating data " + t)
            temp = tempfile.NamedTemporaryFile()
            try:
                generate_data(t, [2010, 2011], 1000, temp.name)
                insert(temp.name, t)
            finally:
                temp.close()
    create_indices()
setup()

# cursor.close()
# conn.close()



