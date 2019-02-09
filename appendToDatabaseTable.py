import pandas as pd
import sys
import os
from sqlalchemy import create_engine

db = sys.argv[1]
table = sys.argv[2]
input_file = sys.argv[3]

df = pd.read_csv(input_file)

dbuser = os.environ["ICEES_DBUSER"]
dbpass = os.environ["ICEES_DBPASS"]
host = os.environ["ICEES_HOST"]
port = os.environ["ICEES_PORT"]

engine = create_engine("postgresql://" + dbuser + ":" + dbpass + "@" + host + ":" + port + "/" + db)

df.to_sql(table, engine, if_exists="append", index="False")
