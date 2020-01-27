from sqlalchemy import create_engine
import json
import os
from contextlib import contextmanager


service_name = "ICEES"

serv_user = os.environ[service_name + "_DBUSER"]
serv_password = os.environ[service_name + "_DBPASS"]
serv_host = os.environ[service_name + "_HOST"]
serv_port = os.environ[service_name + "_PORT"]
serv_database = json.loads(os.environ[service_name + "_DATABASE"])
serv_max_overflow = int(os.environ[service_name + "_DB_MAX_OVERFLOW"])
serv_pool_size = int(os.environ[service_name + "_DB_POOL_SIZE"])

engine = None

def get_db_connection():
    global engine
    if engine is None:
        engine = create_engine("postgresql+psycopg2://"+serv_user+":"+serv_password+"@"+serv_host+":"+serv_port+"/"+serv_database, pool_size=serv_pool_size, max_overflow=serv_max_overflow)

    return engine


@contextmanager
def DBConnection():
    engine = get_db_connection()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()
    #    engine.dispose()


