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

# engine_map = {}

def get_db_connection(version):
    #if hasattr(engine_map, version):
    #    engine = engine_map[version]
    #else:
    engine = create_engine("postgresql+psycopg2://"+serv_user+":"+serv_password+"@"+serv_host+":"+serv_port+"/"+serv_database[version], pool_size=serv_pool_size, max_overflow=serv_max_overflow)
    #    engine_map[version] = engine

    return engine


@contextmanager
def DBConnection(version):
    conn = get_db_connection(version)
    try:
        yield conn.connect()
    finally:
        conn.dispose()


