"""Initialize database."""
import logging
import os
import tempfile
from pathlib import Path

from lib.dbutils import create, insert, create_indices
from lib.features import features_dict
from lib.sample import generate_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info("waiting for database startup")


def setup():
    """Set up database."""
    if os.environ["ICEES_DB"] == "sqlite" and (Path(os.environ["DATA_PATH"]) / "example.db").exists():
        return

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


if __name__ == "__main__":
    setup()
