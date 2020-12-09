import csv
import os
import logging
import yaml
from tx.functional.either import Left, Right
from .config import get_config_path

logger = logging.getLogger(__name__)

input_file = os.path.join(get_config_path(), "identifiers.yml")

with open(input_file) as inpf:
    input_dict = yaml.safe_load(inpf)

def get_identifiers(table, feature, return_empty_list=False):
    if table in input_dict:
        identifier_dict = input_dict[table]
    else:
        raise RuntimeError("Cannot find table " + table)
    if feature in identifier_dict:
        return identifier_dict[feature]
    else:
        errmsg = "Cannot find identifiers for feature " + feature
        logger.error(errmsg)
        if return_empty_list:
            return []
        else:
            raise RuntimeError(errmsg)

        
def get_features_by_identifier(table, identifier):
    if table in input_dict:
        identifier_dict = input_dict[table]
    else:
        raise Left(f"Cannot find table {table}, available {input_dict}")

    return Right([feature for feature, identifiers in identifier_dict.items() if identifier in identifiers])
        

