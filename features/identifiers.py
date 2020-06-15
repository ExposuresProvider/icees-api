import csv
import os
import logging
import yaml

logger = logging.getLogger(__name__)

input_file = os.path.join(os.path.dirname(__file__), "..", "config", "identifiers.csv")

input_dict = yaml.load(input_file)

def get_identifiers(table, feature, return_empty_list=False):
    if table in input_dict:
        identifier_dict = input_dict[table]
    else:
        raise RuntimeError("Cannot find table " + table)
    if feature2 in identifier_dict:
        return identifier_dict[feature2]
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
        raise RuntimeError("Cannot find table " + table)

    return [feature for feature, identifiers in identifier_dict.items() if identifier in identifiers]
        

