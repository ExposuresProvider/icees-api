"""Identifiers."""
import logging
import os

from tx.functional.either import Left, Right
import yaml

from .config import get_config_path

logger = logging.getLogger(__name__)

input_file = os.path.join(get_config_path(), "identifiers.yml")

with open(input_file) as inpf:
    input_dict = yaml.safe_load(inpf)


def get_identifiers(table, feature, return_empty_list=False):
    """Get identifiers."""
    if table in input_dict:
        identifier_dict = input_dict[table]
    else:
        raise RuntimeError("Cannot find table " + table)
    if feature not in identifier_dict:
        errmsg = "Cannot find identifiers for feature " + feature
        logger.error(errmsg)
        if return_empty_list:
            return []
        else:
            raise RuntimeError(errmsg)
    return identifier_dict[feature]


def get_features_by_identifier(table, identifier):
    """Get features by identifier."""
    if table not in input_dict:
        raise Left(f"Cannot find table {table}, available {input_dict}")
    identifier_dict = input_dict[table]

    return Right([
        feature
        for feature, identifiers in identifier_dict.items()
        if identifier in identifiers
    ])
