"""Feature -> (categories, ids) mappings."""
import csv
import os

import yaml

from .config import get_config_path


with open(os.path.join(get_config_path(), "mappings.yml"), "r") as f:
    mappings = yaml.load(f, Loader=yaml.SafeLoader)

with open(os.path.join(get_config_path(), "value_sets.yml"), "r") as f:
    value_sets = yaml.load(f, Loader=yaml.SafeLoader)

with open(os.path.join(get_config_path(), "correlations.csv"), "r") as f:
    correlations = list(csv.reader(f))
