"""Feature -> (categories, ids) mappings."""
import os

import yaml

from .config import get_config_path


with open(os.path.join(get_config_path(), "mappings.yml"), "r") as f:
    mappings = yaml.load(f, Loader=yaml.SafeLoader)
