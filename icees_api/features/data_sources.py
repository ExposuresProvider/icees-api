"""Data sources."""
import json
import os

from .config import get_config_path


with open(os.path.join(get_config_path(), "data_sources.json"), "r") as f:
    data_sources = json.load(f)
