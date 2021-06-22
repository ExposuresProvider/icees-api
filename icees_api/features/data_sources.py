"""Data sources."""
import os

from .config import get_config_path


with open(os.path.join(get_config_path(), "data_sources.txt"), "r") as f:
    data_sources = [
        source
        for line in f.readlines()
        if (source := line.strip())
    ]
