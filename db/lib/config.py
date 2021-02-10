import os
from pathlib import Path


def get_config_path():
    return os.environ.get("CONFIG_PATH", Path(__file__).parent.parent.parent / "config")
