import os
import os.path


def get_config_path():
    return os.environ.get("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "..", "config"))
