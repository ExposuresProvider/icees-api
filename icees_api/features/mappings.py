import os
import yaml
from .config import get_config_path


all_features = None
value_sets = None


def get_all_features():
    global all_features

    if all_features is None:
        with open(os.path.join(get_config_path(), "all_features.yaml"), "r") as f:
            all_features_defined = yaml.load(f, Loader=yaml.SafeLoader)
            if 'patient' not in all_features_defined:
                raise ValueError('all features yaml file must contain patient key')
            all_features = all_features_defined['patient'].keys()
    return all_features


def get_value_sets():
    global value_sets
    if value_sets is None:
        with open(os.path.join(get_config_path(), "value_sets.yml"), "r") as f:
            value_sets = yaml.load(f, Loader=yaml.SafeLoader)
    return value_sets
