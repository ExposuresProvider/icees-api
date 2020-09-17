import yaml
import os
from sqlalchemy import Integer, String, Enum, Float
from typing import Union, List
from dataclasses import dataclass
from .config import get_config_path


@dataclass
class Feature:
    name: str 
    _type: Union[Integer, String, Enum, Float]
    options: List[str]
    biolink_class: str


with open(os.path.join(get_config_path(), 'features.yml'), 'r') as f:
    features_dict = yaml.load(f, Loader=yaml.SafeLoader)


def dict_to_Feature(table, key, value):
    """Convert feature from dict form to tuple."""
    if value['type'] == 'integer':
        _type = Integer
        if 'minimum' in value and 'maximum' in value:
            options = list(range(value['minimum'], value['maximum'] + 1))
        else:
            options = None
    elif value['type'] == 'string':
        if 'enum' in value:
            options = value['enum']
            _type = Enum(*options, name=table+"_"+key)
        else:
            _type = String
            options = None
    elif value['type'] == 'number':
        _type = Float
        options = None
    else:
        raise ValueError('Unsupported type {}'.format(value['type']))
    biolink_type = value.get('biolinkType')
    if biolink_type is None:
        print("cannot find biolinkType for " + key + " in " + str(value))
    return Feature(key, _type, options, biolink_type)


features = {
    key0: [dict_to_Feature(key0, key1, value1) for key1, value1 in value0.items()]
    for key0, value0 in features_dict.items()
}


def lookUpFeatureClass(table, feature):
    for f in features[table]:
        if f.name == feature:
            return f.biolink_class
    return None
