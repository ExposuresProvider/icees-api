import yaml
from sqlalchemy import Integer, String, Enum


with open('config/features.yml', 'r') as f:
    features_dict = yaml.load(f, Loader=yaml.SafeLoader)


def dict_to_tuple(key, value):
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
            _type = Enum(*options, name=key)
        else:
            _type = String
            options = None
    return (key, _type, options, value['biolinkType'])


features = {
    key0: [dict_to_tuple(key1, value1) for key1, value1 in value0.items()]
    for key0, value0 in features_dict.items()
}


def lookUpFeatureClass(table, feature):
    for n, _, _, c in features[table]:
        if n == feature:
            return c
    return None
