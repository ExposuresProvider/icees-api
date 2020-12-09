def opposite(qualifier):
    return {
        "operator": {
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
            "=": "<>",
            "<>": "="
        }[qualifier["operator"]],
        "value": qualifier["value"]
    }


def to_qualifiers(feature):
    if len(feature) == 1:
        k, v = list(feature.items())[0]
        return {
            "feature_name": k,
            "feature_qualifiers": [v, opposite(v)]
        }
    else:
        k = feature["feature_name"]
        v = feature["feature_qualifier"]
        return {
            "feature_name": k,
            "feature_qualifiers": [v, opposite(v)]
        }

def to_qualifiers2(feature):
    if len(feature) == 1:
        k, v = list(feature.items())[0]
        return {
            "feature_name": k,
            "feature_qualifiers": v
        }
    else:
        return feature
