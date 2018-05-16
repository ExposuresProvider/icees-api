from features import features
from sqlalchemy import String, Integer


def qualifier_schema(ty, levels):
    if ty is String:
        jsontype = {
                "type": "string"
        }
    elif ty is Integer:
        jsontype = {
            "type": "integer"
        }
    else:
        jsontype = {}

    if levels is not None:
        jsontype["enum"] = levels

    return {
        "type": "object",
        "properties": {
            "operator": {
                "type": "string",
                "enum": ["<", ">", "<=", ">=", "=", "<>"]
            },
            "value": {
                "type": jsontype
            }
        },
        "required": ["operator", "value"]
    }


def cohort_schema(table_name):
    return {
        "type": "object",
        "properties": {k: qualifier_schema(v, levels) for k, v, levels in features[table_name]}
    }


def feature_association_schema(table_name):
    return {
        "type": "object",
        "properties": {
            "feature_a": cohort_schema(table_name),
            "feature_b": cohort_schema(table_name)
        },
        "required": ["feature_a", "feature_b"]
    }


def associations_to_all_features_schema(table_name):
    return {
        "type": "object",
        "properties": {
            "feature": cohort_schema(table_name),
            "maximum_p_value": {
                "type": "number"
            }
        },
        "required": ["feature", "maximum_p_value"]
    }
