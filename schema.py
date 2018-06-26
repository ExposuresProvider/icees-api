from features import features
from sqlalchemy import String, Integer
import yaml
import os

def qualifier_schema(ty, levels):
    if ty is String:
        yamltype = {
            "type": "string"
        }
    elif ty is Integer:
        yamltype = {
            "type": "integer"
        }
    else:
        yamltype = {}

    if levels is not None:
        yamltype["enum"] = list(levels)

    return {
        "type": "object",
        "properties": {
            "operator": {
                "type": "string",
                "enum": ["<", ">", "<=", ">=", "=", "<>"]
            },
            "value": yamltype
        },
        "required": ["operator", "value"],
        "additionalProperties": False
    }


def cohort_schema(table_name):
    return {
        "type": "object",
        "properties": {k: qualifier_schema(v, levels) for k, v, levels in features[table_name]},
        "additionalProperties": False
    }


def feature_association_schema(table_name):
    return {
        "type": "object",
        "properties": {
            "feature_a": cohort_schema(table_name),
            "feature_b": cohort_schema(table_name)
        },
        "required": ["feature_a", "feature_b"],
        "additionalProperties": False
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
        "required": ["feature", "maximum_p_value"],
        "additionalProperties": False
    }


def features_schema_output(table_name):
    return {
    }


def cohort_dictionary_schema_output(table_name):
    return {
    }


def cohort_schema_output(table_name):
    return {
    }


def feature_association_schema_output(table_name):
    return {
    }


def associations_to_all_features_schema_output(table_name):
    return {
    }


def generate_schema():
    dir = "definitions"
    if not os.path.exists(dir):
        os.makedirs(dir)    
    with open(dir + "/cohort_patient_input.yaml", "w") as f:
        yaml.dump(cohort_schema("patient"), f)
    with open(dir + "/feature_association_patient_input.yaml", "w") as f:
        yaml.dump(feature_association_schema("patient"), f)
    with open(dir + "/associations_to_all_features_patient_input.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema("patient"), f)
    with open(dir + "/cohort_visit_input.yaml", "w") as f:
        yaml.dump(cohort_schema("visit"), f)
    with open(dir + "/feature_association_visit_input.yaml", "w") as f:
        yaml.dump(feature_association_schema("visit"), f)
    with open(dir + "/associations_to_all_features_visit_input.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema("visit"), f)
    with open(dir + "/features_patient_output.yaml", "w") as f:
        yaml.dump(features("patient"), f)
    with open(dir + "/cohort_dictionary_patient_output.yaml", "w") as f:
        yaml.dump(features("patient"), f)
    with open(dir + "/cohort_patient_output.yaml", "w") as f:
        yaml.dump(cohort_schema_output("patient"), f)
    with open(dir + "/feature_association_patient_output.yaml", "w") as f:
        yaml.dump(feature_association_schema_output("patient"), f)
    with open(dir + "/associations_to_all_features_patient_output.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema_output("patient"), f)
    with open(dir + "/cohort_visit_output.yaml", "w") as f:
        yaml.dump(cohort_schema_output("visit"), f)
    with open(dir + "/features_visit_output.yaml", "w") as f:
        yaml.dump(features("visit"), f)
    with open(dir + "/cohort_dictionary_visit_output.yaml", "w") as f:
        yaml.dump(features("visit"), f)
    with open(dir + "/feature_association_visit_output.yaml", "w") as f:
        yaml.dump(feature_association_schema_output("visit"), f)
    with open(dir + "/associations_to_all_features_visit_output.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema_output("visit"), f)
    
if __name__ == '__main__':
    generate_schema()
