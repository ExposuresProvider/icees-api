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

def bin_qualifier_schema(ty, levels):
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
        "anyOf": [
            {
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
            },{
                "type": "object",
                "properties": {
                    "operator": {
                        "type": "string",
                        "enum": ["between"]
                    },
                    "value_a": yamltype,
                    "value_b": yamltype
                },
                "required": ["operator", "value_a", "value_b"],
                "additionalProperties": False
            },{
                "type": "object",
                "properties": {
                    "operator": {
                        "type": "string",
                        "enum": ["in"]
                    },
                    "values": {
                        "type" : "array",
                        "items": yamltype
                    }
                },
                "required": ["operator", "values"],
                "additionalProperties": False
            }]
    }


def bins_schema(table_name):
    return {
        "type": "object",
        "properties": {k: {
            "type": "array",
            "items": bin_qualifier_schema(v, levels)
        } for k, v, levels in features[table_name]},
        "additionalProperties": False
    }


def feature_association2_schema(table_name):
    return {
        "type": "object",
        "properties": {
            "feature_a": bins_schema(table_name),
            "feature_b": bins_schema(table_name)
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


def feature_association2_schema_output(table_name):
    return {
    }


def associations_to_all_features_schema_output(table_name):
    return {
    }


class ExplicitDumper(yaml.SafeDumper):
    """
    A dumper that will never emit aliases.
    """

    def ignore_aliases(self, data):
        return True

def generate_schema():
    dir = "definitions"
    if not os.path.exists(dir):
        os.makedirs(dir)    
    with open(dir + "/cohort_patient_input.yaml", "w") as f:
        yaml.dump(cohort_schema("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association_patient_input.yaml", "w") as f:
        yaml.dump(feature_association_schema("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association2_patient_input.yaml", "w") as f:
        yaml.dump(feature_association2_schema("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/associations_to_all_features_patient_input.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/cohort_visit_input.yaml", "w") as f:
        yaml.dump(cohort_schema("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association_visit_input.yaml", "w") as f:
        yaml.dump(feature_association_schema("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association2_visit_input.yaml", "w") as f:
        yaml.dump(feature_association2_schema("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/associations_to_all_features_visit_input.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/features_patient_output.yaml", "w") as f:
        yaml.dump(features_schema_output("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/cohort_dictionary_patient_output.yaml", "w") as f:
        yaml.dump(cohort_dictionary_schema_output("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/cohort_patient_output.yaml", "w") as f:
        yaml.dump(cohort_schema_output("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association_patient_output.yaml", "w") as f:
        yaml.dump(feature_association_schema_output("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association2_patient_output.yaml", "w") as f:
        yaml.dump(feature_association2_schema_output("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/associations_to_all_features_patient_output.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema_output("patient"), f, Dumper=ExplicitDumper)
    with open(dir + "/cohort_visit_output.yaml", "w") as f:
        yaml.dump(cohort_schema_output("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/features_visit_output.yaml", "w") as f:
        yaml.dump(features_schema_output("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/cohort_dictionary_visit_output.yaml", "w") as f:
        yaml.dump(cohort_dictionary_schema_output("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association_visit_output.yaml", "w") as f:
        yaml.dump(feature_association_schema_output("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/feature_association2_visit_output.yaml", "w") as f:
        yaml.dump(feature_association2_schema_output("visit"), f, Dumper=ExplicitDumper)
    with open(dir + "/associations_to_all_features_visit_output.yaml", "w") as f:
        yaml.dump(associations_to_all_features_schema_output("visit"), f, Dumper=ExplicitDumper)
    
if __name__ == '__main__':
    generate_schema()
