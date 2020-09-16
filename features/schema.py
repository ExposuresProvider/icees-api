from .features import features
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
            "value": yamltype,
        },
        "required": ["operator", "value"],
        "additionalProperties": False
    }


def qualifier_schema_list(f, ty, levels):
    return {
        "type": "object",
        "properties": {
            "feature_name": {
                "type": "string",
                "enum": [f],
            },
            "feature_qualifier": qualifier_schema(ty, levels),
            "year": {
                "type": "integer"
            }
        },
        "required": ["feature_name", "feature_qualifier"],
        "additionalProperties": False
    }


def cohort_schema(table_name):
    return {
        "anyOf": [
            {
                "type": "object",
                "properties": {k: qualifier_schema(v, levels) for k, v, levels, _ in features[table_name]},
                "additionalProperties": False
            },
            {
                "type": "array",
                "items": {
                    "anyOf": [qualifier_schema_list(k, v, levels) for k, v, levels, _ in features[table_name]]
                }
            }
        ]
    }

def name_schema_output():
    return {
        "type": "object",
        "properties": {
            "name" : {
                "type" : "string"
            },
            "cohort_id" : {
                "type" : "string"
            }
        },
        "required": ["name", "cohort_id"],
        "additionalProperties": False
    }

def add_name_by_id_schema():
    return {
        "type": "object",
        "properties": {
            "cohort_id" : {
                "type" : "string"
            }
        },
        "required": ["cohort_id"],
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


def bin_qualifier_schema_list(f, ty, levels):
    return {
        "type": "object",
        "properties": {
            "feature_name": {
                "type": "string",
                "enum": [f]
            },
            "feature_qualifier": bin_qualifier_schema(ty, levels),
            "year": {
                "type": "integer"
            }
        },
        "required": ["feature_name", "feature_qualifier"],
        "additionalProperties": False
    }


def bins_schema(table_name):
    return {
        "anyOf": [
            {
                "type": "object",
                "properties": {k: {
                    "type": "array",
                    "items": bin_qualifier_schema(v, levels)
                } for k, v, levels, _ in features[table_name]},
                "additionalProperties": False
            },
            {
                "type": "array",
                "items": {
                    "anyOf": [bin_qualifier_schema_list(k, v, levels) for k, v, levels, _ in features[table_name]]
                }
            }
        ]
    }


def feature_association_schema(table_name):
    return feature_association_schema_common(table_name, cohort_schema(table_name))


def feature_association2_schema(table_name):
    return feature_association_schema_common(table_name, bins_schema(table_name))

    
def feature_association_schema_common(table_name, feature_schema):
    return {
        "type": "object",
        "properties": {
            "feature_a": feature_schema,
            "feature_b": feature_schema,
            "check_coverage_is_full": {
                "type": "boolean"
            }
        },
        "required": ["feature_a", "feature_b"],
        "additionalProperties": False
    }


def associations_to_all_features_schema(table_name):
    return associations_to_all_features_schema_common(table_name, cohort_schema(table_name))


def associations_to_all_features2_schema(table_name):
    return associations_to_all_features_schema_common(table_name, bins_schema(table_name))


def associations_to_all_features_schema_common(table_name, feature_schema):
    return {
        "type": "object",
        "properties": {
            "feature": feature_schema,
            "maximum_p_value": {
                "type": "number"
            },
            "correction": {
                "type": "object",
                "anyOf": [
                    {
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [
                                    "bonferroni",
                                    "sidak",
                                    "holm-sidak",
                                    "holm",
                                    "simes-hochberg",
                                    "hommel",
                                    "fdr_bh",
                                    "fdr_by" 
                                ]
                            }
                        },
                        "required": [
                            "method"
                        ],
                        "additionalProperties": False
                    }, {
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [
                                    "fdr_tsbh" ,
                                    "fdr_tsbky"
                                ]
                            },
                            "alpha": {
                                "type": "number"
                            }
                        },
                        "required": [
                            "method",
                            "alpha"
                        ],
                        "additionalProperties": False
                    }
                ]
            },
            "check_coverage_is_full": {
                "type": "boolean"
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


def identifiers_output():
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
    with open(dir + "/add_name_by_id_input.yaml", "w") as f:
        yaml.dump(add_name_by_id_schema(), f, Dumper=ExplicitDumper)
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
    with open(dir + "/name_output.yaml", "w") as f:
        yaml.dump(name_schema_output(), f, Dumper=ExplicitDumper)
    with open(dir + "/identifiers_output.yaml", "w") as f:
        yaml.dump(identifiers_output(), f, Dumper=ExplicitDumper)
    
if __name__ == '__main__':
    generate_schema()
