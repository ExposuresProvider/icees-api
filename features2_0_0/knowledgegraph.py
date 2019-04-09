from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, func, Sequence, between
from sqlalchemy.sql import select
from scipy.stats import chi2_contingency
import json
import os
from .features import features, lookUpFeatureClass
import inflection
import numpy as np
from .model import select_cohort, get_db_connection, select_associations_to_all_features
import datetime
from utils import to_qualifiers
import traceback

def get(conn, obj):
    try:
        query_message = obj["query_message"]
        cohort_definition = query_message["query_options"]
        table = cohort_definition["table"]
        year = cohort_definition["year"]
        cohort_features = cohort_definition["cohort_features"]
        feature = to_qualifiers(cohort_definition["feature"])
        maximum_p_value = cohort_definition["maximum_p_value"]

        query_graph = query_message["query_graph"]

        nodes = query_graph["nodes"]
        edges = query_graph["edges"]

        if len(nodes) != 2:
            raise NotImplementedError("Number of nodes in query graph must be 2")

        if len(edges) != 1:
            raise NotImplementedError("Number of edges in query graph must be 1")

        nodes_dict = {node["node_id"]: node for node in nodes}
        [edge] = edges

        source_id = edge["source_id"]
        if nodes_dict[source_id]["type"] != "population_of_individual_organisms":
            raise NotImplementedError("Sounce node must be population_of_individual_organisms")

        supported_types = {
            "chemical_substance": ["chemical_substance", "drug"]
        }

        target_id = edge["target_id"]
        target_node_type = nodes_dict[target_id]["type"]
        if target_node_type not in supported_types:
            raise NotImplementedError("Target node must be one of " + str(supported_types.keys()))

        def supported_type(feature_matrix):
            return feature_matrix["feature_b"]["biolink_class"] in supported_types[target_node_type]

        def feature_properties(feature_matrix):
            return {
                "feature_name": feature_matrix["feature_b"]["feature_name"],
                "p_value": feature_matrix["p_value"]
            }

        cohort_id, size = select_cohort(conn, table, year, cohort_features)

        ataf = select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value)

        feature_list = list(map(feature_properties, filter(supported_type, ataf)))

        def result(feature_property):
            return {
                "row_data" : [feature_property["feature_name"]],
                "score": feature_property["p_value"],
                "score_name": "p value"
            }


        message = {
            "reasoner_id": "ICEES",
            "tool_version": "2.0.0",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "n_results": len(feature_list),
            "message_code": "OK",
            "code_description": "",
            "table_column_names": [target_id],
            "results": list(map(result, feature_list))
        }
    except Exception as e:
        traceback.print_exc()
        message = {
            "reasoner_id": "ICEES",
            "tool_version": "2.0.0",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "message_code": "Error",
            "code_description": str(e),
        }

    return message

def get_schema():
    return {
        "population_of_individual_organisms": {
            "chemical_substance": [
                "affect"
            ]
        }
    }