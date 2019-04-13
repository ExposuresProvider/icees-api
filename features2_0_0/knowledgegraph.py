from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, func, Sequence, between
from sqlalchemy.sql import select
from scipy.stats import chi2_contingency
import json
import os
from .features import features, lookUpFeatureClass
import inflection
import numpy as np
from .model import get_ids_by_feature, get_db_connection, select_associations_to_all_features
import datetime
from utils import to_qualifiers
import traceback
import itertools
from .identifiers import get_identifiers

schema = {
        "population_of_individual_organisms": {
            "chemical_substance": ["association"]
        }
    }

subtypes = {
    "chemical_substance": ["chemical_substance", "drug"]
}

def get(conn, obj):
    try:
        # query_message = obj["query_message"]
        # cohort_definition = query_message["query_options"]
        cohort_definition = obj["query_options"]
        table = cohort_definition["table"]
        year = cohort_definition["year"]
        if "cohort_features" in cohort_definition:
            cohort_features = cohort_definition["cohort_features"]
        else:
            cohort_features = {}
        feature = to_qualifiers(cohort_definition["feature"])
        maximum_p_value = cohort_definition["maximum_p_value"]

        # query_graph = query_message["query_graph"]
        query_graph = obj["machine_question"]

        nodes = query_graph["nodes"]
        edges = query_graph["edges"]

        if len(nodes) != 2:
            raise NotImplementedError("Number of nodes in query graph must be 2")

        if len(edges) != 1:
            raise NotImplementedError("Number of edges in query graph must be 1")

        nodes_dict = {node["node_id"]: node for node in nodes}
        [edge] = edges

        source_id = edge["source_id"]
        source_node_type = nodes_dict[source_id]["type"]
        if source_node_type not in schema:
            raise NotImplementedError("Sounce node must be one of " + str(schema.keys()))

        target_id = edge["target_id"]
        target_node_type = nodes_dict[target_id]["type"]
        supported_node_types = schema[source_node_type]
        if target_node_type not in supported_node_types:
            raise NotImplementedError("Target node must be one of " + str(supported_node_types.keys()))

        supported_edge_types = supported_node_types[target_node_type]
        edge_id = edge["edge_id"]
        edge_type = edge["type"]
        if edge_type not in supported_edge_types:
            raise NotImplementedError("Edge must be one of " + str(supported_edge_types))

        def supported_type(feature_matrix):
            return feature_matrix["feature_b"]["biolink_class"] in subtypes[target_node_type]

        def feature_properties(feature_matrix):
            return {
                "feature_name": feature_matrix["feature_b"]["feature_name"],
                "p_value": feature_matrix["p_value"],
                "biolink_class": feature_matrix["feature_b"]["biolink_class"]
            }

        cohort_id, size = get_ids_by_feature(conn, table, year, cohort_features)

        ataf = select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value)

        feature_list = list(map(feature_properties, filter(supported_type, ataf)))

        def result(feature_property):
            node_name = feature_property["feature_name"]
            node_ids = get_identifiers(table, node_name)
            def result2(node_id):
                return {
                    "node_bindings" : {
                        source_id: cohort_id,
                        target_id: node_id
                    },
                    "edge_bindings" : {
                        edge_id: [cohort_id + "_" + node_id]
                    },
                    "score": feature_property["p_value"],
                    "score_name": "p value"
                }
            return list(map(result2, node_ids))

        def knowledge_graph_node(feature_property):
            node_name = feature_property["feature_name"]
            node_ids = get_identifiers(table, node_name)
            def knowledge_graph_node2(node_id):
                return {
                    "name": node_name,
                    "id": node_id,
                    "type": feature_property["biolink_class"]
                }
            return list(map(knowledge_graph_node2, node_ids))

        def knowledge_graph_edge(feature_property):
            edge_name = "association"
            node_ids = get_identifiers(table, feature_property["feature_name"])
            def knowledge_graph_edge2(node_id):
                return {
                    "type": edge_name,
                    "id": cohort_id + "_" + node_id,
                    "source_id": cohort_id,
                    "target_id": node_id
                }
            return list(map(knowledge_graph_edge2, node_ids))

        knowledge_graph_nodes = [{
            "name": "cohort",
            "id": cohort_id,
            "type": "population_of_individual_organisms"
        }] + list(itertools.chain.from_iterable(map(knowledge_graph_node, feature_list)))

        knowledge_graph_edges = list(itertools.chain.from_iterable(map(knowledge_graph_edge, feature_list)))

        knowledge_graph = {
            "nodes": knowledge_graph_nodes,
            "edges": knowledge_graph_edges
        }
        
        message = {
            "reasoner_id": "ICEES",
            "tool_version": "2.0.0",
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "n_results": sum(map(lambda x : len(get_identifiers(table, x["feature_name"])), feature_list)),
            "message_code": "OK",
            "code_description": "",
            # "query_graph": query_graph,
            "question_graph": query_graph,
            "knowledge_graph": knowledge_graph,
            # "results": list(map(result, feature_list))
            "answers": list(map(result, feature_list))
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
    return schema
