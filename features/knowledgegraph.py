import json
import os
import datetime
import traceback
import itertools
from functools import reduce, partial
import re
import logging
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, func, Sequence, between
from sqlalchemy.sql import select
import numpy as np
import inflection
from tx.functional.either import Left, Right
from tx.functional.maybe import Nothing, Just
import tx.functional.maybe as maybe
from tx.functional.utils import compose
from utils import to_qualifiers
from .features import features, lookUpFeatureClass, features_dict
from .model import get_ids_by_feature, select_associations_to_all_features, select_feature_matrix
from .identifiers import get_identifiers, get_features_by_identifier

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schema = {
        "population_of_individual_organisms": {
            "chemical_substance": ["correlated_with"],
            "disease": ["correlated_with"],
            "phenotypic_feature": ["correlated_with"],
            "disease_or_phenotypic_feature": ["correlated_with"],
            "chemical_substance": ["correlated_with"],
            "environment": ["correlated_with"],
            "activity_and_behavior": ["correlated_with"],
            "drug": ["correlated_with"],
            "named_thing": ["correlated_with"]
        }
    }

subtypes = {
    "chemical_substance": ["drug"],
    "disease_or_phenotypic_feature": ["disease", "phenotypic_feature"],
    "named_thing": ["chemical_substance", "disease_or_phenotypeic_feature", "environment"]
}

TOOL_VERSION = "4.0.0"

def closure_subtype(node_type):
    return reduce(lambda x, y : x + y, map(closure_subtype, subtypes.get(node_type, [])), [node_type])


def name_to_ids(table, filter_regex, node_name):
    return list(dict.fromkeys(filter(lambda x: re.match(filter_regex, x), get_identifiers(table, node_name, True))))

def gen_edge_id(cohort_id, node_name, node_id):
    return cohort_id + "_" + node_name + "_" + node_id

def gen_node_id_and_equivalent_ids(node_ids):
    id_prefixes = ["CHEBI", "CHEMBL", "DRUGBANK", "PUBCHEM", "MESH", "HMDB", "INCHI", "INCHIKEY", "UNII", "KEGG", "gtpo"]
    inode_id = next(((i, node_id) for i, node_id in enumerate(node_ids) if any(node_id.upper().startswith(x.upper() + ":") for x in id_prefixes)), None)
    if inode_id is None:
        return node_ids[0], node_ids[1:]
    else:
        i, node_id = inode_id
        return node_id, node_ids[:i] + node_ids[i+1:]
    
def result(source_id, source_curie, edge_id, node_name, target_id, table, filter_regex, score, score_name):
    node_ids = name_to_ids(table, filter_regex, node_name)
    if len(node_ids) == 0:
        return Nothing
    else:
        node_id, *equivalent_ids = gen_node_id_and_equivalent_ids(node_ids)

        return Just({
            "node_bindings" : [
                {"qg_id": source_id, "kg_id": source_curie},
                {"qg_id": target_id, "kg_id": node_id}
            ],
            "edge_bindings" : [
                {"qg_id": edge_id, "kg_id": gen_edge_id(source_curie, node_name, node_id)}
            ],
            "score": score,
            "score_name": score_name
        })

def knowledge_graph_node(node_name, table, filter_regex, biolink_class):
    node_ids = name_to_ids(table, filter_regex, node_name)
    if len(node_ids) == 0:
        return Nothing
    else:
        node_id, equivalent_ids = gen_node_id_and_equivalent_ids(node_ids)

        return Just({
            "name": node_name,
            "id": node_id,
            "equivalent_identifiers": equivalent_ids,
            "type": [biolink_class]
        })
    

def knowledge_graph_edge(source_id, node_name, table, filter_regex, feature_property):
    node_ids = name_to_ids(table, filter_regex, node_name)
    if len(node_ids) == 0:
        return Nothing
    else:
        node_id, *equivalent_ids = gen_node_id_and_equivalent_ids(node_ids)
        
        edge_name = "correlated_with"
        
        return Just({
            "type": edge_name,
            "id": gen_edge_id(source_id, node_name, node_id),
            "source_id": source_id,
            "target_id": node_id,
            "edge_attributes": feature_property
        })
    

def get(conn, query):
    try:
        message = query.get("message", query)
        query_options = query.get("query_options", {})
        cohort_id, table, year, cohort_features, size = message_cohort(conn, query_options)
        maximum_p_value = query["query_options"].get("maximum_p_value", MAX_P_VAL_DEFAULT)
        filter_regex = query["query_options"].get("regex", ".*")
        feature = to_qualifiers(query["query_options"]["feature"])

        query_graph = message.get("query_graph", message.get("machine_question"))

        nodes = query_graph["nodes"]
        edges = query_graph["edges"]

        if len(nodes) != 2:
            raise NotImplementedError("Number of nodes in query graph must be 2")

        if len(edges) != 1:
            raise NotImplementedError("Number of edges in query graph must be 1")

        nodes_dict = {node_get_id(node): node for node in nodes}
        [edge] = edges

        source_id = edge["source_id"]
        source_node = nodes_dict[source_id]
        source_node_type = source_node["type"]
        source_curie = cohort_id

        if source_node_type not in schema:
            raise NotImplementedError("Sounce node must be one of " + str(schema.keys()))

        target_id = edge["target_id"]
        target_node_type = nodes_dict[target_id]["type"]
        supported_node_types = schema[source_node_type]
        if target_node_type not in supported_node_types:
            raise NotImplementedError("Target node must be one of " + str(supported_node_types.keys()))

        supported_edge_types = supported_node_types[target_node_type]
        edge_id = edge_get_id(edge)
        edge_type = edge["type"]
        if edge_type not in supported_edge_types:
            raise NotImplementedError("Edge must be one of " + str(supported_edge_types))

        cohort_id, size = get_ids_by_feature(conn, table, year, cohort_features)

        supported_types = closure_subtype(target_node_type)

        feature_list = select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value, lambda x : inflection.underscore(x.biolink_class) in supported_types)
        logger.info(f"feature_list = {feature_list}")

        nodes = {}
        knowledge_graph_edges = []
        results = []
        for feature in feature_list:
            feature_b = feature["feature_b"]
            feature_name = feature_b["feature_name"]
            biolink_class = feature_b["biolink_class"]
            p_value = feature["p_value"]
            
            knowledge_graph_node(feature_name, table, filter_regex, biolink_class).bind(lambda node: add_node(nodes, node))

            knowledge_graph_edge(source_curie, feature_name, table, filter_regex, feature).bind(lambda edge: knowledge_graph_edges.append(edge))

            result(source_id, cohort_id, edge_id, feature_name, target_id, table, filter_regex, p_value, "p value").bind(lambda item: results.append(item))
            
        knowledge_graph_nodes = [{
            "name": "cohort",
            "id": cohort_id,
            "type": ["population_of_individual_organisms"]
        }] + list(nodes.values())

        knowledge_graph = {
            "nodes": knowledge_graph_nodes,
            "edges": knowledge_graph_edges
        }

        n_results = len(results)
        
        message = {
            "reasoner_id": "ICEES",
            "tool_version": TOOL_VERSION,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "n_results": n_results,
            "message_code": "OK",
            "code_description": "",
            "query_graph": query_graph,
            "knowledge_graph": knowledge_graph,
            "results": results
        }
    except Exception as e:
        traceback.print_exc()
        message = {
            "reasoner_id": "ICEES",
            "tool_version": TOOL_VERSION,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "message_code": "Error",
            "code_description": str(e),
        }

    return message


def query_feature(table, feature):
    feature_def = features_dict[table][feature]
    ty = feature_def["type"]
    if ty == "string":
        if "enum" not in feature_def:
            return Left("node has type string but has no enum")
        else:
            return Right({
                "feature_name": feature,
                "feature_qualifiers": [{
                    "operator":"=",
                    "value":v
                } for v in feature_def["enum"]]
            })
    elif ty == "integer":
        if "maximum" not in feature_def or "minimum" not in feature_def:
            return Left("node has type integer but has no maximum or has no minimum")
        else:
            return Right({
                "feature_name": feature,
                "feature_qualifiers": [{
                    "operator":"=",
                    "value":v
                } for v in range(feature_def["minimum"], feature_def["maximum"]+1)]
            })
    else:
        return Left(f"unsupported node type {ty}")

    
def co_occurrence_feature_edge(conn, table, year, cohort_features, src_feature, tgt_feature):
    return (
        query_feature(table, src_feature)
        .bind(lambda src_query_feature: (
            query_feature(table, tgt_feature)
            .map(lambda tgt_query_feature: (
                select_feature_matrix(conn, table, year, cohort_features, year, src_query_feature, tgt_query_feature)["p_value"]
            ))
        ))
    )


def feature_names(table, node_curie):
    return (
        maybe.from_python(node_curie)
        .rec(Right, Left("no curie specified at node"))
        .bind(partial(get_features_by_identifier, table))
    )


def co_occurrence_edge(conn, table, year, cohort_features, src_node, tgt_node):
    def handle_src_and_tgt_features(src_features, tgt_features):
        edge_property_value = []
        for src_feature in src_features:
            for tgt_feature in tgt_features:
                edge = co_occurrence_feature_edge(conn, table, year, cohort_features, src_feature, tgt_feature)
                if isinstance(edge, Right):
                    edge_property_value.append({
                        "src_feature": src_feature,
                        "tgt_feature": tgt_feature,
                        "p_value": edge.value
                    })
                else:
                    return edge
        if len(edge_property_value) == 0:
            return Left("no edge found")
        else:
            return Right(edge_property_value)
                    
    return (
        feature_names(table, src_node["id"])
        .bind(lambda src_features: (
            feature_names(table, tgt_node["id"])
            .bind(lambda tgt_features: (
                handle_src_and_tgt_features(src_features, tgt_features)  
            ))
        ))
    )
            

def generate_edge_id(src_node, tgt_node):
    return node_get_id(src_node) + "_" + node_get_id(tgt_node)


def node_get_id(node):
    node_id = node.get("id")
    return node_id if node_id is not None else node.get("node_id")


def edge_get_id(node):
    edge_id = node.get("id")
    return edge_id if edge_id is not None else node.get("edge_id")


def attr(s):
    return lambda d: maybe.from_python(d.get(s))

    
def generate_edge(src_node, tgt_node, edge_attributes=None):
    return {
        "id": generate_edge_id(src_node, tgt_node),
        "type": "correlated_with",
        "source_id": node_get_id(src_node),
        "target_id": node_get_id(tgt_node),
        **({
           "edge_attributes": edge_attributes
        } if edge_attributes is not None else {})
    }


def convert(attribute_map, qnode):
    return {
        k : res.value for k, k_qnode in attribute_map.items() if isinstance((res := k_qnode(qnode)), Just)
    }


def convert_qnode_to_node(qnode):
    attribute_map = {
        "id": attr("curie"),
        "type": attr("type")
    }
    return convert(attribute_map, qnode)


def convert_qedge_to_edge(qedge):
    attribute_map = {
        "id": compose(edge_get_id, Just),
        "type": attr("type"),
        "relation": attr("relation"),
        "source_id": attr("source_id"),
        "target_id": attr("target_id"),
        "negated": attr("negated")
    }
    return convert(attribute_map, qedge)
    

def message_cohort(conn, cohort_definition):
    cohort_id = cohort_definition.get("cohort_id")
    if cohort_id is None:
        table = cohort_definition.get("table", "patient")
        year = cohort_definition.get("year")
        features = cohort_definition.get("cohort_features", {})
        cohort_id, size = get_ids_by_feature(conn, table, year, features)
    else:
        cohort_definition = get_cohort_definition_by_id(cohort_id)
        if cohort_definition is Nothing:
            raise RuntimeError("cohort with cohort_id not found")
        else:
            table = cohort_definition["table"]
            year = cohort_definition["year"]
            features = cohort_defintion["features"]
            size = cohort_definition["size"]

    return cohort_id, table, year, features, size


MAX_P_VAL_DEFAULT = 1

def co_occurrence_overlay(conn, query):
    try:
        message = query["message"]
        query_options = query.get("query_options", {})

        cohort_id, table, year, features, size = message_cohort(conn, query_options)
        
        kgraph = message.get("knowledge_graph")

        knodes = kgraph["nodes"]
        kedges = kgraph["edges"]

        nodes = knodes
        edges = kedges

        overlay_edges = []
        for src_node in knodes:
            for tgt_node in knodes:
                edge_attributes = co_occurrence_edge(conn, table, year, features, src_node, tgt_node)
                if isinstance(edge_attributes, Left):
                    return {
                        "reasoner_id": "ICEES",
                        "tool_version": TOOL_VERSION,
                        "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
                        "message_code": "Error",
                        "code_description": edge_attributes.value,
                    }
                else:
                    overlay_edges.append(generate_edge(src_node, tgt_node, edge_attributes=edge_attributes.value))
        knowledge_graph = {
            "nodes": nodes,
            "edges": edges + overlay_edges
        }
        
        message = {
            "reasoner_id": "ICEES",
            "tool_version": TOOL_VERSION,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "message_code": "OK",
            "code_description": "",
            "knowledge_graph": knowledge_graph,
        }
    except Exception as e:
        traceback.print_exc()
        message = {
            "reasoner_id": "ICEES",
            "tool_version": TOOL_VERSION,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "message_code": "Error",
            "code_description": str(e),
        }

    return message


def add_node(nodes, node):
    node_id = node_get_id(node)
    node_curr = nodes.get(node_id)
    if node_curr is None:
        nodes[node_id] = node
    else:
        node_curr["name"] += f",{node['name']}"

    
def one_hop(conn, query):
    try:
        message = query["message"]
        query_options = query.get("query_options", {})
        cohort_id, table, year, cohort_features, size = message_cohort(conn, query_options)
        maximum_p_value = query.get("query_options", {}).get("maximum_p_value", MAX_P_VAL_DEFAULT)
        filter_regex = query.get("query_options", {}).get("regex", ".*")
        query_graph = message["query_graph"]

        nodes = query_graph["nodes"]
        edges = query_graph["edges"]

        if len(nodes) != 2:
            raise NotImplementedError("Number of nodes in query graph must be 2")

        if len(edges) != 1:
            raise NotImplementedError("Number of edges in query graph must be 1")

        nodes_dict = {node_get_id(node): node for node in nodes}
        [edge] = edges

        source_id = edge["source_id"]
        source_node = nodes_dict[source_id]
        source_node_type = source_node.get("type")
        source_curie = source_node["curie"]

        msource_node_feature_names = feature_names(table, source_curie)
        if isinstance(msource_node_feature_names, Left):
            raise NotImplementedError(msource_node_feature_names)
        else:
            source_node_feature_names = msource_node_feature_names.value

        target_id = edge["target_id"]
        target_node_type = nodes_dict[target_id]["type"]

        edge_id = edge_get_id(edge)

        feature_set = {}
        supported_types = closure_subtype(target_node_type)

        for source_node_feature_name in source_node_feature_names:
            feature = query_feature(table, source_node_feature_name).value
            ataf = select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value, feature_set=lambda x : inflection.underscore(x.biolink_class) in supported_types)
            for feature in ataf:
                feature_name = feature["feature_b"]["feature_name"]
                biolink_class = feature["feature_b"]["biolink_class"]
                if feature_name in feature_set:
                    _, feature_properties = feature_set[feature_name]
                    feature_properties.append(feature)
                else:
                    feature_set[feature_name] = biolink_class, [feature]

        nodes = {}
        knowledge_graph_edges = []
        results = []

        def p_values(feature_list):
            return [feature["p_value"] for feature in feature_list]
        
        for feature_name, (biolink_class, feature_list) in feature_set.items():
            knowledge_graph_node(feature_name, table, filter_regex, biolink_class).bind(lambda node: add_node(nodes, node))

            knowledge_graph_edge(source_curie, feature_name, table, filter_regex, feature_list).bind(lambda edge: knowledge_graph_edges.append(edge))
    
            result(source_id, source_curie, edge_id, feature_name, target_id, table, filter_regex, p_values(feature_list), "p value").bind(lambda item: results.append(item))

        knowledge_graph_nodes = [convert_qnode_to_node(source_node), *nodes.values()]
            
        knowledge_graph = {
            "nodes": knowledge_graph_nodes,
            "edges": knowledge_graph_edges
        }

        n_results = len(results)

        message = {
            "reasoner_id": "ICEES",
            "tool_version": TOOL_VERSION,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "n_results": n_results,
            "message_code": "OK",
            "code_description": "",
            "query_graph": query_graph,
            "knowledge_graph": knowledge_graph,
            "results": results
        }
        
    except Exception as e:
        traceback.print_exc()
        message = {
            "reasoner_id": "ICEES",
            "tool_version": TOOL_VERSION,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
            "message_code": "Error",
            "code_description": traceback.format_exc(),
        }

    return message


def get_schema():
    return schema
