"""Knowledge graph methods."""
import datetime
from functools import reduce
from itertools import combinations
import logging
import os
import re
import traceback
from typing import List
import uuid

import tx.functional.maybe as maybe
from tx.functional.maybe import Nothing, Just
from tx.functional.utils import compose

from ..utils import to_qualifiers
from .mappings import mappings
from .data_sources import data_sources
from .sql import get_ids_by_feature, select_associations_to_all_features, select_feature_matrix, get_feature_levels
from .identifiers import get_identifiers, get_features_by_identifier
from .qgraph_utils import normalize_qgraph

INFORES_CURIE = os.environ.get("ICEES_INFORES_CURIE", "infores:icees-asthma")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

schema = {
    "biolink:PopulationOfIndividualOrganisms": {
        "biolink:ChemicalSubstance": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:Disease": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:PhenotypicFeature": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:DiseaseOrPhenotypic_feature": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:ChemicalSubstance": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:Environment": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:ActivityAndBehavior": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:Drug": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ],
        "biolink:NamedThing": [
            "biolink:correlated_with",
            "biolink:has_real_world_evidence_of_association_with",
        ]
    }
}

subtypes = {
    "biolink:ChemicalSubstance": ["biolink:Drug"],
    "biolink:DiseaseOrPhenotypicFeature": ["biolink:Disease", "biolink:PhenotypicFeature"],
    "biolink:NamedThing": [
        "biolink:ChemicalSubstance",
        "biolink:DiseaseOrPhenotypicFeature",
        "biolink:Environment",
    ]
}

TOOL_VERSION = "6.0.0"


def closure_subtype(node_type):
    return reduce(
        lambda x, y : x + y,
        map(
            closure_subtype,
            subtypes.get(node_type, [])
        ),
        [node_type],
    )


def name_to_ids(table, filter_regex, node_name):
    return list(dict.fromkeys(filter(
        lambda x: re.match(filter_regex, x),
        get_identifiers(table, node_name, True)
    )))


def gen_node_id_and_equivalent_ids(node_ids):
    id_prefixes = [
        "CHEBI",
        "CHEMBL",
        "DRUGBANK",
        "PUBCHEM",
        "MESH",
        "HMDB",
        "INCHI",
        "INCHIKEY",
        "UNII",
        "KEGG",
        "gtpo",
    ]
    inode_id = next((
        (i, node_id)
        for i, node_id in enumerate(node_ids)
        if any(
            node_id.upper().startswith(x.upper() + ":")
            for x in id_prefixes
        )
    ), None)
    if inode_id is None:
        return node_ids[0], node_ids[1:]
    i, node_id = inode_id
    return node_id, node_ids[:i] + node_ids[i+1:]


def result(
        source_id,
        source_curie,
        qedge_id,
        kedge_ids,
        target_curie,
        target_id,
        score,
        score_name,
):
    return {
        "node_bindings": {
            source_id: [{"id": source_curie}],
            target_id: [{"id": target_curie}],
        },
        "edge_bindings": {
            qedge_id: [
                {
                    "id": kedge_id,
                }
                for kedge_id in kedge_ids
            ]
        },
        "score": score,
        "score_name": score_name
    }


def knowledge_graph_node(node_name, table, filter_regex, biolink_class):
    node_ids = name_to_ids(table, filter_regex, node_name)
    if len(node_ids) == 0:
        raise ValueError(f"No identifiers for {node_name}")
    node_id, equivalent_ids = gen_node_id_and_equivalent_ids(node_ids)

    return node_id, {
        "name": node_name,
        "attributes": [
            {
                "attribute_type_id": "biolink:synonym",
                "value": equivalent_ids,
            }
        ],
        "categories": [biolink_class],
    }


def knowledge_graph_edges(
        subject_id: str,
        object_id: str,
        feature_property,
):
    source_attributes = [
        {
            "attribute_type_id": "biolink:supporting_data_source",
            "value": data_source["name"],
            "value_url": data_source.get("url", None),
        }
        for data_source in data_sources
    ] + [
        {
            "attribute_type_id": "biolink:original_knowledge_source",
            "value": INFORES_CURIE,
        }
    ]

    return {
        f"{subject_id}_{object_id}_{str(uuid.uuid4())[:8]}": {
            "predicate": "biolink:correlated_with",
            "subject": subject_id,
            "object": object_id,
            "attributes": [{
                "attribute_type_id": "contingency:matrices",
                "value": feature_property
            }] + source_attributes,
        },
        f"{subject_id}_{object_id}_{str(uuid.uuid4())[:8]}": {
            "predicate": "biolink:has_real_world_evidence_of_association_with",
            "subject": subject_id,
            "object": object_id,
            "attributes": [{
                "attribute_type_id": "contingency:matrices",
                "value": feature_property
            }] + source_attributes,
        },
    }


def get(conn, query, verbose=False):
    message = query.get("message", query)
    query_options = query.get("query_options", {})
    cohort_id, table, year, cohort_features, size = message_cohort(conn, query_options)
    maximum_p_value = query["query_options"].get("maximum_p_value", MAX_P_VAL_DEFAULT)
    filter_regex = query["query_options"].get("regex", ".*")
    feature = to_qualifiers(query["query_options"]["feature"])

    query_graph = message.get("query_graph", message.get("machine_question"))

    nodes_dict = query_graph["nodes"]
    edges_dict = query_graph["edges"]

    if len(nodes_dict) != 2:
        raise NotImplementedError("Number of nodes in query graph must be 2")

    if len(edges_dict) != 1:
        raise NotImplementedError("Number of edges in query graph must be 1")

    # nodes_dict = {node_get_id(node): node for node in nodes}
    edge_id, edge = next(iter(edges_dict.items()))

    source_id = edge["subject"]
    source_node = nodes_dict[source_id]
    source_node_types = source_node["categories"]
    source_node_type = source_node_types[0]
    source_curie = cohort_id

    if source_node_type not in schema:
        raise NotImplementedError("Sounce node must be one of " + str(schema.keys()))

    target_id = edge["object"]
    target_node_types = nodes_dict[target_id]["categories"]
    target_node_type = target_node_types[0]
    supported_node_types = schema[source_node_type]
    if target_node_type not in supported_node_types:
        raise NotImplementedError("Target node must be one of " + str(supported_node_types.keys()))

    supported_edge_types = supported_node_types[target_node_type]
    # edge_id = edge_get_id(edge)
    edge_types = edge["predicates"]
    edge_type = edge_types[0]
    if edge_type not in supported_edge_types:
        raise NotImplementedError("Edge must be one of " + str(supported_edge_types))

    cohort_id, size = get_ids_by_feature(conn, table, year, cohort_features)

    supported_types = closure_subtype(target_node_type)

    feature_list = select_associations_to_all_features(
        conn,
        table,
        year,
        cohort_id,
        feature,
        maximum_p_value,
        lambda x: mappings.get(x)["categories"][0] in supported_types,
    )

    nodes = {}
    kedges = {}
    results = []
    for feature in feature_list:
        feature_b = feature["feature_b"]
        feature_name = feature_b["feature_name"]
        biolink_class = feature_b["biolink_class"]
        p_value = feature["p_value"]

        node_id, node = knowledge_graph_node(
            feature_name,
            table,
            filter_regex,
            biolink_class,
        )
        nodes[node_id] = node

        node_ids = name_to_ids(table, filter_regex, feature_name)
        if len(node_ids) == 0:
            raise ValueError(f"No identifiers for {feature_name}")
        object_id, equivalent_ids = gen_node_id_and_equivalent_ids(node_ids)
        new_kedges = knowledge_graph_edges(
            source_curie,
            object_id,
            feature,
        )
        kedges.update(new_kedges)

        result_ = result(
            source_id,
            cohort_id,
            edge_id,
            list(new_kedges.keys()),
            object_id,
            target_id,
            p_value,
            "p value",
        )
        results.append(result_)

    nodes[cohort_id] = {
        "name": "cohort",
        "categories": ["biolink:PopulationOfIndividualOrganisms"]
    }

    knowledge_graph = {
        "nodes": nodes,
        "edges": kedges,
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

    return message


def query_feature(conn, table_name, feature):
    """Generate feature specification."""
    levels = get_feature_levels(feature)
    return {
        "feature_name": feature,
        "feature_qualifiers": [{
            "operator": "=",
            "value": v
        } for v in levels]
    }


def co_occurrence_feature_edge(
        conn,
        table,
        year,
        cohort_features,
        src_feature,
        tgt_feature,
):
    """Get co-occurrence p-value."""
    return select_feature_matrix(
        conn, table, year, cohort_features, year,
        query_feature(conn, table, src_feature),
        query_feature(conn, table, tgt_feature),
    )["p_value"]


def feature_names(table, node_curie):
    """Get feature names from concept curie."""
    return get_features_by_identifier(table, node_curie)


def co_occurrence_edge(
        conn,
        table_name: str,
        year,
        cohort_features,
        src_node,
        tgt_node,
):
    def handle_src_and_tgt_features(src_features, tgt_features):
        edge_property_value = []
        for src_feature in src_features:
            for tgt_feature in tgt_features:
                edge = co_occurrence_feature_edge(
                    conn,
                    table_name,
                    year,
                    cohort_features,
                    src_feature,
                    tgt_feature,
                )
                edge_property_value.append({
                    "src_feature": src_feature,
                    "tgt_feature": tgt_feature,
                    "p_value": edge
                })
        if len(edge_property_value) == 0:
            raise RuntimeError("no edge found")
        return edge_property_value

    src_features = feature_names(table_name, src_node)
    tgt_features = feature_names(table_name, tgt_node)
    columns = conn.tables[table_name].columns
    src_features = [
        feature for feature in src_features
        if feature in columns
    ]
    tgt_features = [
        feature for feature in tgt_features
        if feature in columns
    ]
    return handle_src_and_tgt_features(src_features, tgt_features)


def generate_edge_id(src_node, tgt_node):
    """Generate edge id."""
    return src_node + "_" + tgt_node + "_" + str(uuid.uuid4())[:8]


def node_get_id(node):
    node_id = node.get("id")
    return node_id if node_id is not None else node.get("node_id")


def edge_get_id(node):
    edge_id = node.get("id")
    return edge_id if edge_id is not None else node.get("edge_id")


def attr(s):
    return lambda d: maybe.from_python(d.get(s))


def generate_edges(src_node, tgt_node, edge_attributes=None):
    return {
        generate_edge_id(src_node, tgt_node): {
            "predicate": "biolink:correlated_with",
            "subject": src_node,
            "object": tgt_node,
            **({
                "edge_attributes": edge_attributes
            } if edge_attributes is not None else {})
        },
        generate_edge_id(src_node, tgt_node): {
            "predicate": "biolink:has_real_world_evidence_of_association_with",
            "subject": src_node,
            "object": tgt_node,
            **({
                "edge_attributes": edge_attributes
            } if edge_attributes is not None else {})
        },
    }


def convert(attribute_map, qnode):
    return {
        k : res.value
        for k, k_qnode in attribute_map.items()
        if isinstance((res := k_qnode(qnode)), Just)
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
    message = query["message"]
    query_options = query.get("query_options", None) or {}

    cohort_id, table, year, features, size = message_cohort(conn, query_options)

    kgraph = message.get("knowledge_graph")

    knodes = kgraph["nodes"]
    kedges = kgraph["edges"]

    nodes = knodes
    edges = kedges

    overlay_edges = dict()
    for src_node, tgt_node in combinations(knodes.keys(), r=2):
        edge_attributes = co_occurrence_edge(conn, table, year, features, src_node, tgt_node)
        overlay_edges.update(generate_edges(
            src_node,
            tgt_node,
            edge_attributes=edge_attributes,
        ))
    knowledge_graph = {
        "nodes": nodes,
        "edges": {**edges, **overlay_edges}
    }

    message = {
        "reasoner_id": "ICEES",
        "tool_version": TOOL_VERSION,
        "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
        "message_code": "OK",
        "code_description": "",
        "knowledge_graph": knowledge_graph,
    }

    return message


def add_node(nodes, node):
    node_id = node_get_id(node)
    node_curr = nodes.get(node_id)
    if node_curr is None:
        nodes[node_id] = node
    else:
        node_curr["name"] += f",{node['name']}"


def type_is_supported(feature: str, supported_types: List[str]) -> bool:
    """Determine whether feature type is supported."""
    return (
        feature in mappings
        and mappings[feature]["categories"][0] in supported_types
    )


def matches_qnode(feature_name, qnode, table):
    """Determine whether feature can be bound to qnode."""
    if "ids" in qnode:
        if not any(
            feature_name in feature_names(table, curie)
            for curie in qnode["ids"]
        ):
            return False
    if "categories" in qnode:
        if not type_is_supported(feature_name, qnode["categories"]):
            return False
    return True


def one_hop(conn, query, verbose=False):
    try:
        message = query["message"]
        query_options = query.get("query_options", None) or {}
        cohort_id, table, year, cohort_features, size = message_cohort(conn, query_options)
        maximum_p_value = query_options.get("maximum_p_value", MAX_P_VAL_DEFAULT)
        filter_regex = query_options.get("regex", ".*")
        query_graph = message["query_graph"]
        normalize_qgraph(query_graph)

        nodes_dict = query_graph["nodes"]
        edges_dict = query_graph["edges"]

        if len(nodes_dict) != 2:
            raise NotImplementedError("Number of nodes in query graph must be 2")

        if len(edges_dict) != 1:
            raise NotImplementedError("Number of edges in query graph must be 1")

        predicates = next(iter(edges_dict.values()))["predicates"]
        if (
            "biolink:correlated_with" not in predicates and
            "biolink:has_real_world_evidence_of_association_with" not in predicates
        ):
            return {
                "reasoner_id": "ICEES",
                "tool_version": TOOL_VERSION,
                "datetime": datetime.datetime.now().strftime("%Y-%m-%D %H:%M:%S"),
                "n_results": 0,
                "message_code": "OK",
                "code_description": "",
                "query_graph": query_graph,
                "knowledge_graph": {"nodes": {}, "edges": {}},
                "results": [],
            }

        edge_id, edge = next(iter(edges_dict.items()))

        source_id = edge["subject"]
        source_node = nodes_dict[source_id]

        target_id = edge["object"]
        target_node = nodes_dict[target_id]

        def p_values(feature_list):
            return [feature["p_value"] for feature in feature_list]

        knowledge_graph_nodes = dict()
        kedges = dict()
        results = []

        ataf = select_associations_to_all_features(
            conn,
            table,
            year,
            cohort_id,
            feature_filter_a=lambda name: matches_qnode(name, source_node, table),
            maximum_p_value=maximum_p_value,
            feature_filter_b=lambda name: matches_qnode(name, target_node, table),
        )
        for assoc in ataf:
            try:
                knode_a_id, knode_a = knowledge_graph_node(
                    assoc["feature_a"]["feature_name"],
                    table,
                    filter_regex,
                    assoc["feature_a"]["biolink_class"],
                )
            except ValueError:
                continue

            knowledge_graph_nodes[knode_a_id] = knode_a

            try:
                knode_b_id, knode_b = knowledge_graph_node(
                    assoc["feature_b"]["feature_name"],
                    table,
                    filter_regex,
                    assoc["feature_b"]["biolink_class"],
                )
            except ValueError:
                continue

            knowledge_graph_nodes[knode_b_id] = knode_b

            new_kedges = knowledge_graph_edges(
                knode_a_id,
                knode_b_id,
                [assoc],
            )
            kedges.update(new_kedges)

            item = result(
                source_id,
                knode_a_id,
                edge_id,
                list(new_kedges.keys()),
                knode_b_id,
                target_id,
                p_values([assoc])[0],
                "p value",
            )
            results.append(item)
            
        knowledge_graph = {
            "nodes": knowledge_graph_nodes,
            "edges": kedges,
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
            "query_graph": query_graph            
        }

    return message


def get_schema():
    return schema
