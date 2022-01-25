"""ICEES API handlers."""
from collections import defaultdict
import copy
import os
import json
from typing import Dict, Union, Optional

from fastapi import APIRouter, Body, Depends, Security, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from reasoner_pydantic import Query, Message
from sqlalchemy.sql.expression import table
from starlette.status import HTTP_403_FORBIDDEN

from .dependencies import get_db
from .features import knowledgegraph, sql
from .features.identifiers import get_identifiers, input_dict
from .features.qgraph_utils import normalize_qgraph
from .features.sql import validate_range
from .features.mappings import mappings, correlations
from .features.config import get_config_path
from .models import (
    Features,
    FeatureAssociation, FeatureAssociation2,
    AllFeaturesAssociation, AllFeaturesAssociation2,
    AddNameById,
)
from .utils import to_qualifiers, to_qualifiers2, associations_have_feature_matrices


API_KEY = os.environ.get("API_KEY")
API_KEY_NAME = os.environ.get("API_KEY_NAME")
COOKIE_DOMAIN = os.environ.get("COOKIE_DOMAIN")
TABLES = ("patient", "visit")


def validate_table(table_name):
    """Validate table name."""
    if table_name not in TABLES:
        raise HTTPException(400, f"Invalid table '{table_name}'")


if API_KEY is None:
    async def get_api_key():
        return None
else:
    api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
    api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
    api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)

    async def get_api_key(
        api_key_query: str = Security(api_key_query),
        api_key_header: str = Security(api_key_header),
        api_key_cookie: str = Security(api_key_cookie),
    ):

        if api_key_query == API_KEY:
            return api_key_query
        elif api_key_header == API_KEY:
            return api_key_header
        elif api_key_cookie == API_KEY:
            return api_key_cookie
        else:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
            )

        
ROUTER = APIRouter()


@ROUTER.post("/{table}/cohort", response_model=Dict)
def discover_cohort(
        table: str,
        req_features: Features = Body(..., example={}),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Cohort discovery."""
    validate_table(table)
    cohort_id, size = sql.get_ids_by_feature(
        conn,
        table,
        None,
        req_features,
    )

    if size == -1:
        return_value = (
            "Input features invalid or cohort ≤10 patients. "
            "Please try again."
        )
    else:
        return_value = {
            "cohort_id": cohort_id,
            "size": size
        }
    return {"return value": return_value}


@ROUTER.get(
    "/{table}/cohort/dictionary",
    response_model=Dict,
)
def dictionary(
        table: str,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Get cohort dictionary."""
    validate_table(table)
    return_value = sql.get_cohort_dictionary(conn, table, None)
    return {"return value": return_value}


@ROUTER.put("/{table}/cohort/{cohort_id}", response_model=Dict)
def edit_cohort(
        table: str,
        cohort_id: str,
        req_features: Features = Body(..., example={}),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Cohort discovery."""
    validate_table(table)
    cohort_id, size = sql.select_cohort(
        conn,
        table,
        None,
        req_features,
        cohort_id,
    )

    if size == -1:
        return_value = (
            "Input features invalid or cohort ≤10 patients. "
            "Please try again."
        )
    else:
        return_value = {
            "cohort_id": cohort_id,
            "size": size
        }
    return {"return value": return_value}


@ROUTER.get("/{table}/cohort/{cohort_id}", response_model=Dict)
def get_cohort(
        table: str,
        cohort_id: str,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Get definition of a cohort."""
    validate_table(table)
    cohort_features = sql.get_cohort_by_id(
        conn,
        table,
        None,
        cohort_id,
    )

    if cohort_features is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        return_value = cohort_features
    return {"return value": return_value}


with open("examples/feature_association.json") as stream:
    FEATURE_ASSOCIATION_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/{table}/cohort/{cohort_id}/feature_association",
    response_model=Dict,
)
def feature_association(
        table: str,
        cohort_id: str,
        year: Optional[str] = None,
        obj: FeatureAssociation = Body(
            ...,
            example=FEATURE_ASSOCIATION_EXAMPLE,
        ),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Hypothesis-driven 2 x 2 feature associations.

    Users select a predefined cohort and two feature variables, and the service
    returns a 2 x 2 feature table with a correspondingChi Square statistic and
    P value.
    """
    validate_table(table)
    feature_a = to_qualifiers(obj["feature_a"])
    feature_b = to_qualifiers(obj["feature_b"])

    cohort_meta = sql.get_features_by_id(conn, table, cohort_id)

    if cohort_meta is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return_value = sql.select_feature_matrix(
            conn,
            table,
            year,
            cohort_features,
            cohort_year,
            feature_a,
            feature_b,
        )
        if not return_value['feature_matrix']:
            return_value = "Empty query result returned. Please try again"
    return {"return value": return_value}


with open("examples/feature_association2.json") as stream:
    FEATURE_ASSOCIATION2_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/{table}/cohort/{cohort_id}/feature_association2",
    response_model=Dict,
)
def feature_association2(
        table: str,
        cohort_id: str,
        year: Optional[str] = None,
        obj: FeatureAssociation2 = Body(
            ...,
            example=FEATURE_ASSOCIATION2_EXAMPLE,
        ),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Hypothesis-driven N x N feature associations.

    Users select a predefined cohort, two feature variables, and bins, which
    can be combined, and the service returns a N x N feature table with a
    corresponding Chi Square statistic and P value.
    """
    validate_table(table)
    feature_a = to_qualifiers2(obj["feature_a"])
    feature_b = to_qualifiers2(obj["feature_b"])
    to_validate_range = obj.get("check_coverage_is_full", False)
    if to_validate_range:
        validate_range(conn, table, feature_a)
        validate_range(conn, table, feature_b)

    cohort_meta = sql.get_features_by_id(conn, table, cohort_id)

    if cohort_meta is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return_value = sql.select_feature_matrix(
            conn,
            table,
            year,
            cohort_features,
            cohort_year,
            feature_a,
            feature_b,
        )
        if not return_value['feature_matrix']:
            return_value = "Empty query result returned. Please try again"

    return {"return value": return_value}


with open("examples/associations_to_all_features.json") as stream:
    ASSOCIATIONS_TO_ALL_FEATURES_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/{table}/cohort/{cohort_id}/associations_to_all_features",
    response_model=Dict,
)
def associations_to_all_features(
        table: str,
        cohort_id: str,
        year: Optional[str] = None,
        obj: AllFeaturesAssociation = Body(
            ...,
            example=ASSOCIATIONS_TO_ALL_FEATURES_EXAMPLE,
        ),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Exploratory 1 X N feature associations.

    Users select a predefined cohort and a feature variable of interest, and
    the service returns a 1 x N feature table with corrected Chi Square
    statistics and associated P values.
    """
    validate_table(table)
    feature = to_qualifiers(obj["feature"])
    maximum_p_value = obj.get("maximum_p_value", 1)
    correction = obj.get("correction")
    return_value = sql.select_associations_to_all_features(
        conn,
        table,
        year,
        cohort_id,
        feature,
        maximum_p_value,
        correction=correction,
    )

    if associations_have_feature_matrices(return_value):
        return {"return value": return_value}
    else:
        return {"return value": "Empty query result returned. Please try again"}


with open("examples/associations_to_all_features2.json") as stream:
    ASSOCIATIONS_TO_ALL_FEATURES2_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/{table}/cohort/{cohort_id}/associations_to_all_features2",
    response_model=Dict,
)
def associations_to_all_features2(
        table: str,
        cohort_id: str,
        year: Optional[str] = None,
        obj: AllFeaturesAssociation2 = Body(
            ...,
            example=ASSOCIATIONS_TO_ALL_FEATURES2_EXAMPLE,
        ),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Exploratory 1 X N feature associations.

    Users select a predefined cohort and a feature variable of interest and
    bins, which can be combined, and the service returns a 1 x N feature table
    with corrected Chi Square statistics and associated P values.
    """
    validate_table(table)
    feature = to_qualifiers2(obj["feature"])
    to_validate_range = obj.get("check_coverage_is_full", False)
    if to_validate_range:
        validate_range(conn, table, feature)
    maximum_p_value = obj["maximum_p_value"]
    correction = obj.get("correction")
    return_value = sql.select_associations_to_all_features(
        conn,
        table,
        year,
        cohort_id,
        feature,
        maximum_p_value,
        correction=correction,
    )
    if associations_have_feature_matrices(return_value):
        return {"return value": return_value}
    else:
        return {"return value": "Empty query result returned. Please try again"}


@ROUTER.get(
    "/{table}/cohort/{cohort_id}/features",
    response_model=Dict,
)
def features(
        table: str,
        cohort_id: str,
        year: Optional[str] = None,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Feature-rich cohort discovery.

    Users select a predefined cohort as the input parameter, and the service
    returns a profile of that cohort in terms of all feature variables.
    """
    validate_table(table)
    cohort_meta = sql.get_features_by_id(conn, table, cohort_id)
    if cohort_meta is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return_value = sql.get_cohort_features(
            conn,
            table,
            year,
            cohort_features,
            cohort_year,
        )

    return {"return value": return_value}


@ROUTER.get(
    "/{table}/{feature}/identifiers",
    response_model=Dict,
)
def identifiers(
        table: str,
        feature: str,
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Feature identifiers."""
    validate_table(table)
    return_value = {
        "identifiers": get_identifiers(table, feature)
    }
    return {"return value": return_value}


@ROUTER.get(
    "/{table}/name/{name}",
    response_model=Dict,
)
def get_name(
        table: str,
        name: str,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Return cohort id associated with name."""
    validate_table(table)
    return_value = sql.get_id_by_name(conn, table, name)
    return {"return value": return_value}


@ROUTER.post(
    "/{table}/name/{name}",
    response_model=Dict,
)
def post_name(
        table: str,
        name: str,
        obj: AddNameById,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Associate name with cohort id."""
    validate_table(table)
    return_value = sql.add_name_by_id(
        conn,
        table,
        name,
        obj["cohort_id"],
    )
    return {"return value": return_value}


with open("examples/knowledge_graph.json") as stream:
    KNOWLEDGE_GRAPH_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/knowledge_graph",
    response_model=Union[Message, Dict],
)
def knowledge_graph(
        obj: Query = Body(..., example=KNOWLEDGE_GRAPH_EXAMPLE),
        reasoner: bool = False,
        verbose: bool = False,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Query for knowledge graph associations between concepts."""
    return_value = knowledgegraph.get(conn, obj, verbose=verbose)

    return_value = {
        "message": {
            "query_graph": return_value.pop("query_graph"),
            "knowledge_graph": return_value.pop("knowledge_graph"),
            "results": return_value.pop("results"),
        },
        **return_value,
    }
    if reasoner:
        return return_value
    return {"return value": return_value}


@ROUTER.get(
    "/knowledge_graph/schema",
    response_model=Dict,
)
def knowledge_graph_schema(
        reasoner: bool = False,
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Query the ICEES clinical reasoner for knowledge graph schema."""
    return_value = knowledgegraph.get_schema()
    if reasoner:
        return return_value
    return {"return value": return_value}


@ROUTER.get(
    "/meta_knowledge_graph",
    tags=["trapi"],
)
def predicates(
        api_key: APIKey = Depends(get_api_key),
):
    """Get meta-knowledge graph."""
    all_categories = set()
    id_prefixes = defaultdict(set)
    for feature in mappings:
        categories = mappings[feature]["categories"]
        all_categories.update(categories)
        identifiers = input_dict["patient"][feature]
        for category in categories:
            for identifier in identifiers:
                id_prefixes[category].add(identifier.split(":")[0])
    id_prefixes = {
        key: list(value)
        for key, value in id_prefixes.items()
    }
    return {
        "nodes": {
            category: {"id_prefixes": prefixes}
            for category, prefixes in id_prefixes.items()
        },
        "edges": [
            {
                "subject": sub,
                "object": obj,
                "predicate": "biolink:correlated_with",
            }
            for sub in all_categories for obj in all_categories
        ] + [
            {
                "subject": sub,
                "object": obj,
                "predicate": "biolink:has_real_world_evidence_of_association_with",
            }
            for sub in all_categories for obj in all_categories
        ],
    }


with open("examples/knowledge_graph_overlay.json") as stream:
    KG_OVERLAY_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/knowledge_graph_overlay",
    response_model=Union[Message, Dict],
)
def knowledge_graph_overlay(
        obj: Query = Body(..., example=KG_OVERLAY_EXAMPLE),
        reasoner: bool = False,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Query for knowledge graph co-occurrence overlay."""
    return_value = knowledgegraph.co_occurrence_overlay(
        conn,
        obj,
    )

    return_value = {
        "message": {
            "query_graph": obj["message"].get("query_graph", None),
            "knowledge_graph": return_value.pop("knowledge_graph"),
            "results": obj["message"].get("results", None),
        },
        **return_value,
    }
    if reasoner:
        return return_value
    return {"return value": return_value}


with open("examples/knowledge_graph_one_hop.json") as stream:
    KG_ONEHOP_EXAMPLE = json.load(stream)


def knowledge_graph_one_hop(
        obj: Query = Body(..., example=KG_ONEHOP_EXAMPLE),
        reasoner: bool = True,
        verbose: bool = False,
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Query the ICEES clinical reasoner for knowledge graph one hop."""
    if obj.get("workflow", [{"id": "lookup"}]) != [{"id": "lookup"}]:
        raise HTTPException(400, "The only supported workflow is a single 'lookup' operation")
    return_value = knowledgegraph.one_hop(conn, obj, verbose=verbose)

    return_value = {
        "message": {
            "query_graph": return_value.pop("query_graph"),
            "knowledge_graph": return_value.pop("knowledge_graph", None),
            "results": return_value.pop("results", None),
        },
        "workflow": [
            {"id": "lookup"},
        ],
        **return_value,
    }
    if reasoner:
        return return_value
    return {"return value": return_value}


ROUTER.post(
    "/knowledge_graph_one_hop",
    response_model=Dict,
    deprecated=True,
)(knowledge_graph_one_hop)


feature_to_curies = {
    feature: value["identifiers"]
    for feature, value in mappings.items()
}

feature_to_categories = {
    feature: value["categories"]
    for feature, value in mappings.items()
}

curie_to_features = defaultdict(list)
for feature, value in mappings.items():
    for identifier in value["identifiers"]:
        curie_to_features[identifier].append(feature)

category_to_features = defaultdict(list)
for feature, value in mappings.items():
    for category in value["categories"]:
        category_to_features[category].append(feature)


def features_from_node(source_node):
    return [
        feature
        for curie in source_node["ids"]
        for feature in curie_to_features[curie]
    ] if source_node.get("ids") is not None else [
        feature
        for category in source_node["categories"]
        for feature in category_to_features[category]
    ]


# feature_names = correlations[0][1:]
# correlations = [row[1:] for row in correlations[1:]]
# correlations = {
#     tuple(sorted((feature_names[irow], feature_names[icol]))): float(correlations[irow][icol])
#     for irow in range(0, len(correlations))
#     for icol in range(irow + 1, len(correlations))
# }


def knode(source_feature):
    source_curies = feature_to_curies[source_feature]
    source_id, source_synonyms = knowledgegraph.gen_node_id_and_equivalent_ids(source_curies)
    source_categories = feature_to_categories[source_feature]
    return source_id, {
        "name": source_feature,
        "attributes": [
            {
                "attribute_type_id": "biolink:synonym",
                "value": source_synonyms,
            }
        ],
        "categories": source_categories,
    }


def query(
        obj: Query = Body(..., example=KG_ONEHOP_EXAMPLE),
) -> Dict:
    """Solve a one-hop TRAPI query."""
    if obj.get("workflow", [{"id": "lookup"}]) != [{"id": "lookup"}]:
        raise HTTPException(400, "The only supported workflow is a single 'lookup' operation")
    qgraph = copy.deepcopy(obj["message"]["query_graph"])
    normalize_qgraph(qgraph)
    if len(qgraph["nodes"]) != 2:
        raise NotImplementedError("Number of nodes in query graph must be 2")
    if len(qgraph["edges"]) != 1:
        raise NotImplementedError("Number of edges in query graph must be 1")
    qedge_id, qedge = next(iter(qgraph["edges"].items()))
    if (
        "biolink:correlated_with" not in qedge["predicates"] and
        "biolink:has_real_world_evidence_of_association_with" not in qedge["predicates"]
    ):
        return {
            "message": {
                "query_graph": qgraph,
                "knowledge_graph": {"nodes": {}, "edges": {}},
                "results": [],
            }
        }

    source_qid = qedge["subject"]
    source_qnode = qgraph["nodes"][source_qid]
    target_qid = qedge["object"]
    target_qnode = qgraph["nodes"][target_qid]

    # features = correlations[0]
    source_features = features_from_node(source_qnode)
    target_features = features_from_node(target_qnode)
    kedge_pairs = [
        tuple(sorted([source_feature, target_feature]))
        for source_feature in source_features
        for target_feature in target_features
    ]

    kgraph = {
        "nodes": {},
        "edges": {},
    }
    results = []
    for pair in kedge_pairs:
        if pair not in correlations:
            continue
        p_value = correlations[pair]
        source_feature, target_feature = pair  # note the source and target may be flipped, which is okay
        source_kid, source_knode = knode(source_feature)
        target_kid, target_knode = knode(target_feature)
        kgraph["nodes"].update({
            source_kid: source_knode,
            target_kid: target_knode,
        })
        kedges = knowledgegraph.knowledge_graph_edges(source_kid, target_kid, p_value=p_value)
        kgraph["edges"].update(kedges)
        results.append({
            "node_bindings": {
                source_qid: [{"id": source_kid}],
                target_qid: [{"id": target_kid}],
            },
            "edge_bindings": {
                qedge_id: [
                    {
                        "id": kedge_id,
                    }
                    for kedge_id in kedges
                ]
            },
            "score": p_value,
            "score_name": "p value"
        })

    return {
        "message": {
            "query_graph": obj["message"]["query_graph"], # Return unmodified
            "knowledge_graph": kgraph,
            "results": results,
        },
        "workflow": [
            {"id": "lookup"},
        ],
    }
ROUTER.post(
    "/query",
    response_model=Dict,
    tags=["reasoner"],
)(knowledge_graph_one_hop) # Change back to query

@ROUTER.get(
    "/bins",
    response_model=Dict,
)
def handle_bins(
        year: str = None,
        table: str = None,
        feature: str = None,
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Return bin values."""
    input_file = os.path.join(get_config_path(), "bins.json") 
    with open(input_file, "r") as stream:
        bins = json.load(stream)
    if feature is not None:
        bins = {
            year_key: {
                table_key: table_value.get(feature, None)
                for table_key, table_value in year_value.items()
            }
            for year_key, year_value in bins.items()
        }
    if table is not None:
        bins = {
            year_key: year_value.get(table, None)
            for year_key, year_value in bins.items()
        }
    if year is not None:
        bins = bins.get(year, None)
    return {"return_value": bins}
