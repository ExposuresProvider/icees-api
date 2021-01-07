"""ICEES API handlers."""
from functools import partial
import json
from typing import Dict

from fastapi import APIRouter, Body, Depends
from reasoner_converter.interfaces import upgrade_reasoner
from reasoner_pydantic import Query, Message

from dependencies import get_db
from features import model, knowledgegraph
from features.identifiers import get_identifiers
from features.model import validate_range
from models import (
    Features,
    FeatureAssociation, FeatureAssociation2,
    AllFeaturesAssociation, AllFeaturesAssociation2,
    AddNameById,
)
from utils import to_qualifiers, to_qualifiers2


ROUTER = APIRouter()


@ROUTER.post("/{table}/{year}/cohort", response_model=Dict)
def discover_cohort(
        table: str,
        year: int,
        req_features: Features = Body(..., example={}),
        conn=Depends(get_db),
) -> Dict:
    """Cohort discovery."""
    cohort_id, size = model.get_ids_by_feature(
        conn,
        table,
        year,
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
    "/{table}/{year}/cohort/dictionary",
    response_model=Dict,
)
def dictionary(
        table: str,
        year: int,
        conn=Depends(get_db),
) -> Dict:
    """Get cohort dictionary."""
    return_value = model.get_cohort_dictionary(conn, table, year)
    return {"return value": return_value}


@ROUTER.put("/{table}/{year}/cohort/{cohort_id}", response_model=Dict)
def edit_cohort(
        table: str,
        year: int,
        cohort_id: str,
        req_features: Features = Body(..., example={}),
        conn=Depends(get_db),
) -> Dict:
    """Cohort discovery."""
    cohort_id, size = model.select_cohort(
        conn,
        table,
        year,
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


@ROUTER.get("/{table}/{year}/cohort/{cohort_id}", response_model=Dict)
def get_cohort(
        table: str,
        year: int,
        cohort_id: str,
        conn=Depends(get_db),
) -> Dict:
    """Get definition of a cohort."""
    cohort_features = model.get_cohort_by_id(
        conn,
        table,
        year,
        cohort_id,
    )

    if cohort_features is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        return_value = cohort_features
    return {"return value": return_value}


with open("examples/feature_association.json") as stream:
    feature_association_example = json.load(stream)


@ROUTER.post(
    "/{table}/{year}/cohort/{cohort_id}/feature_association",
    response_model=Dict,
)
def feature_association(
        table: str,
        year: int,
        cohort_id: str,
        obj: FeatureAssociation = Body(
            ...,
            example=feature_association_example,
        ),
        conn=Depends(get_db),
) -> Dict:
    """Hypothesis-driven 2 x 2 feature associations.

    Users select a predefined cohort and two feature variables, and the service
    returns a 2 x 2 feature table with a correspondingChi Square statistic and
    P value.
    """
    feature_a = to_qualifiers(obj["feature_a"])
    feature_b = to_qualifiers(obj["feature_b"])

    cohort_meta = model.get_features_by_id(conn, table, cohort_id)

    if cohort_meta is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return_value = model.select_feature_matrix(
            conn,
            table,
            year,
            cohort_features,
            cohort_year,
            feature_a,
            feature_b,
        )
    return {"return value": return_value}


with open("examples/feature_association2.json") as stream:
    feature_association2_example = json.load(stream)


@ROUTER.post(
    "/{table}/{year}/cohort/{cohort_id}/feature_association2",
    response_model=Dict,
)
def feature_association2(
        table: str,
        year: int,
        cohort_id: str,
        obj: FeatureAssociation2 = Body(
            ...,
            example=feature_association2_example,
        ),
        conn=Depends(get_db),
) -> Dict:
    """Hypothesis-driven N x N feature associations.

    Users select a predefined cohort, two feature variables, and bins, which
    can be combined, and the service returns a N x N feature table with a
    corresponding Chi Square statistic and P value.
    """
    feature_a = to_qualifiers2(obj["feature_a"])
    feature_b = to_qualifiers2(obj["feature_b"])
    to_validate_range = obj.get("check_coverage_is_full", False)
    if to_validate_range:
        validate_range(table, feature_a)
        validate_range(table, feature_b)

    cohort_meta = model.get_features_by_id(conn, table, cohort_id)

    if cohort_meta is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return_value = model.select_feature_matrix(
            conn,
            table,
            year,
            cohort_features,
            cohort_year,
            feature_a,
            feature_b,
        )

    return {"return value": return_value}


with open("examples/associations_to_all_features.json") as stream:
    associations_to_all_features_example = json.load(stream)


@ROUTER.post(
    "/{table}/{year}/cohort/{cohort_id}/associations_to_all_features",
    response_model=Dict,
)
def associations_to_all_features(
        table: str,
        year: int,
        cohort_id: str,
        obj: AllFeaturesAssociation = Body(
            ...,
            example=associations_to_all_features_example,
        ),
        conn=Depends(get_db),
) -> Dict:
    """Exploratory 1 X N feature associations.

    Users select a predefined cohort and a feature variable of interest, and
    the service returns a 1 x N feature table with corrected Chi Square
    statistics and associated P values.
    """
    feature = to_qualifiers(obj["feature"])
    maximum_p_value = obj["maximum_p_value"]
    correction = obj.get("correction")
    return_value = model.select_associations_to_all_features(
        conn,
        table,
        year,
        cohort_id,
        feature,
        maximum_p_value,
        correction=correction,
    )
    return {"return value": return_value}


with open("examples/associations_to_all_features2.json") as stream:
    associations_to_all_features2_example = json.load(stream)


@ROUTER.post(
    "/{table}/{year}/cohort/{cohort_id}/associations_to_all_features2",
    response_model=Dict,
)
def associations_to_all_features2(
        table: str,
        year: int,
        cohort_id: str,
        obj: AllFeaturesAssociation2 = Body(
            ...,
            example=associations_to_all_features2_example,
        ),
        conn=Depends(get_db),
) -> Dict:
    """Exploratory 1 X N feature associations.

    Users select a predefined cohort and a feature variable of interest and
    bins, which can be combined, and the service returns a 1 x N feature table
    with corrected Chi Square statistics and associated P values.
    """
    feature = to_qualifiers2(obj["feature"])
    to_validate_range = obj.get("check_coverage_is_full", False)
    if to_validate_range:
        validate_range(table, feature)
    maximum_p_value = obj["maximum_p_value"]
    correction = obj.get("correction")
    return_value = model.select_associations_to_all_features(
        conn,
        table,
        year,
        cohort_id,
        feature,
        maximum_p_value,
        correction=correction,
    )
    return {"return value": return_value}


@ROUTER.get(
    "/{table}/{year}/cohort/{cohort_id}/features",
    response_model=Dict,
)
def features(
        table: str,
        year: int,
        cohort_id: str,
        conn=Depends(get_db),
) -> Dict:
    """Feature-rich cohort discovery.

    Users select a predefined cohort as the input parameter, and the service
    returns a profile of that cohort in terms of all feature variables.
    """
    cohort_meta = model.get_features_by_id(conn, table, cohort_id)
    if cohort_meta is None:
        return_value = "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return_value = model.get_cohort_features(
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
) -> Dict:
    """Feature identifiers."""
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
) -> Dict:
    """Return cohort id associated with name."""
    return_value = model.get_id_by_name(conn, table, name)
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
) -> Dict:
    """Associate name with cohort id."""
    return_value = model.add_name_by_id(
        conn,
        table,
        name,
        obj["cohort_id"],
    )
    return {"return value": return_value}


with open("examples/knowledge_graph.json") as stream:
    knowledge_graph_example = json.load(stream)


@ROUTER.post(
    "/knowledge_graph",
    response_model=Dict,
)
def knowledge_graph(
        obj: Query = Body(..., example=knowledge_graph_example),
        reasoner: bool = False,
        conn=Depends(get_db),
) -> Message:
    """Query for knowledge graph associations between concepts."""
    return_value = knowledgegraph.get(conn, obj)
    if reasoner:
        return return_value
    return {"return value": return_value}


@ROUTER.get(
    "/knowledge_graph/schema",
    response_model=Dict,
)
def knowledge_graph_schema(
        reasoner: bool = False,
) -> Dict:
    """Query the ICEES clinical reasoner for knowledge graph schema."""
    return_value = knowledgegraph.get_schema()
    if reasoner:
        return return_value
    return {"return value": return_value}


with open("examples/knowledge_graph_overlay.json") as stream:
    kg_overlay_example = json.load(stream)


@ROUTER.post(
    "/knowledge_graph_overlay",
    response_model=Dict,
)
def knowledge_graph_overlay(
        obj: Query = Body(..., example=kg_overlay_example),
        reasoner: bool = False,
        conn=Depends(get_db),
) -> Message:
    """Query for knowledge graph co-occurrence overlay."""
    return_value = knowledgegraph.co_occurrence_overlay(conn, obj)
    if reasoner:
        return return_value
    return {"return value": return_value}


with open("examples/knowledge_graph_one_hop.json") as stream:
    kg_onehop_example = json.load(stream)


@ROUTER.post(
    "/knowledge_graph_one_hop",
    response_model=Dict,
)
def knowledge_graph_one_hop(
        obj: Query = Body(..., example=kg_onehop_example),
        reasoner: bool = False,
        conn=Depends(get_db),
) -> Message:
    """Query the ICEES clinical reasoner for knowledge graph one hop."""
    return_value = upgrade_reasoner(partial(knowledgegraph.one_hop, conn))(obj)
    if reasoner:
        return return_value
    return {"return value": return_value}


@ROUTER.get(
    "/bins",
    response_model=Dict,
)
def handle_bins(
        year: str = None,
        table: str = None,
        feature: str = None,
) -> Dict:
    """Return bin values."""
    with open("config/bins.json", "r") as stream:
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
