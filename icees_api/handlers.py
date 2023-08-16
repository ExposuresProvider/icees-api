"""ICEES API handlers."""
import os
import json
from typing import Dict, Optional, List

from fastapi import APIRouter, Body, Depends, Security, HTTPException
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from starlette.status import HTTP_403_FORBIDDEN

from .dependencies import get_db
from .features import sql
from .features.sql import validate_range, validate_feature_value_in_table_column_for_equal_operator
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
    returns a 2 x 2 feature table with a corresponding Chi Square statistic and
    P value, Fisher's exact odds ratio, log odds ratio with 95% confidence interval,
    and Fisher's exact P value.
    """
    validate_table(table)
    feature_a = to_qualifiers(obj["feature_a"])
    feature_b = to_qualifiers(obj["feature_b"])
    try:
        validate_feature_value_in_table_column_for_equal_operator(conn, table, feature_a)
        validate_feature_value_in_table_column_for_equal_operator(conn, table, feature_b)
    except RuntimeError as ex:
        return {"return value": str(ex)}

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
    corresponding Chi Square statistic, Fisher's exact odds ratio,
    log odds ratio with 95% confidence interval, and Fisher's exact P value
    """
    validate_table(table)
    feature_a = to_qualifiers2(obj["feature_a"])
    feature_b = to_qualifiers2(obj["feature_b"])
    try:
        validate_feature_value_in_table_column_for_equal_operator(conn, table, feature_a)
        validate_feature_value_in_table_column_for_equal_operator(conn, table, feature_b)
    except RuntimeError as ex:
        return {"return value": str(ex)}

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
    statistics and associated P values, Fisher's exact odds ratio, log odds ratio
    with 95% confidence interval, and Fisher's exact P value
    """
    validate_table(table)
    feature = to_qualifiers(obj["feature"])
    try:
        validate_feature_value_in_table_column_for_equal_operator(conn, table, feature)
    except RuntimeError as ex:
        return {"return value": str(ex)}

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
    with corrected Chi Square statistics and associated P values, Fisher's exact odds ratio,
    log odds ratio with 95% confidence interval, and Fisher's exact P value.
    """
    validate_table(table)
    feature = to_qualifiers2(obj["feature"])
    try:
        validate_feature_value_in_table_column_for_equal_operator(conn, table, feature)
    except RuntimeError as ex:
        return {"return value": str(ex)}

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
    if not os.path.exists(input_file):
        return {"return_value": None,
                "message": "Binning results are not available"}

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


with open("examples/multivariate_associations.json") as stream:
    MULTIVARIATE_ASSOCIATION_EXAMPLE = json.load(stream)


@ROUTER.post(
    "/cohort/{cohort_id}/multivariate_feature_analysis",
    response_model=Dict,
)
def multivariate_feature_analysis(
        cohort_id: str,
        year: Optional[str] = None,
        feature_variables: List[str] = Body(
            ...,
            example=MULTIVARIATE_ASSOCIATION_EXAMPLE,
        ),
        conn=Depends(get_db),
        api_key: APIKey = Depends(get_api_key),
) -> Dict:
    """Exploratory multivariate analysis of patient feature variables.

    Users select a predefined cohort and a list of up to eight patient feature variables or predictors
    in a particular order, and the service returns a multivariate table, with contingencies between feature variables
    maintained. Note that the open multivariate functionality incurs a certain amount of data loss due to privacy
    constraints that limit the ability of create cohorts < 10 patients. The amount of data loss varies and is
    influenced by the order in which the feature variables are selected. Note that a complex algorithm is used to
    openly create ICEES multivariate tables; this process may take a while or time out. Users are encouraged to
    structure queries as CURLs rather than work through the Swagger UI.
    """
    table = 'patient'

    return_value = sql.compute_multivariate_table(
        conn,
        table,
        year,
        cohort_id,
        feature_variables
    )
    return {"return value": return_value}
