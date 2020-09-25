"""ICEES API entrypoint."""
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from time import strftime
import traceback
from typing import Dict

from fastapi import FastAPI, Request, Body
from jsonschema import validate, ValidationError
from reasoner_pydantic import Request as Query, Message
from starlette.responses import StreamingResponse
from structlog import wrap_logger
from structlog.processors import JSONRenderer

import db
from features import model, schema, format_, knowledgegraph
from features.identifiers import get_identifiers
from features.knowledgegraph import TOOL_VERSION
from features.model import validate_range
from utils import opposite, to_qualifiers, to_qualifiers2

with open("static/api_description.html", "r") as stream:
    description = stream.read()

OPENAPI_HOST = os.getenv('OPENAPI_HOST', 'localhost:8080')
OPENAPI_SCHEME = os.getenv('OPENAPI_SCHEME', 'http')

app = FastAPI(
    title="ICEES API",
    description=description,
    version=TOOL_VERSION,
    terms_of_service='N/A',
    servers=[
        {"url": f"{OPENAPI_SCHEME}://{OPENAPI_HOST}"},
    ],
)

with open('terms.txt', 'r') as content_file:
    terms_and_conditions = content_file.read()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(os.path.join(
    os.environ["ICEES_API_LOG_PATH"],
    "server",
))

logger.addHandler(handler)
logger = wrap_logger(logger, processors=[JSONRenderer()])


@app.middleware("http")
async def fix_tabular_outputs(request: Request, call_next):
    """Fix tabular outputs."""
    response = await call_next(request)

    timestamp = strftime('%Y-%b-%d %H:%M:%S')
    logger.info(
        event="request",
        timestamp=timestamp,
        remote_addr=request.client.host,
        method=request.method,
        schema=request.url.scheme,
        full_path=request.url.path,
        # data=(await request.body()).decode('utf-8'),
        response_status=response.status_code,
        # x_forwarded_for=request.headers.getlist("X-Forwarded-For"),
    )

    if request.headers.get("content-type", None) == "text/tabular":
        # logger.debug("Converting to %s", request.headers["content-type"])
        data = json.loads(''.join([
            str(i) async for i in response.body_iterator
        ]))
        content = format_.format_tabular(
            terms_and_conditions,
            data.get("return value", data),
        )
        response = StreamingResponse(
            content,
            media_type="text/tabular"
        )
    return response

# api = Api(app)


def wrapped(data, reasoner=False):
    """Add terms and conditions to response data."""
    if reasoner:
        return {
            "terms and conditions": terms_and_conditions,
            **data,
        }
    else:
        return {
            "terms and conditions": terms_and_conditions,
            "return value": data,
        }


@app.post("/{table}/{year}/cohort", response_model=Dict)
def discover_cohort(
        table: str,
        year: int,
        req_features: Dict = Body(..., example={}),
) -> Dict:
    """Cohort discovery."""
    try:
        with db.DBConnection() as conn:
            validate(req_features, schema.features_schema(table))
            cohort_id, size = model.get_ids_by_feature(conn, table, year, req_features)

            if size == -1:
                return_value = "Input features invalid or cohort ≤10 patients. Please try again."
            else:
                return_value = {
                    "cohort_id": cohort_id,
                    "size": size
                }

    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.put("/{table}/{year}/cohort/{cohort_id}", response_model=Dict)
def edit_cohort(
        table: str,
        year: int,
        cohort_id: str,
        req_features: Dict = Body(..., example={}),
) -> Dict:
    """Cohort discovery."""
    try:
        with db.DBConnection() as conn:
            validate(req_features, schema.features_schema(table))
            cohort_id, size = model.select_cohort(
                conn,
                table,
                year,
                req_features,
                cohort_id,
            )

            if size == -1:
                return_value = "Input features invalid or cohort ≤10 patients. Please try again."
            else:
                return_value = {
                    "cohort_id": cohort_id,
                    "size": size
                }
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.get("/{table}/{year}/cohort/{cohort_id}", response_model=Dict)
def get_cohort(
        table: str,
        year: int,
        cohort_id: str,
) -> Dict:
    """Get definition of a cohort."""
    try:
        with db.DBConnection() as conn:
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
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.post(
    "/{table}/{year}/cohort/{cohort_id}/feature_association",
    response_model=Dict,
)
def feature_association(
        table: str,
        year: int,
        cohort_id: str,
        obj: Dict = Body(..., example={
            "feature_a": {
                "Sex": {
                    "operator": "=",
                    "value": "Female"
                }
            },
            "feature_b": {
                "AsthmaDx": {
                    "operator": "=",
                    "value": 1
                }
            }
        }),
) -> Dict:
    """Hypothesis-driven 2 x 2 feature associations.

    Users select a predefined cohort and two feature variables, and the service
    returns a 2 x 2 feature table with a correspondingChi Square statistic and
    P value.
    """
    try:
        logger.info(f"validating {obj} schema {schema.feature_association_schema(table)}")
        validate(obj, schema.feature_association_schema(table))
        feature_a = to_qualifiers(obj["feature_a"])
        feature_b = to_qualifiers(obj["feature_b"])

        with db.DBConnection() as conn:
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
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.post(
    "/{table}/{year}/cohort/{cohort_id}/feature_association2",
    response_model=Dict,
)
def feature_association2(
        table: str,
        year: int,
        cohort_id: str,
        obj: Dict = Body(..., example={
            "feature_a": {
                "Sex": [
                    {
                        "operator": "=",
                        "value": "Female"
                    },
                    {
                        "operator": "=",
                        "value": "Male"
                    }
                ]
            },
            "feature_b": {
                "AsthmaDx": [
                    {
                        "operator": "=",
                        "value": 1
                    },
                    {
                        "operator": "=",
                        "value": 0
                    }
                ]
            }
        }),
) -> Dict:
    """Hypothesis-driven N x N feature associations.

    Users select a predefined cohort, two feature variables, and bins, which
    can be combined, and the service returns a N x N feature table with a
    corresponding Chi Square statistic and P value.
    """
    try:
        validate(obj, schema.feature_association2_schema(table))
        feature_a = to_qualifiers2(obj["feature_a"])
        feature_b = to_qualifiers2(obj["feature_b"])
        to_validate_range = obj.get("check_coverage_is_full", False)
        if to_validate_range:
            validate_range(table, feature_a)
            validate_range(table, feature_b)

        with db.DBConnection() as conn:
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

    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.post(
    "/{table}/{year}/cohort/{cohort_id}/associations_to_all_features",
    response_model=Dict,
)
def associations_to_all_features(
        table: str,
        year: int,
        cohort_id: str,
        obj: Dict = Body(..., example={
            "feature": {
                "Sex": {
                    "operator": "=",
                    "value": "Female"
                }
            },
            "maximum_p_value": 1,
            "correction": {
                "method": "bonferroni"
            }
        }),
) -> Dict:
    """Exploratory 1 X N feature associations.

    Users select a predefined cohort and a feature variable of interest, and
    the service returns a 1 x N feature table with corrected Chi Square
    statistics and associated P values.
    """
    try:
        validate(obj, schema.associations_to_all_features_schema(table))
        feature = to_qualifiers(obj["feature"])
        maximum_p_value = obj["maximum_p_value"]
        correction = obj.get("correction")
        with db.DBConnection() as conn:
            return_value = model.select_associations_to_all_features(
                conn,
                table,
                year,
                cohort_id,
                feature,
                maximum_p_value,
                correction=correction,
            )
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.post(
    "/{table}/{year}/cohort/{cohort_id}/associations_to_all_features2",
    response_model=Dict,
)
def associations_to_all_features2(
        table: str,
        year: int,
        cohort_id: str,
        obj: Dict = Body(..., example={
            "feature": {
                "Sex": [
                    {
                        "operator": "=",
                        "value": "Female"
                    },
                    {
                        "operator": "=",
                        "value": "Male"
                    }
                ]
            },
            "maximum_p_value": 1,
            "correction": {
                "method": "bonferroni"
            }
        }),
) -> Dict:
    """Exploratory 1 X N feature associations.

    Users select a predefined cohort and a feature variable of interest and
    bins, which can be combined, and the service returns a 1 x N feature table
    with corrected Chi Square statistics and associated P values.
    """
    try:
        validate(obj, schema.associations_to_all_features2_schema(table))
        feature = to_qualifiers2(obj["feature"])
        to_validate_range = obj.get("check_coverage_is_full", False)
        if to_validate_range:
            validate_range(table, feature)
        maximum_p_value = obj["maximum_p_value"]
        correction = obj.get("correction")
        with db.DBConnection() as conn:
            return_value = model.select_associations_to_all_features(
                conn,
                table,
                year,
                cohort_id,
                feature,
                maximum_p_value,
                correction=correction,
            )
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.get(
    "/{table}/{year}/cohort/{cohort_id}/features",
    response_model=Dict,
)
def features(
        table: str,
        year: int,
        cohort_id: str,
) -> Dict:
    """Feature-rich cohort discovery.

    Users select a predefined cohort as the input parameter, and the service
    returns a profile of that cohort in terms of all feature variables.
    """
    try:
        with db.DBConnection() as conn:
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

    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.get(
    "/{table}/{year}/cohort/dictionary",
    response_model=Dict,
)
def dictionary(
        table: str,
        year: int,
) -> Dict:
    """Get cohort dictionary."""
    try:
        with db.DBConnection() as conn:
            return_value = model.get_cohort_dictionary(conn, table, year)
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.get(
    "/{table}/{feature}/identifiers",
    response_model=Dict,
)
def identifiers(
        table: str,
        feature: str,
) -> Dict:
    """Feature identifiers."""
    try:
        return wrapped({
            "identifiers": get_identifiers(table, feature)
        })
    except Exception as err:
        traceback.print_exc()
        return wrapped(str(err))


@app.get(
    "/{table}/name/{name}",
    response_model=Dict,
)
def get_name(
        table: str,
        name: str,
) -> Dict:
    """Return cohort id associated with name."""
    try:
        with db.DBConnection() as conn:
            return_value = model.get_id_by_name(conn, table, name)
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.post(
    "/{table}/name/{name}",
    response_model=Dict,
)
def post_name(
        table: str,
        name: str,
        obj: Dict,
) -> Dict:
    """Associate name with cohort id."""
    try:
        validate(obj, schema.add_name_by_id_schema())
        with db.DBConnection() as conn:
            return_value = model.add_name_by_id(
                conn,
                table,
                name,
                obj["cohort_id"],
            )
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value)


@app.post(
    "/knowledge_graph",
    response_model=Dict,
)
def knowledge_graph(
        obj: Query,
        reasoner: bool = False,
) -> Message:
    """Query the ICEES clinical reasoner for knowledge graph associations between concepts."""
    obj = obj.dict()
    try:
        with db.DBConnection() as conn:
            return_value = knowledgegraph.get(conn, obj)
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value, reasoner=reasoner)


@app.get(
    "/knowledge_graph/schema",
    response_model=Dict,
)
def knowledge_graph_schema(
        reasoner: bool = False,
) -> Dict:
    """Query the ICEES clinical reasoner for knowledge graph schema."""
    try:
        return_value = knowledgegraph.get_schema()
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value, reasoner=reasoner)


with open("examples/knowledge_graph_overlay.json") as stream:
    kg_overlay_example = json.load(stream)


@app.post(
    "/knowledge_graph_overlay",
    response_model=Dict,
)
def knowledge_graph_overlay(
        obj: Query = Body(..., example=kg_overlay_example),  # QueryOverlay
        reasoner: bool = False,
) -> Message:
    """Query the ICEES clinical reasoner for knowledge graph co-occurrence overlay."""
    obj = obj.dict()
    try:
        # validate(obj, schema.add_name_by_id_schema())
        with db.DBConnection() as conn:
            return_value = knowledgegraph.co_occurrence_overlay(conn, obj)
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value, reasoner=reasoner)


with open("examples/knowledge_graph_one_hop.json") as stream:
    kg_onehop_example = json.load(stream)


@app.post(
    "/knowledge_graph_one_hop",
    response_model=Dict,
)
def knowledge_graph_one_hop(
        obj: Query = Body(..., example=kg_onehop_example),  # QueryOneHop
        reasoner: bool = False,
) -> Message:
    """Query the ICEES clinical reasoner for knowledge graph one hop."""
    obj = obj.dict()
    try:
        # validate(obj, schema.add_name_by_id_schema())
        with db.DBConnection() as conn:
            return_value = knowledgegraph.one_hop(conn, obj)
    except ValidationError as err:
        traceback.print_exc()
        return_value = err.message
    except Exception as err:
        traceback.print_exc()
        return_value = str(err)
    return wrapped(return_value, reasoner=reasoner)
