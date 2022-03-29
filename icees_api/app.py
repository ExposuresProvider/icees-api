"""ICEES API entrypoint."""
from functools import wraps
import inspect
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path
from time import strftime
from typing import Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.openapi.utils import get_openapi
from fastapi.responses import PlainTextResponse
from jsonschema import ValidationError
from starlette.responses import Response, JSONResponse
from structlog import wrap_logger
from structlog.processors import JSONRenderer
import yaml

from .features import format_
from .features.knowledgegraph import TOOL_VERSION

from .handlers import ROUTER
from .trapi import TRAPI

CONFIG_PATH = os.getenv('CONFIG_PATH', './config')
DESCRIPTION_FILE = Path(CONFIG_PATH) / "static" / "api_description.html"
with open(DESCRIPTION_FILE, "r") as stream:
    DESCRIPTION = stream.read()

OPENAPI_TITLE = os.getenv('OPENAPI_TITLE', 'ICEES API')
OPENAPI_HOST = os.getenv('OPENAPI_HOST', 'localhost:8080')
OPENAPI_SCHEME = os.getenv('OPENAPI_SCHEME', 'http')
OPENAPI_SERVER_URL = os.getenv("OPENAPI_SERVER_URL")


class NaNResponse(JSONResponse):
    """JSONResponse subclass inserting null for NaNs."""

    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        """Convert to str."""
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=True,
            indent=None,
            separators=(",", ":"),
        ).replace(":NaN", ":null").encode("utf-8")


openapi_args = dict(
    title=OPENAPI_TITLE,
    description=DESCRIPTION,
    docs_url="/apidocs",
    default_response_class=NaNResponse,
    redoc_url=None,
    version=TOOL_VERSION,
    terms_of_service="/tos",
    translator_component="KP",
    translator_teams=["Exposures Provider"],
    contact={
        "name": "Kenneth Morton",
        "email": "kenny@covar.com",
        "x-id": "kennethmorton",
        "x-role": "responsible developer",
    },
    infores=os.getenv("ICEES_INFORES_CURIE", "infores:icees")
)
OPENAPI_SERVER_MATURITY = os.getenv("OPENAPI_SERVER_MATURITY", "development")
OPENAPI_SERVER_LOCATION = os.getenv("OPENAPI_SERVER_LOCATION", "RENCI")
if OPENAPI_SERVER_URL:
    openapi_args["servers"] = [
        {
            "url": OPENAPI_SERVER_URL,
            "x-maturity": OPENAPI_SERVER_MATURITY,
            "x-location": OPENAPI_SERVER_LOCATION
        }
    ]
APP = TRAPI(**openapi_args)

with open(Path(CONFIG_PATH) / "static" / "terms.txt", 'r') as content_file:
    TERMS_AND_CONDITIONS = content_file.read()

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.ERROR)

HANDLER = TimedRotatingFileHandler(os.path.join(
    os.environ.get("ICEES_API_LOG_PATH", "./logs"),
    "server",
))

LOGGER.addHandler(HANDLER)
LOGGER = wrap_logger(LOGGER, processors=[JSONRenderer()])


@APP.get("/tos", response_class=PlainTextResponse)
def terms_of_service():
    """Get terms of service."""
    return TERMS_AND_CONDITIONS


@APP.middleware("http")
async def fix_tabular_outputs(request: Request, call_next):
    """Fix tabular outputs."""
    response = await call_next(request)

    timestamp = strftime('%Y-%b-%d %H:%M:%S')
    LOGGER.info(
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
    return response


def jsonable_safe(obj):
    """Convert to JSON-able, if possible.

    Based on fastapi.encoders.jsonable_encoder.
    """
    try:
        return jsonable_encoder(obj, exclude_none=True)
    except:
        return obj


def prepare_output(func):
    """Prepare output."""
    @wraps(func)
    def wrapper(*args, request=None, **kwargs):
        """Wrap func."""
        # convert arguments to jsonable, where possible
        args = [jsonable_safe(arg) for arg in args]
        kwargs = {
            key: jsonable_safe(value) if key != "conn" else value
            for key, value in kwargs.items()
        }

        # run func, logging errors
        try:
            return_value = func(*args, **kwargs)

        except ValidationError as err:
            LOGGER.exception(err)
            return_value = {"return value": err.message}
        except HTTPException:
            raise
        except Exception as err:
            LOGGER.exception(err)
            return_value = {"return value": str(err)}

        # return tabular data, if requested
        if request.headers["accept"] == "text/tabular":
            content = format_.format_tabular(
                TERMS_AND_CONDITIONS,
                return_value.get("return value", return_value),
            )
            return Response(
                content,
                media_type="text/tabular"
            )

        # add terms and conditions
        return {
            "terms and conditions": TERMS_AND_CONDITIONS,
            **return_value,
        }

    # add `request` to function signature
    # without this, FastAPI will not send it
    wrapper.__signature__ = inspect.Signature(
        [inspect.Parameter(
            "request",
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=Request,
        )]
        + list(inspect.signature(func).parameters.values())
    )
    wrapper.__annotations__ = {
        **func.__annotations__,
        "request": Request,
    }
    return wrapper


for route in ROUTER.routes:
    APP.add_api_route(
        route.path,
        (
            prepare_output(route.endpoint)
            if route.path != "/predicates" else
            route.endpoint
        ),
        responses={
            200: {
                "content": {"text/tabular": {}},
                "description": "Return the tabular output.",
            }
        },
        response_model=route.response_model,
        tags=route.tags,
        deprecated=route.deprecated,
        methods=route.methods,
    )
