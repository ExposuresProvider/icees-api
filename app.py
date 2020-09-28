"""ICEES API entrypoint."""
from functools import wraps
import inspect
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from time import strftime

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from starlette.responses import Response
from structlog import wrap_logger
from structlog.processors import JSONRenderer

from features import format_
from features.knowledgegraph import TOOL_VERSION

from handlers import router

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
    return response


def jsonable_safe(obj):
    """Convert to JSON-able, if possible.

    Based on fastapi.encoders.jsonable_encoder.
    """
    try:
        return jsonable_encoder(obj)
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
            print(return_value)
        except Exception as err:
            logger.exception(err)
            raise err

        # return tabular data, if requested
        if request.headers["accept"] == "text/tabular":
            content = format_.format_tabular(
                terms_and_conditions,
                return_value.get("return value", return_value),
            )
            return Response(
                content,
                media_type="text/tabular"
            )

        # add terms and conditions
        return {
            "terms and conditions": terms_and_conditions,
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


for route in router.routes:
    app.add_api_route(
        route.path,
        prepare_output(route.endpoint),
        responses={
            200: {
                "content": {"text/tabular": {}},
                "description": "Return the tabular output.",
            }
        },
        response_model=route.response_model,
        methods=route.methods,
    )
