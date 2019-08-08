FROM python:3.5

RUN pip install flask flask-restful flask-limiter sqlalchemy psycopg2-binary scipy gunicorn jsonschema pyyaml tabulate structlog pandas argparse inflection flasgger simplejson requests pytest
COPY . /icees-api

WORKDIR /icees-api

ENTRYPOINT ["pytest"]

