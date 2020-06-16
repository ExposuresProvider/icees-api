FROM python:3.8

RUN pip install flask flask-restful flask-limiter sqlalchemy psycopg2-binary scipy gunicorn jsonschema pyyaml tabulate structlog pandas argparse inflection flasgger simplejson requests tx-functional pytest
COPY . /icees-api

WORKDIR /icees-api

ENTRYPOINT ["pytest"]

