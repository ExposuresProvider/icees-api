FROM python:3.5

RUN pip install sqlalchemy psycopg2-binary scipy jsonschema pyyaml tabulate structlog pandas inflection simplejson requests
COPY . /icees-api

WORKDIR /icees-api

ENTRYPOINT ["python","-m", "test.test"]

