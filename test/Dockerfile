FROM python:3.5

RUN pip install sqlalchemy psycopg2-binary scipy jsonschema pyyaml structlog pandas simplejson requests
COPY . /icees-api

WORKDIR /icees-api

ENTRYPOINT ["python","-m", "test.test"]

