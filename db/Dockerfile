FROM postgres:11.3

RUN apt-get update && apt-get install python3 python3-pip -y
RUN pip3 install flask flask-restful flask-limiter sqlalchemy psycopg2-binary scipy gunicorn jsonschema pyyaml tabulate structlog pandas argparse inflection flasgger

ENV PGDATA=/data
ENV ICEES_DBUSER=icees_dbuser
ENV ICEES_DBPASS=icees_dbpass
ENV ICEES_DATABASE=icees_database

RUN mkdir icees-api
COPY ./initdb.py icees-api/initdb.py
COPY ./db.py icees-api/db.py
COPY ./dbutils.py icees-api/dbutils.py
COPY ./sample.py icees-api/sample.py
COPY ./db icees-api/db
COPY ./features icees-api/features
ENTRYPOINT ["icees-api/db/entrypoint.sh"]

