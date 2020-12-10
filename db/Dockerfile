FROM postgres:12.4-alpine

RUN apk add bash gcc python3 py3-pip postgresql-dev python3-dev musl-dev gfortran build-base wget freetype-dev libpng-dev openblas-dev --no-cache
RUN pip install flask
RUN pip install flask-restful
RUN pip install flask-limiter
RUN pip install sqlalchemy
RUN pip install psycopg2-binary
RUN pip install scipy
RUN pip install gunicorn
RUN pip install jsonschema
RUN pip install pyyaml
RUN pip install tabulate
RUN pip install structlog
RUN pip install pandas
RUN pip install argparse
RUN pip install inflection
RUN pip install flasgger
RUN pip install tx-functional
RUN pip install statsmodels==0.12.0
RUN pip install fastapi

ENV PGDATA=/data
ENV ICEES_DBUSER=icees_dbuser
ENV ICEES_DBPASS=icees_dbpass
ENV ICEES_DATABASE=icees_database
ENV POSTGRES_PASSWORD=icees_postgres_password

RUN mkdir icees-api
COPY ./initdb.py icees-api/initdb.py
COPY ./db.py icees-api/db.py
COPY ./dbutils.py icees-api/dbutils.py
COPY ./sample.py icees-api/sample.py
COPY ./db icees-api/db
COPY ./features icees-api/features
ENTRYPOINT ["icees-api/db/entrypoint.sh"]

