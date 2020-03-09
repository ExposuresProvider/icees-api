FROM python:3.5
				
RUN pip install flask flask-restful flask-limiter sqlalchemy psycopg2-binary scipy gunicorn==19.10.0 jsonschema pyyaml tabulate structlog pandas==0.25.3 argparse inflection flasgger simplejson
RUN mkdir icees-api
COPY ./app.py /icees-api/app.py
COPY ./db.py /icees-api/db.py
COPY ./utils.py /icees-api/utils.py
COPY ./terms.txt /icees-api/terms.txt
COPY ./TranslatorReasonersAPI.yaml /icees-api/TranslatorReasonersAPI.yaml
COPY ./features /icees-api/features

WORKDIR /icees-api

ENTRYPOINT ["gunicorn","--workers", "4", "--timeout", "300", "--bind", "0.0.0.0:8080"]

CMD ["app:app"]