FROM python:3.5

RUN openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365 -subj "/C=US/ST=North Carolina/L=Chapel Hill/O=UNC Chapel Hill/OU=RENCI/CN=icees"
				
RUN pip install flask flask-restful flask-limiter sqlalchemy psycopg2-binary scipy gunicorn jsonschema pyyaml tabulate structlog pandas argparse inflection flasgger simplejson
RUN mkdir icees-api
COPY ./app.py /icees-api/app.py
COPY ./db.py /icees-api/db.py
COPY ./utils.py /icees-api/utils.py
COPY ./terms.txt /icees-api/terms.txt
COPY ./TranslatorReasonersAPI.yaml /icees-api/TranslatorReasonersAPI.yaml
COPY ./features /icees-api/features

WORKDIR /icees-api

ENTRYPOINT ["gunicorn","--workers", "4", "--timeout", "300", "--certfile", "/cert.pem","--keyfile","/key.pem","--bind", "0.0.0.0:8080"]

CMD ["app:app"]