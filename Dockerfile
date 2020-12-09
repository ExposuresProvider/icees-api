FROM python:3.8
				
RUN mkdir icees-api
COPY ./app.py /icees-api/app.py
COPY ./db.py /icees-api/db.py
COPY ./utils.py /icees-api/utils.py
COPY ./terms.txt /icees-api/terms.txt
COPY ./TranslatorReasonersAPI.yaml /icees-api/TranslatorReasonersAPI.yaml
COPY ./features /icees-api/features
COPY ./requirements.txt /icees-api/requirements.txt

RUN pip install -r icees-api/requirements.txt

WORKDIR /icees-api

ENTRYPOINT ["gunicorn","--workers", "4", "--timeout", "1800", "--bind", "0.0.0.0:8080"]

CMD ["app:app"]