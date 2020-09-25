FROM python:3.8
				
RUN mkdir icees-api
COPY ./requirements.txt /icees-api/requirements.txt

RUN pip install -r icees-api/requirements.txt

COPY ./app.py /icees-api/app.py
COPY ./db.py /icees-api/db.py
COPY ./utils.py /icees-api/utils.py
COPY ./terms.txt /icees-api/terms.txt
COPY ./TranslatorReasonersAPI.yaml /icees-api/TranslatorReasonersAPI.yaml
COPY ./features /icees-api/features
COPY ./static /icees-api/static

WORKDIR /icees-api

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
