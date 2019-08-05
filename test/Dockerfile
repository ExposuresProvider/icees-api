FROM python:3.5

RUN pip install requests
COPY . /icees-api

WORKDIR /icees-api/test

ENTRYPOINT ["python","test.py"]

