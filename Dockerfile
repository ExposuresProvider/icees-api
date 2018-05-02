FROM python:3.5

RUN pip install flask-restful gunicorn
RUN git clone https://github.com/xu-hao/ddcr-api

WORKDIR /ddcr-api

ENTRYPOINT ["gunicorn"]

CMD ["app:app", "--bind", "0.0.0.0:8080"]