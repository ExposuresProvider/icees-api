FROM python:3.5

RUN openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365 -subj "/C=US/ST=North Carolina/L=Chapel Hill/O=UNC Chapel Hill/OU=RENCI/CN=ddcr"
				
RUN pip install flask flask-restful flask-limiter sqlalchemy psycopg2 scipy gunicorn
RUN git clone https://github.com/xu-hao/ddcr-api

WORKDIR /ddcr-api

ENTRYPOINT ["gunicorn","--certfile", "/cert.pem","--keyfile","/key.pem","--bind", "0.0.0.0:8080"]

CMD ["app:app"]