FROM python:3.5

RUN openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365 -subj "/C=US/ST=North Carolina/L=Chapel Hill/O=UNC Chapel Hill/OU=RENCI/CN=icees"
				
COPY . /icees-api
RUN pip install -r icees-api/requirements.txt

WORKDIR /icees-api

ENTRYPOINT ["gunicorn","--workers", "4", "--timeout", "300", "--certfile", "/cert.pem","--keyfile","/key.pem","--bind", "0.0.0.0:8080"]

CMD ["app:app"]