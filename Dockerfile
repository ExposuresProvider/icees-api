FROM python:3.8

WORKDIR /

# install requirements
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

# set up API things
COPY ./icees_api icees_api
COPY ./main.sh main.sh
COPY ./examples examples
COPY ./openapi-info.yml /icees-api/openapi-info.yml

# run API
CMD ["./main.sh"]
