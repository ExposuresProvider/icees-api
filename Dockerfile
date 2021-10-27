FROM python:3.8

WORKDIR /

# install requirements
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN mkdir log
# set up API things
COPY ./icees_api icees_api
COPY ./main.sh main.sh
COPY ./config config
COPY ./data data
COPY ./examples examples
COPY ./.env .env
# run API
CMD ["./main.sh"]
