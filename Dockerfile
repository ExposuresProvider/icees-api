FROM python:3.8

ARG UID
ARG GID

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /
# install requirements
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

# create a new user and use it.
RUN mkdir -p /home/iceesuser
RUN groupadd --system -g $GID iceesuser && useradd --system -g iceesuser --shell /bin/bash -u $UID --home /home/iceesuser iceesuser

WORKDIR /home/iceesuser
# set up API things
COPY ./icees_api icees_api
COPY ./main.sh main.sh
COPY ./examples examples

RUN chown -R iceesuser:iceesuser /home/iceesuser

USER iceesuser
WORKDIR /home/iceesuser

# run API
CMD ["./main.sh"]
