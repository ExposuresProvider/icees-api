#!/usr/bin/env bash

export $(egrep -v '^#' .env | xargs)

# run api server
uvicorn icees_api.app:APP --host 0.0.0.0 --port 8080 --reload
