#!/usr/bin/env bash

IFS='
'
export $(egrep -v '^#' .env | xargs -0)
IFS=

# run api server
uvicorn icees_api.app:APP --host 0.0.0.0 --port 8080
