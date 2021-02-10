#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

export $(egrep -v '^#' .env | xargs)

pip install -r $DIR/requirements.txt

python $DIR/initdb.py
