#!/bin/bash
set -m
/usr/local/bin/docker-entrypoint.sh postgres &
pushd icees-api
python3 initdb.py
popd
fg %1

