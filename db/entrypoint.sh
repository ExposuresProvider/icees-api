#!/bin/bash
set -m
./docker-entrypoint.sh postgres &
pushd icees-api
db/initdb.sh
popd
fg %1

