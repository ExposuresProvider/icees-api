#!/bin/bash
echo waiting for database startup
sleep 10
ls -A /data

db=$(psql -qtAX -U postgres -c "SELECT count(*) FROM pg_catalog.pg_user WHERE usename = '"$ICEES_DBUSER"';")

echo db == \"$db\"

if [[ "$db" == "0" ]]  
then
    echo data directory empty initializing db
    psql -U postgres -c "CREATE USER "$ICEES_DBUSER" with password '"$ICEES_DBPASS"'"
    echo ICEES_DATABASE = $ICEES_DATABASE
    ICEES_DATABASE_CREATE=$(python3 db/values.py $ICEES_DATABASE)
    echo ICEES_DATABASE_CREATE = $ICEES_DATABASE_CREATE
    for db in $ICEES_DATABASE_CREATE
    do
	psql -U postgres -c "CREATE DATABASE "$db
	psql -U postgres -c "GRANT ALL ON DATABASE "$db" to "$ICEES_DBUSER
    done
    export ICEES_HOST=localhost
    export ICEES_PORT=5432
    python3 dbutils.py --version 2.0.0 create
    python3 dbutils.py --version 2.0.0 insert db/data/patient.csv patient PatientId
    python3 dbutils.py --version 2.0.0 insert db/data/visit.csv visit VisitId
    echo db initialized
fi

