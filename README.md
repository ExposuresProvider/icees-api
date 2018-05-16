### Run Flask 

set env variables

`DDCR_DBUSER` 

`DDCR_DBPASS`

`DDCR_HOST` 

`DDCR_PORT` 

`DDCR_DATABASE`: json
 
 Example:

```
{"1.0.0":"ddcrdb"}
```



### Set up Database ###

#### Create Database

```createdb <database>```

#### Create User

```createuser -P <dbuser>```

enter `<dbpass>` for new user

#### Create Tables

```psql -d<database>```

```\i create.sql```

#### Create Sequence


#### Create Permissions

```grant all privileges on all tables in schema public to <dbuser>```

```grant all privileges on all sequence in schema public to <dbuser>```

### Build Container

```
docker build . -t ddcr-api:0.1.0
```

### Run Container

```
docker run -e DDCR_DBUSER=<dbuser> -e DDCR_DBPASS=<dbpass> -e DDCR_HOST=<host> -e DDCR_PORT=<port> -e DDCR_DATABASE=<database> --rm -p 8080:8080 ddcr-api:0.1.0
```

### Setting up `systemd`

run docker containers
```
docker run -d -e DDCR_DBUSER=<dbuser> -e DDCR_DBPASS=<dbpass> -e DDCR_HOST=<host> -e DDCR_PORT=<port> -e DDCR_DATABASE=<database> --name ddcr-api_server -p 8080:8080 ddcr-api:0.1.0
docker stop ddcr-api_server
```

copy `<repo>/ddcr-api-container.servic` to `/etc/systemd/system/ddcr-api-container.service`

start service

```
systemctl start ddcr-api-container
```

### Examples ###

create cohort of all patients
```
curl -k -XGET https://localhost:5000/1.0.0/patient/2010/cohort
```

create cohort of patients with `feature_3 = false`
```
curl -k -XGET https://localhost:5000/1.0.0/patient/2010/cohort -H "Content-Type: application/json" -d '{"feature_3":{"operator":"=","value":false}}'
```

create cohort of patients with `feature_3 <> false`
```
curl -k -XGET https://localhost:5000/1.0.0/patient/2010/cohort -H "Content-Type: application/json" -d '{"feature_3":{"operator":"<>","value":false}}'
```

get features of cohort id `COHORT:3`
```
curl -k -XGET https://localhost:5000/1.0.0/patient/2010/cohort/COHORT:3
```

calculate `p-value` and `chi squared`
```
curl -k -XGET https://localhost:5000/1.0.0/patient/2010/feature_association -H "Content-Type: application/json" -d '{"cohort_id":"COHORT:3", "feature_a":{"feature_name":"feature_4","feature_qualifier":{"operator":">", "value":5}},"feature_b":{"feature_name":"feature_5","feature_qualifier":{"operator":">=", "value":10}}}'
```






