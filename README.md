### set up database ###
Create user. In the following, we assume the user is `ddcr`

set env variables

`DDCR_DBUSER`, `DDCR_DBPASS`,`DDCR_HOST`, `DDCR_PORT`, `DDCR_DATABASE`

```psql -d<database>```

```grant all privileges on all tables in schema public to ddcr```

```grant all privileges on all sequence in schema public to ddcr```

### build container

```
docker build . -t ddcr-api:0.1.0
```

### run container

```
docker run --rm -p 8080:8080 ddcr-api:0.1.0
```

### setting up systemd

run docker containers
```
docker run -d --name ddcr-api_server -p 8080:8080 ddcr-api:0.1.0
```

add following file to `/etc/systemd/system/ddcr-api-container.service`

```
[Unit]
Description=DDCR API container
After=docker.service

[Service]
Restart=always
ExecStart=/usr/bin/docker start -a ddcr-api_server
ExecStop=/usr/bin/docker stop -t 2 ddcr-api_server

[Install]
WantedBy=local.target
```

### example ###

create cohort of all patients
```
curl localhost:5000/get_ids_by_feature
```

create cohort of patients with `feature_3 = false`
```
curl -XGET localhost:5000/get_ids_by_feature -H "Content-Type: application/json" -d '{"feature_3":{"operator":"=","value":false}}'
```

get features of cohort id `COHORT:3`
```
curl -XGET localhost:5000/get_features_by_id/COHORT:3
```

calculate `p-value` and `chi squared`
```
curl -XGET localhost:5000/get_feature_association -H "Content-Type: application/json" -d '{"cohort_id":"COHORT:3", "feature_a":{"feature_name":"feature_4","feature_qualifier":{"operator":">", "value":5}},"feature_b":{"feature_name":"feature_5","feature_qualifier":{"operator":">=", "value":10}}}'
```






