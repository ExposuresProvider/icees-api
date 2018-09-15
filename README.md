### Run Flask 

set env variables

`ICEES_DBUSER` 

`ICEES_DBPASS`

`ICEES_HOST` 

`ICEES_PORT` 

`ICEES_DATABASE`: json
 
 Example:

```
{"1.0.0":"iceesdb"}
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

The following steps can be run using the `redepoly.sh`

### Build Container

```
docker build . -t icees-api:0.1.0
```

### Run Container

```
docker run -e ICEES_DBUSER=<dbuser> -e ICEES_DBPASS=<dbpass> -e ICEES_HOST=<host> -e ICEES_PORT=<port> -e ICEES_DATABASE=<database> --rm -p 8080:8080 icees-api:0.1.0
```

```
docker run -e ICEES_DBUSER=<dbuser> -e ICEES_DBPASS=<dbpass> -e ICEES_HOST=<host> -e ICEES_PORT=<port> -e ICEES_DATABASE=<database> --rm --net host icees-api:0.1.0
```

### Setting up `systemd`

run docker containers
```
docker run -d -e ICEES_DBUSER=<dbuser> -e ICEES_DBPASS=<dbpass> -e ICEES_HOST=<host> -e ICEES_PORT=<port> -e ICEES_DATABASE=<database> --name icees-api_server -p 8080:8080 icees-api:0.1.0
```

```
docker run -d -e ICEES_DBUSER=<dbuser> -e ICEES_DBPASS=<dbpass> -e ICEES_HOST=<host> -e ICEES_PORT=<port> -e ICEES_DATABASE=<database> --name icees-api_server --net host icees-api:0.1.0
```

```
docker stop icees-api_server
```

copy `<repo>/icees-api-container.service` to `/etc/systemd/system/icees-api-container.service`

start service

```
systemctl start icees-api-container
```
### REST API ###

#### create cohort
method
```
POST
```

route
```
/1.0.0/(patient|visit)/(2010|2011)/cohort
```
schema
```
{"<feature name>":{"operator":<operator>,"value":<value>},...,"<feature name>":{"operator":<operator>,"value":<value>}}
```

`feature name`: see Kara's spreadsheet

`operator ::= <|>|<=|>=|=|<>`

#### get cohort definition
method
```
GET
```

route
```
/1.0.0/(patient|visit)/(2010|2011)/cohort/<cohort id>
```

#### get cohort features
method
```
GET
```

route
```
/1.0.0/(patient|visit)/(2010|2011)/cohort/<cohort id>/features
```

#### get cohort dictionary
method
```
GET
```

route
```
/1.0.0/(patient|visit)/(2010|2011)/cohort/dictionary
```

#### feature association between two features
method
```
POST
```

route
```
/1.0.0/(patient|visit)/(2010|2011)/cohort/<cohort id>/feature_association
```
schema
```
{"feature_a":{"<feature name>":{"operator":<operator>,"value":<value>}},"feauture_b":{"<feature name>":{"operator":<operator>,"value":<value>}}}
```

#### associations of one feature to all features
method
```
POST
```

route
```
/1.0.0/(patient|visit)/(2010|2011)/cohort/<cohort id>/associations_to_all_features
```
schema
```
{"feature":{"<feature name>":{"operator":<operator>,"value":<value>}},"maximum_p_value":<maximum p value>}
```

### Examples ###

get cohort of all patients

```
curl -k -XPOST https://localhost:8080/1.0.0/patient/2010/cohort -H "Content-Type: application/json" -H "Accept: application/json" -d '{}'
```

get cohort of patients with `AgeStudyStart = 0-2`

```
curl -k -XPOST https://localhost:8080/1.0.0/patient/2010/cohort -H "Content-Type: application/json" -H "Accept: application/json" -d '{"AgeStudyStart":{"operator":"=","value":"0-2"}}'
```

Assuming we have cohort id `COHORT:10`

get definition of cohort

```
curl -k -XGET https://localhost:8080/1.0.0/patient/2010/cohort/COHORT:10 -H "Accept: application/json"
```

get features of cohort

```
curl -k -XGET https://localhost:8080/1.0.0/patient/2010/cohort/COHORT:10/features -H "Accept: application/json"
```

get cohort dictionary 

```
curl -k -XGET https://localhost:8080/1.0.0/patient/2010/cohort/COHORT:10/features -H "Accept: application/json"
```

get feature association


```
curl -k -XPOST https://localhost:8080/1.0.0/patient/2010/cohort/COHORT:10/feature_association -H "Content-Type: application/json" -d '{"feature_a":{"AgeStudyStart":{"operator":"=", "value":"0-2"}},"feature_b":{"ObesityBMI":{"operator":"=", "value":0}}}'
```

get association to all features


```
curl -k -XPOST https://localhost:8080/1.0.0/patient/2010/cohort/COHORT:10/associations_to_all_features -H "Content-Type: application/json" -d '{"feature":{"AgeStudyStart":{"operator":"=", "value":"0-2"}},"maximum_p_value":0.1}' -H "Accept: application/json"
```




