[![Build Status](https://travis-ci.com/NCATS-Tangerine/icees-api.svg?branch=master)](https://travis-ci.com/NCATS-Tangerine/icees-api)

# How to run

## Run docker compose

### Run tests
```
test/test.sh
```

### Deployment

#### Edit schema and identifiers

ICEES API allows define custom schema and identifiers. The schema is stored at `config/features.yml`. The identifier is stored at `config/identifiers.yml`. Edit them to fit your dataset.

ICEES API has the following assumptions: 
 * Each table named `<table>` should have a column named `<table>Id` as the identifier.
 * Each table has a column named `year`. 

These columns do not need to be specified in `features.yml`.

#### Data for database

For each table in the schema, create a directory with its table name under `db/data` and put csv files under that directory. The csv files should have the same headers as the table. For example, put `patient.csv` under `db/data/patient` and put `visit.csv` under `db/data/visit`.

To generate random samples, run
```
python samples.py patient 2010 1000 db/data/patient/patient.csv
```
```
python samples.py visit 2010 1000 db/data/visit/visit.csv
```

#### Start services

The  `.env` file contains environmental variables that control the services. Edit it to fit your application.

`ICEES_PORT`: the database port in the container

`ICEES_HOST`: the database host in the container

`ICEES_DBUSER`: the database user in the container

`ICEES_DBPASS`: the password for the database user in the container

`POSTGRES_PASSWORD`: the password for database user `postgres` in the container

`ICEES_DATABASE`: the database name in the container

`ICEES_API_LOG_PATH`: the path where logs are stored on the host

`ICEES_API_HOST_PORT`: the port where icees api is listening to on the host

`OPENAPI_HOST`: the host where icees api is deployed

`OPENAPI_SCHEME`: the protocol where icees api is deployed

`DATA_PATH`: the directory where database tables csvs are stored on the host

`DB_PATH`: the directory where the database files are stored on the host

`CONFIG_PATH`: the directory where schema and identifiers are stored

`ICEES_API_INSTANCE_NAME`: icees api instance name

run
```
docker-compose up --build -d
```


## Build Container

```
docker build . -t icees-api:0.4.0
```

## REST API

### features schema
A feature qualifier limited values of a feature
```
<operator> ::= <
             | >
             | <=
             | >=
             | = 
             | <>`

<feature_qualifier> ::= {"operator":<operator>, "value":<value>}
                      | {"operator":"in", "values":[<value>, ..., <value>]}
                      | {"operator":"between", "value_a":<value>, "value_b":<value>}
```

There are two ways to specify a feature of a set of features, using a list or a dict. We show the schema for the former first, then show the schema for the latter.

```
<feature> ::= {
    "feature_name": "<feature name>",
    "feature_qualifier": <feature_qualifier>,
    "year": <year>
  }
```

where 

`feature name`: see config/features.yml

`year` is optional. When `year` is specified, it uses features from that year, otherwise it gets year from context

Example
```
{
  "feature_name": "AgeStudyStart",
  "feature_qualifier": {
    "operator": "=",
    "value": "0-2"
  }
}

```
```
<features> ::= [<feature>, ..., <feature>]
```

Example
```
[{
  "feature_name": "AgeStudyStart",
  "feature_qualifier": {
    "operator": "=",
    "value": "0-2"
  }
}, {
  "feature_name": "ObesityBMI",
  "feature_qualifier": {
    "operator": "=",
    "value": 0
  }
}]
```

In the apis that allows aggregation of bins, we can specify multiple feature qualifiers for each feature.
```
<feature2> ::= {
  "feature_name": "<feature name>",
  "feature_qualifiers": [<feature_qualifiere>, ..., <feature_qualifier>],
  "year": <year>
}
```

Example:
```
{
  "feature_name": "AgeStudyStart",
  "feature_qualifiers": [
            {
                "operator":"=",
                "value":"0-2"
            }, {
                "operator":"between",
                "value_a":"3-17",
                "value_b":"18-34"
            }, {
                "operator":"in", 
                "values":["35-50","51-69"]
            }, {
                "operator":"=",
                "value":"70+"
            }
  ]
}
```

similarly for a set of features
```
<features2> ::= [<feature2>, ..., <feature2>]
```
Example
```
[{
  "feature_name": "AgeStudyStart",
  "feature_qualifiers": [
    {
      "operator":"=",
      "value":"0-2"
    }, {
      "operator":"between",
      "value_a":"3-17",
      "value_b":"18-34"
    }, {
      "operator":"in", 
      "values":["35-50","51-69"]
    },{
      "operator":"=",
      "value":"70+"
    }
  ]
}, {
  "feature_name": "EstResidentialDensity",
  "feature_qualifiers": [
    {
      "operator": "<",
      "value": 1
    }
  ]
}]
```

`in` and `between` currently only supported in `<feature2>`.

Now, we turn to defined a feature or a feature set using a dict.
```
<feature> ::= {"<feature name>": <feature_qualifier>} 
<features> ::= {"<feature name>": <feature_qualifier>, ..., "<feature name>": <feature_qualifier>}
<feature2> ::= {"<feature name>": [<feature_qualifier>, ..., <feature_qualifier>]} 
<features2> ::= {"<feature name>": [<feature_qualifier>, ..., <feature_qualifier>], ..., "<feature name>": [<feature_qualifier>, ..., <feature_qualifier>]}
```

### create cohort
method
```
POST
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort
```
schema
```
<features>
```

### get cohort definition
method
```
GET
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/<cohort id>
```

### get cohort features
method
```
GET
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/<cohort id>/features
```

### get cohort dictionary
method
```
GET
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/dictionary
```

### feature association between two features
method
```
POST
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/<cohort id>/feature_association
```
schema
```
{"feature_a":<feature>,"feauture_b":<feature>}
```

### feature association between two features using combined bins
method
```
POST
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/<cohort id>/feature_association2
```
schema
```
{"feature_a":<feature2>,"feature_b":<feature2>,"check_coverage_is_full":<boolean>}
```
example
```
{
    "feature_a": {
      "feature_name": "AgeStudyStart",
      "feature_qualifiers": [
            {
                "operator":"=",
                "value":"0-2"
            }, {
                "operator":"between",
                "value_a":"3-17",
                "value_b":"18-34"
            }, {
                "operator":"in", 
                "values":["35-50","51-69"]
            },{
                "operator":"=",
                "value":"70+"
            }
      ]
    },
    "feature_b": {
      "feature_name": "ObesityBMI",
      "feature_qualifiers": [
            {
                "operator":"=",
                "value":0
            }, {
                "operator":"<>", 
                "value":0
            }
      ]
    }
}
```

### associations of one feature to all features
method
```
POST
```

route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/<cohort id>/associations_to_all_features
```
schema
```
{
  "feature": <feature>,
  "maximum_p_value": <maximum p value>,
  "correction": {
    "method": <correction method>,
    "alpha": <correction alpha>
  }
}
```
where `correction` is optional, `alpha` is optional. `method` and `alpha` are specified here: https://www.statsmodels.org/dev/generated/statsmodels.stats.multitest.multipletests.html

### associations of one feature to all features using combined bins
method
```
POST
```
route
```
/(patient|visit)/(2010|2011|2012|2013|2014|2015|2016)/cohort/<cohort id>/associations_to_all_features2
```
schema
```
{
  "feature": <feature>,
  "maximum_p_value": <maximum p value>, 
  "check_coverage_is_full": <boolean>,
  "correction": {
    "method": <correction method>,
    "alpha": <correction alpha>
  }
}
```
where `correction` is optional, `alpha` is optional. `method` and `alpha` are specified here: https://www.statsmodels.org/dev/generated/statsmodels.stats.multitest.multipletests.html

example
```
{
    "feature":{
        "AgeStudyStart":[
            {
                "operator":"=",
                "value":"0-2"
            }, {
                "operator":"between",
                "value_a":"3-17",
                "value_b":"18-34"
            }, {
                "operator":"in", 
                "values":["35-50","51-69"]
            },{
                "operator":"=",
                "value":"70+"
            }
        ]
    },
    "maximum_p_value": 0.1
}
```
### knowledge graph
method
```
POST
```

route
```
/knowledge_graph
```

input parameters:
 * `query_options`
   * `table` : ICEES table
   * `year` : ICEES year
   * `cohort_features`: features for defining the cohort
   * `feature`: a feature and operator and value for spliting the cohort to two subcohorts
   * `maximum_p_value`: ICEES maximum p value. The p value is calculated for each ICEES feature in `table`, using 2 * n contingency table where the rows are subcohorts and the columns are individual values of that feature. Any feature with p value greater than maximum p value is filtered out.
   * `regex`: filter target node name by regex.   
example
```
{
        "query_options": {
            "table": "patient", 
            "year": 2010, 
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }, 
            "feature": {
                "EstResidentialDensity": {
                    "operator": "<",
                    "value": 1
                }
            }, 
            "maximum_p_value":1
        }, 
        "machine_question": {
            "nodes": [
                {
                    "id": "n00",
                    "type": "population_of_individual_organisms"
                },
                {
                    "id": "n01",
                    "type": "chemical_substance"
                }   
            ], 
            "edges": [
                {
                    "id": "e00",
                    "type": "association",
                    "source_id": "n00",
                    "target_id": "n01"
                } 
            ]
        }
}
```

### knowledge graph overlay
method
```
POST
```

route
```
/knowledge_graph_overlay?reasoner=
```

input parameters:
```
<query_options> ::= {
                      "table": <string>,
                      "year": <integer>,
                      "cohort_features": <features>
                    }
                  | {
                      "cohort_id": <string>
                    }
```

```
{
   "message": {
      "query_options": <query_options>,
      "knowledge_graph": <knowledge_graph>
   }
}
```
if `reasoner` is specified, then it returns a Reason API response.

### knowledge graph one hop
method
```
POST
```

route
```
/knowledge_graph_one_hop?reasoner=
```

if `reasoner` is specified, then it returns a Reason API response.

input parameters:
```
{
   "query_options": <query_options>,
   "query_graph": <query_graph>
}
```


## Examples

get cohort of all patients

```
curl -k -XPOST https://localhost:8080/patient/2010/cohort -H "Content-Type: application/json" -H "Accept: application/json" -d '{}'
```

get cohort of all patients active in a year

```
curl -k -XPOST https://localhost:8080/patient/2010/cohort -H "Content-Type: application/json" -H "Accept: application/json" -d '[{
  "feature_name": "Active_In_Year",
  "feature_qualifier": {
    "operator": "=",
    "value": 1
  }
}]'
```

get cohort of patients with `AgeStudyStart = 0-2`

```
curl -k -XPOST https://localhost:8080/patient/2010/cohort -H "Content-Type: application/json" -H "Accept: application/json" -d '[{
  "feature_name": "AgeStudyStart",
  "feature_qualifier": {
    "operator":"=",
    "value":"0-2"
  }
}]'
```

Assuming we have cohort id `COHORT:10`

get definition of cohort

```
curl -k -XGET https://localhost:8080/patient/2010/cohort/COHORT:10 -H "Accept: application/json"
```

get features of cohort

```
curl -k -XGET https://localhost:8080/patient/2010/cohort/COHORT:10/features -H "Accept: application/json"
```

get cohort dictionary 

```
curl -k -XGET https://localhost:8080/patient/2010/cohort/COHORT:10/features -H "Accept: application/json"
```

get feature association


```
curl -k -XPOST https://localhost:8080/patient/2010/cohort/COHORT:10/feature_association -H "Content-Type: application/json" -d '{
  "feature_a": {
    "feature_name": "AgeStudyStart",
    "feature_qualifier: {"operator":"=", "value":"0-2"}
  },
  "feature_b": {
    "feature_name": "ObesityBMI",
    "feature_qualifier": {"operator":"=", "value":0}
  }
}'
```

get association to all features


```
curl -k -XPOST https://localhost:8080/patient/2010/cohort/COHORT:10/associations_to_all_features -H "Content-Type: application/json" -d '{
  "feature": {
    "feature_name": "AgeStudyStart",
    "feature_qualifier": {"operator":"=", "value":"0-2"}
  },
  "maximum_p_value":0.1
}' -H "Accept: application/json"
```

knowledge graph

```
curl -X POST -k "http://localhost:5000/knowledge_graph" -H  "accept: application/json" -H  "Content-Type: application/json" -d '
{
        "query_options": {
            "table": "patient", 
            "year": 2010, 
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }, 
            "feature": {
                "EstResidentialDensity": {
                    "operator": "<",
                    "value": 1
                }
            }, 
            "maximum_p_value":1
        }, 
        "machine_question": {
            "nodes": [
                {
                    "id": "n00",
                    "type": "population_of_individual_organisms"
                },
                {
                    "id": "n01",
                    "type": "chemical_substance"
                }   
            ], 
            "edges": [
                {
                    "id": "e00",
                    "type": "association",
                    "source_id": "n00",
                    "target_id": "n01"
                } 
            ]
        }
}
'
```

knowledge graph schema

```
curl -X GET -k "http://localhost:5000/knowledge_graph/schema" -H  "accept: application/json"
```

