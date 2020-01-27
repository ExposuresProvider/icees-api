import unittest
import requests
import json
from subprocess import Popen
import os
import time
import socket
import logging
import sys
import time
from features import features

wait = os.environ.get("WAIT")
if wait is not None:
    time.sleep(int(wait))

table = "patient"
year = 2010
tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}
host = "server" # "localhost"
prot = "https"  
port = 8080 # 8081

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
logger = logging.getLogger()

def wait(ip, port):
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((ip, port))
            break
        except:
            time.sleep(1)
        finally:
            s.close()

def query(year, biolink_class):
    return {
            "query_options": {
                "table": "patient", 
                "year": year, 
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
                        "type": biolink_class
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

def do_test_get_identifiers(i):
        feature_variables = {}
        resp = requests.get(prot + "://"+host+":"+str(port)+"/{0}/{1}/identifiers".format(table, i), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert "identifiers" in resp_json["return value"]
        for iden in resp_json["return value"]["identifiers"]:
            assert "_" not in iden

def test_post_cohort():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert "cohort_id" in resp_json["return value"]
        assert "size" in resp_json["return value"]

def test_cohort_dictionary():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()

        resp1 = requests.get(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort/dictionary".format(table, year), headers = json_headers, verify = False)
        resp_json1 = resp1.json()
        assert {
            "features": {}, 
            "cohort_id": resp_json["return value"]["cohort_id"], 
            "size": resp_json["return value"]["size"]
        } in resp_json1["return value"]
   
def test_get_identifiers_for_ObesityDx():
        do_test_get_identifiers("ObesityDx")

def test_get_identifiers_Sex2():
        do_test_get_identifiers("Sex2")

def test_get_identifiers_OvarianDysfunctionDx():
        do_test_get_identifiers("OvarianDysfunctionDx")

def test_get_identifiers_OvarianCancerDx():
        do_test_get_identifiers("OvarianCancerDx")

def test_associations_to_all_features2():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        atafdata = {
            "feature": {
                "AgeStudyStart": list(map(lambda x: {
                    "operator": "=",
                    "value": x
                }, features.age_levels))
            },
            "maximum_p_value": 1
        }
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort/{2}/associations_to_all_features2".format(table, year, cohort_id), data=json.dumps(atafdata), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert isinstance(resp_json["return value"], list)

def test_associations_to_all_features2b():
    feature_variables = {}
    resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
            "feature": {
                "AgeStudyStart": [
                    {
                        "operator": "=",
                        "value": "0-2"
                    }, {
                        "operator": "between",
                        "value_a": "3-17", 
                        "value_b": "18-34"
                    }, {
                        "operator":"in", 
                        "values":["35-50","51-69"]
                    }, {
                        "operator":"=",
                        "value":"70+"
                    }
                ]
            },
            "maximum_p_value": 1
        }
    resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort/{2}/associations_to_all_features2".format(table, year, cohort_id), data=json.dumps(atafdata), headers = json_headers, verify = False)
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)
