import unittest
import requests
import json
from subprocess import Popen
import os
import time
import socket
import logging
import sys
from features import features

version = "2.0.0"
table = "patient"
year = 2010
tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}
host = "localhost"
prot = "https"  
port = 8081

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

    
def do_test_knowledge_graph(biolink_class):

        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/knowledge_graph".format(version), data = json.dumps(query(year, biolink_class)), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert "n_results" in resp_json["return value"]
        assert "answers" in resp_json["return value"]
        assert resp_json["return value"]["n_results"] == len(resp_json["return value"]["answers"])
        assert "knowledge_graph" in resp_json["return value"]
        assert "message_code" in resp_json["return value"]
        assert "tool_version" in resp_json["return value"]
        assert "datetime" in resp_json["return value"]


def do_test_knowledge_graph_unique_edge_ids(biolink_class):

        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/knowledge_graph".format(version), data = json.dumps(query(year, biolink_class)), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json

        for edge_bindings in map(lambda x: x["edge_bindings"], resp_json["return value"]["answers"]):
            assert "e00" in edge_bindings
            assert len(edge_bindings) == 1
            assert len(edge_bindings["e00"]) == 1

        edge_ids = list(map(lambda x: x["edge_bindings"]["e00"][0], resp_json["return value"]["answers"]))
        assert len(edge_ids) == len(set(edge_ids))


def do_test_get_identifiers(i):
        feature_variables = {}
        resp = requests.get(prot + "://"+host+":"+str(port)+"/{0}/{1}/{2}/identifiers".format(version, table, i), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert "identifiers" in resp_json["return value"]
        for iden in resp_json["return value"]["identifiers"]:
            assert "_" not in iden

def test_post_cohort():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/{2}/cohort".format(version, table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert "cohort_id" in resp_json["return value"]
        assert "size" in resp_json["return value"]

def test_cohort_dictionary():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/{2}/cohort".format(version, table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()

        resp1 = requests.get(prot + "://"+host+":"+str(port)+"/{0}/{1}/{2}/cohort/dictionary".format(version, table, year), headers = json_headers, verify = False)
        resp_json1 = resp1.json()
        assert {
            "features": {}, 
            "cohort_id": resp_json["return value"]["cohort_id"], 
            "size": resp_json["return value"]["size"]
        } in resp_json1["return value"]
   
def test_knowledge_graph_schema():
        resp = requests.get(prot + "://"+host+":"+str(port)+"/{0}/knowledge_graph/schema".format(version), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert "population_of_individual_organisms" in resp_json["return value"]
        assert "chemical_substance" in resp_json["return value"]["population_of_individual_organisms"]
        assert "association" in resp_json["return value"]["population_of_individual_organisms"]["chemical_substance"]

def test_knowledge_graph_for_chemical_substance():
        do_test_knowledge_graph("chemical_substance")

def test_knowledge_graph_for_phenotypic_feature():
        do_test_knowledge_graph("phenotypic_feature")

def test_knowledge_graph_for_disease():
        do_test_knowledge_graph("disease")

def test_knowledge_graph_unique_edge_ids_for_chemical_substance():
        do_test_knowledge_graph_unique_edge_ids("chemical_substance")

def test_knowledge_graph_unique_edge_ids_for_phenotypic_feature():
        do_test_knowledge_graph_unique_edge_ids("phenotypic_feature")

def test_knowledge_graph_unique_edge_ids_for_disease():
        do_test_knowledge_graph_unique_edge_ids("disease")

def test_get_identifiers_for_():
        do_test_get_identifiers("ObesityDx")

def test_get_identifiers_Sex2():
        do_test_get_identifiers("Sex2")

def test_get_identifiers_OvarianDysfunctionDx():
        do_test_get_identifiers("OvarianDysfunctionDx")

def test_get_identifiers_OvarianCancerDx():
        do_test_get_identifiers("OvarianCancerDx")

def test_associations_to_all_features2():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/{2}/cohort".format(version, table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        atafdata = {
            "feature": {
                "AgeStudyStart": list(map(lambda x: {
                    "operator": "=",
                    "value": x
                }, features[version].age_levels))
            },
            "maximum_p_value": 1
        }
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/{2}/cohort/{3}/associations_to_all_features2".format(version, table, year, cohort_id), data=json.dumps(atafdata), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        assert isinstance(resp_json["return value"], list)

