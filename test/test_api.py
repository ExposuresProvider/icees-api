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
cohort_year = 2011
tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}
host = "server" # "localhost"
prot = "http"  
port = 8080

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

    
def one_hop_query(curie, biolink_class, **kwargs):
    return {
        "message": {
            **kwargs,
            "query_graph": {
                "nodes": [
                    {
                        "id": "n00",
                        "curie": curie
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
    }

    
def do_verify_response(resp_json, results=True):
    assert "return value" in resp_json
    return_value = resp_json["return value"]
    assert "knowledge_graph" in return_value
    knowledge_graph = return_value["knowledge_graph"]
    nodes = knowledge_graph["nodes"]
    for node in nodes:
        if "equivalent_identifiers" in node:
            equivalent_ids = node["equivalent_identifiers"]
            assert isinstance(equivalent_ids, list) and all(isinstance(x, str) for x in equivalent_ids)
    node_ids_list = list(map(lambda x: x["id"], nodes))
    node_ids = set(node_ids_list)
    assert len(node_ids_list) == len(node_ids)
    edges = knowledge_graph["edges"]
    edge_ids_list = list(map(lambda x: x["id"], edges))
    edge_ids = set(edge_ids_list)
    assert len(edge_ids_list) == len(edge_ids)
    for edge in edges:
        assert edge["source_id"] in node_ids
        assert edge["target_id"] in node_ids

    assert "message_code" in resp_json["return value"]
    assert "tool_version" in resp_json["return value"]
    assert "datetime" in resp_json["return value"]

    if results:
        assert len(return_value["results"]) > 1
        assert "n_results" in return_value
        n_results = return_value["n_results"]
        assert "results" in return_value
        results = return_value["results"]
        assert n_results == len(results)
        for result in results:
            node_bindings = result["node_bindings"]
            edge_bindings = result["edge_bindings"]
            for node_binding_value in node_bindings.values():
                assert node_binding_value in node_ids
            for edge_binding_value in edge_bindings.values():
                assert set(edge_binding_value).issubset(edge_ids)
        
    
def do_verify_feature_matrix_response(respjson):
    assert isinstance(respjson, dict)
    assert "chi_squared" in respjson
    assert "p_value" in respjson
    assert "columns" in respjson
    assert "rows" in respjson
    assert "feature_matrix" in respjson

    
def do_test_knowledge_graph(biolink_class):

        resp = requests.post(prot + "://"+host+":"+str(port)+"/knowledge_graph", data = json.dumps(query(year, biolink_class)), headers = json_headers, verify = False)
        resp_json = resp.json()
        do_verify_response(resp_json)


def do_test_one_hop(curie, biolink_class, **kwargs):

        resp = requests.post(prot + "://"+host+":"+str(port)+"/knowledge_graph_one_hop", data = json.dumps(query(curie, biolink_class, **kwargs)), headers = json_headers, verify = False)
        resp_json = resp.json()
        do_verify_response(resp_json)


def do_test_knowledge_graph_overlay(**kwargs):

    query2 = {
        "message": {
            **kwargs, 
            "knowledge_graph": {
                "nodes": [
                    {
                        "id": "n00",
                        "curie": "PUBCHEM:2083",
                        "type": "drug"
                    },
                    {
                        "id": "n01",
                        "curie": "MESH:D052638",
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
    }
    resp = requests.post(prot + "://"+host+":"+str(port)+"/knowledge_graph_overlay", data = json.dumps(query2), headers = json_headers, verify = False)
    resp_json = resp.json()
    logger.info(json.dumps(resp_json, indent=4))
    do_verify_response(resp_json, results=False)


def do_test_knowledge_graph_one_hop(**kwargs):

    source_id = "PUBCHEM:2083"
    query2 = one_hop_query(source_id, "chemical_substance", **kwargs)
    resp = requests.post(prot + "://"+host+":"+str(port)+"/knowledge_graph_one_hop", data = json.dumps(query2), headers = json_headers, verify = False)
    resp_json = resp.json()
    logger.info(resp_json)
    assert "return value" in resp_json
    assert len(resp_json["return value"]["results"]) > 1

    assert "knowledge_graph" in resp_json["return value"]
    assert "nodes" in resp_json["return value"]["knowledge_graph"]
    assert any(map(lambda x: x["id"] == "PUBCHEM:2083", resp_json["return value"]["knowledge_graph"]["nodes"]))

    
    assert "message_code" in resp_json["return value"]
    assert "tool_version" in resp_json["return value"]
    assert "datetime" in resp_json["return value"]


def test_knowledge_graph_overlay_year_table_features():
    do_test_knowledge_graph_overlay(
        query_options = {
                "table": "patient", 
                "year": 2010, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })
    
    
def test_knowledge_graph_overlay_year_table_features():
    do_test_knowledge_graph_overlay(
        query_options = {
                "table": "patient", 
                "year": 2010, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })


def test_knowledge_graph_overlay_table_features():
    do_test_knowledge_graph_overlay(
        query_options = {
                "table": "patient", 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })


def test_knowledge_graph_overlay_year_features():
    do_test_knowledge_graph_overlay(
        query_options = {
                "year": 2010, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })


def test_knowledge_graph_overlay_table_year():

    do_test_knowledge_graph_overlay(
        query_options = {
                "table": "patient",
                "year": 2010
            })


def test_knowledge_graph_overlay():
    do_test_knowledge_graph_overlay()


def test_knowledge_graph_one_hop_year_table_features():
    do_test_knowledge_graph_one_hop(
        query_options = {
                "table": "patient", 
                "year": 2010, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })
    
    
def test_knowledge_graph_one_hop_year_table_features():
    do_test_knowledge_graph_one_hop(
        query_options = {
                "table": "patient", 
                "year": 2010, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })


def test_knowledge_graph_one_hop_table_features():
    do_test_knowledge_graph_one_hop(
        query_options = {
                "table": "patient", 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })


def test_knowledge_graph_one_hop_year_features():
    do_test_knowledge_graph_one_hop(
        query_options = {
                "year": 2010, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }
            })


def test_knowledge_graph_one_hop_table_year():

    do_test_knowledge_graph_one_hop(
        query_options = {
                "table": "patient",
                "year": 2010
            })


def test_knowledge_graph_one_hop():
    do_test_knowledge_graph_one_hop()


def do_test_knowledge_graph_unique_edge_ids(biolink_class):

        resp = requests.post(prot + "://"+host+":"+str(port)+"/knowledge_graph", data = json.dumps(query(year, biolink_class)), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json

        assert len(resp_json["return value"]["results"]) > 1

        for edge_bindings in map(lambda x: x["edge_bindings"], resp_json["return value"]["results"]):
            assert "e00" in edge_bindings
            assert len(edge_bindings) == 1
            assert len(edge_bindings["e00"]) == 1

        edge_ids = list(map(lambda x: x["edge_bindings"]["e00"][0], resp_json["return value"]["results"]))
        assert len(edge_ids) == len(set(edge_ids))


def do_test_knowledge_graph_edge_set(biolink_class):

        resp = requests.post(prot + "://"+host+":"+str(port)+"/knowledge_graph", data = json.dumps(query(year, biolink_class)), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json

        assert len(resp_json["return value"]["results"]) > 1

        edge_ids = set(map(lambda x: x["edge_bindings"]["e00"][0], resp_json["return value"]["results"]))
        edge_ids2 = set(map(lambda x: x["id"], resp_json["return value"]["knowlegde_graph"]["edges"]))
        assert edge_ids == edge_ids2


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
   
def test_knowledge_graph_schema():
        resp = requests.get(prot + "://"+host+":"+str(port)+"/knowledge_graph/schema", headers = json_headers, verify = False)
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

def test_knowledge_graph_edge_set_for_chemical_substance():
        do_test_knowledge_graph_unique_edge_ids("chemical_substance")

def test_knowledge_graph_edge_set_for_phenotypic_feature():
        do_test_knowledge_graph_unique_edge_ids("phenotypic_feature")

def test_knowledge_graph_edge_set_for_disease():
        do_test_knowledge_graph_unique_edge_ids("disease")

def test_get_identifiers_for_ObesityDx():
        do_test_get_identifiers("ObesityDx")

def test_get_identifiers_Sex2():
        do_test_get_identifiers("Sex2")

def test_get_identifiers_OvarianDysfunctionDx():
        do_test_get_identifiers("OvarianDysfunctionDx")

def test_get_identifiers_OvarianCancerDx():
        do_test_get_identifiers("OvarianCancerDx")

def test_feature_association():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        age_levels = next(feature[2] for feature in features.features['patient'] if feature[0] == 'AgeStudyStart')
        atafdata = {
            "feature_a": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": '0-2'
                }
            },
            "feature_b": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": '0-2'
                }
            }
        }
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort/{2}/feature_association".format(table, year, cohort_id), data=json.dumps(atafdata), headers = json_headers, verify = False)
        print(resp.text)
        resp_json = resp.json()
        assert "return value" in resp_json
        do_verify_feature_matrix_response(resp_json["return value"])

        
def test_feature_association_two_years():
        cohort_year = 2010
        year = 2011
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, cohort_year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        print(resp_json)
        cohort_id = resp_json["return value"]["cohort_id"]
        age_levels = next(feature[2] for feature in features.features['patient'] if feature[0] == 'AgeStudyStart')
        atafdata = {
            "feature_a": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": '0-2'
                }
            },
            "feature_b": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": '0-2'
                }
            }
        }
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort/{2}/feature_association".format(table, year, cohort_id), data=json.dumps(atafdata), headers = json_headers, verify = False)
        resp_json = resp.json()
        assert "return value" in resp_json
        do_verify_feature_matrix_response(resp_json["return value"])


def test_associations_to_all_features2():
        feature_variables = {}
        resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        age_levels = next(feature[2] for feature in features.features['patient'] if feature[0] == 'AgeStudyStart')
        atafdata = {
            "feature": {
                "AgeStudyStart": list(map(lambda x: {
                    "operator": "=",
                    "value": x
                }, age_levels))
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
                        "value":"70-89"
                    }
                ]
            },
            "maximum_p_value": 1
        }
    resp = requests.post(prot + "://"+host+":"+str(port)+"/{0}/{1}/cohort/{2}/associations_to_all_features2".format(table, year, cohort_id), data=json.dumps(atafdata), headers = json_headers, verify = False)
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)

