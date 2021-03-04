"""Test API."""
import json
import logging
import os
import socket
import sys
import time

from fastapi.testclient import TestClient

from icees_api.app import APP
from icees_api.features.sql import get_features


testclient = TestClient(APP)

wait = os.environ.get("WAIT")
if wait is not None:
    time.sleep(int(wait))

table = "patient"
year = 2010
cohort_year = 2011
tabular_headers = {"Content-Type": "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type": "application/json", "accept": "application/json"}
# host = "server"
host = "localhost"
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
            "maximum_p_value":1.1
        },
        "message": {
            "query_graph": {
                "nodes": {
                    "n00": {
                        "category": "biolink:PopulationOfIndividualOrganisms"
                    },
                    "n01": {
                        "category": biolink_class
                    }
                },
                "edges": {
                    "e00": {
                        "predicate": "biolink:correlated_with",
                        "subject": "n00",
                        "object": "n01"
                    }
                }
            }
        }
    }


def one_hop_query(curie, biolink_class, **kwargs):
    return {
        **kwargs,
        "message": {
            "query_graph": {
                "nodes": {
                    "n00": {
                        "id": curie
                    },
                    "n01": {
                        "category": biolink_class
                    }
                }, 
                "edges": {
                    "e00": {
                        "predicate": "biolink:correlated_with",
                        "subject": "n00",
                        "object": "n01"
                    } 
                }
            }
        }
    }

    
def do_verify_response(resp_json, results=True):
    """Perform basic formatting checks on response object.

    knode keys should be unique
    kedge keys should be unique
    equivalent_identifiers should be a list of strings
    kedges should reference real knodes
    message_code, tool_version, and datetime should exist
    result bindings should reference real kgraph elements
    """
    assert "return value" in resp_json
    return_value = resp_json["return value"]
    assert "knowledge_graph" in return_value["message"]
    knowledge_graph = return_value["message"]["knowledge_graph"]
    nodes = knowledge_graph["nodes"]
    for node in nodes:
        if "equivalent_identifiers" in node:
            equivalent_ids = node["equivalent_identifiers"]
            assert isinstance(equivalent_ids, list) and all(isinstance(x, str) for x in equivalent_ids)
    node_ids_list = list(nodes)
    node_ids = set(node_ids_list)
    assert len(node_ids_list) == len(node_ids)
    edges = knowledge_graph["edges"]
    edge_ids_list = list(edges)
    edge_ids = set(edge_ids_list)
    assert len(edge_ids_list) == len(edge_ids)
    for edge in edges.values():
        assert edge["subject"] in node_ids
        assert edge["object"] in node_ids

    assert "message_code" in resp_json["return value"]
    assert "tool_version" in resp_json["return value"]
    assert "datetime" in resp_json["return value"]

    if results:
        assert len(return_value["message"]["results"]) > 1
        assert "n_results" in return_value
        n_results = return_value["n_results"]
        assert "results" in return_value["message"]
        results = return_value["message"]["results"]
        assert n_results == len(results)
        for result in results:
            node_bindings = result["node_bindings"]
            edge_bindings = result["edge_bindings"]
            for node_binding_value in node_bindings.values():
                assert all(nbv["id"] in node_ids for nbv in node_binding_value)
            for edge_binding_value in edge_bindings.values():
                assert all(ebv["id"] in edge_ids for ebv in edge_binding_value)


def do_verify_feature_matrix_response(respjson):
    assert isinstance(respjson, dict)
    assert "chi_squared" in respjson
    assert "p_value" in respjson
    assert "columns" in respjson
    assert "rows" in respjson
    assert "feature_matrix" in respjson


def do_verify_feature_count_response(respjson):
    assert isinstance(respjson, list)
    for feature_count in respjson:
        assert "feature" in feature_count
        feature = feature_count["feature"]
        assert "feature_name" in feature
        assert "feature_qualifiers" in feature
        feature_qualifiers = feature["feature_qualifiers"]
        assert isinstance(feature_qualifiers, list)
        for feature_qualifier in feature_qualifiers:
            assert "operator" in feature_qualifier
            assert "value" in feature_qualifier
        assert "feature_matrix" in feature_count
        for stats in feature_count["feature_matrix"]:
            assert "frequency" in stats
            assert "percentage" in stats


def do_test_knowledge_graph(biolink_class):
    resp = testclient.post(
        "/knowledge_graph",
        data=json.dumps(query(year, biolink_class)),
        headers=json_headers,
    )
    resp_json = resp.json()
    do_verify_response(resp_json)


def do_test_one_hop(curie, biolink_class, **kwargs):
    resp = testclient.post(
        "/knowledge_graph_one_hop",
        data=json.dumps(query(curie, biolink_class, **kwargs)),
        headers=json_headers,
    )
    resp_json = resp.json()
    do_verify_response(resp_json)


def do_test_knowledge_graph_overlay(**kwargs):
    query2 = {
        **kwargs,
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "PUBCHEM:2083": {
                        "category": "biolink:Drug"
                    },
                    "MESH:D052638": {
                        "category": "biolink:ChemicalSubstance"
                    }   
                },
                "edges": {
                    "e00": {
                        "predicate": "biolink:association",
                        "subject": "PUBCHEM:2083",
                        "object": "MESH:D052638"
                    }
                }
            }
        }
    }
    resp = testclient.post(
        "/knowledge_graph_overlay",
        data=json.dumps(query2),
        headers=json_headers,
    )
    resp_json = resp.json()
    logger.info(json.dumps(resp_json, indent=4))
    do_verify_response(resp_json, results=False)


def do_test_knowledge_graph_one_hop(**kwargs):
    source_id = "PUBCHEM:2083"
    query2 = one_hop_query(source_id, "biolink:ChemicalSubstance", **kwargs)
    resp = testclient.post(
        "/knowledge_graph_one_hop",
        json=query2,
    )
    resp_json = resp.json()
    logger.info(resp_json)
    assert "return value" in resp_json
    assert len(resp_json["return value"]["message"]["results"]) > 1

    assert "knowledge_graph" in resp_json["return value"]["message"]
    assert "nodes" in resp_json["return value"]["message"]["knowledge_graph"]
    assert "PUBCHEM:2083" in resp_json["return value"]["message"]["knowledge_graph"]["nodes"]

    
    assert "message_code" in resp_json["return value"]
    assert "tool_version" in resp_json["return value"]
    assert "datetime" in resp_json["return value"]


def test_knowledge_graph_overlay_year_table_features():
    do_test_knowledge_graph_overlay(
        query_options={
            "table": "patient",
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_overlay_year_table_features():
    do_test_knowledge_graph_overlay(
        query_options={
            "table": "patient",
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_overlay_table_features():
    do_test_knowledge_graph_overlay(
        query_options={
            "table": "patient",
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_overlay_year_features():
    do_test_knowledge_graph_overlay(
        query_options={
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_overlay_table_year():
    do_test_knowledge_graph_overlay(
        query_options = {
            "table": "patient",
            "year": 2010
        }
    )


def test_knowledge_graph_overlay():
    do_test_knowledge_graph_overlay()


def test_knowledge_graph_one_hop_year_table_features():
    do_test_knowledge_graph_one_hop(
        query_options={
            "table": "patient",
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )
    
    
def test_knowledge_graph_one_hop_year_table_features():
    do_test_knowledge_graph_one_hop(
        query_options={
            "table": "patient",
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_one_hop_table_features():
    do_test_knowledge_graph_one_hop(
        query_options={
            "table": "patient",
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_one_hop_year_features():
    do_test_knowledge_graph_one_hop(
        query_options={
            "year": 2010,
            "cohort_features": {
                "AgeStudyStart": {
                    "operator": "=",
                    "value": "0-2"
                }
            }
        }
    )


def test_knowledge_graph_one_hop_table_year():
    do_test_knowledge_graph_one_hop(
        query_options = {
            "table": "patient",
            "year": 2010
        }
    )


def test_knowledge_graph_one_hop():
    do_test_knowledge_graph_one_hop()


def do_test_knowledge_graph_unique_edge_ids(biolink_class):
    resp = testclient.post(
        "/knowledge_graph",
        json=query(year, biolink_class),
    )
    resp_json = resp.json()
    assert "return value" in resp_json

    assert len(resp_json["return value"]["message"]["results"]) > 1

    for edge_bindings in map(lambda x: x["edge_bindings"], resp_json["return value"]["message"]["results"]):
        assert "e00" in edge_bindings
        assert len(edge_bindings) == 1
        assert len(edge_bindings["e00"]) == 1

    edge_ids = list(map(lambda x: x["edge_bindings"]["e00"][0]["id"], resp_json["return value"]["message"]["results"]))
    assert len(edge_ids) == len(set(edge_ids))


def do_test_knowledge_graph_edge_set(biolink_class):
    resp = testclient.post(
        "/knowledge_graph",
        data=json.dumps(query(year, biolink_class)),
        headers=json_headers,
    )
    resp_json = resp.json()
    assert "return value" in resp_json

    assert len(resp_json["return value"]["results"]) > 1

    edge_ids = set(map(lambda x: x["edge_bindings"]["e00"][0], resp_json["return value"]["results"]))
    edge_ids2 = set(map(lambda x: x["id"], resp_json["return value"]["knowlegde_graph"]["edges"]))
    assert edge_ids == edge_ids2


def do_test_get_identifiers(i):
    feature_variables = {}
    resp = testclient.get("/{0}/{1}/identifiers".format(table, i), headers = json_headers)
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "identifiers" in resp_json["return value"]
    for iden in resp_json["return value"]["identifiers"]:
        assert "_" not in iden


def test_post_cohort():
    feature_variables = {}
    resp = testclient.post("/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers)
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "cohort_id" in resp_json["return value"]
    assert "size" in resp_json["return value"]


def test_cohort_dictionary():
    feature_variables = {}
    resp = testclient.post("/{0}/{1}/cohort".format(table, year), data=json.dumps(feature_variables), headers = json_headers)
    resp_json = resp.json()

    resp1 = testclient.get("/{0}/{1}/cohort/dictionary".format(table, year), headers = json_headers)
    resp_json1 = resp1.json()
    assert {
        "features": {}, 
        "cohort_id": resp_json["return value"]["cohort_id"], 
        "size": resp_json["return value"]["size"]
    } in resp_json1["return value"]
   

def test_knowledge_graph_schema():
    resp = testclient.get("/knowledge_graph/schema", headers = json_headers)
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "population_of_individual_organisms" in resp_json["return value"]
    assert "chemical_substance" in resp_json["return value"]["population_of_individual_organisms"]
    assert "correlated_with" in resp_json["return value"]["population_of_individual_organisms"]["chemical_substance"]


def test_knowledge_graph_for_chemical_substance():
    do_test_knowledge_graph("biolink:ChemicalSubstance")


def test_knowledge_graph_for_phenotypic_feature():
    do_test_knowledge_graph("biolink:PhenotypicFeature")


def test_knowledge_graph_for_disease():
        do_test_knowledge_graph("biolink:Disease")


def test_knowledge_graph_unique_edge_ids_for_chemical_substance():
        do_test_knowledge_graph_unique_edge_ids("biolink:ChemicalSubstance")


def test_knowledge_graph_unique_edge_ids_for_phenotypic_feature():
        do_test_knowledge_graph_unique_edge_ids("biolink:PhenotypicFeature")


def test_knowledge_graph_unique_edge_ids_for_disease():
        do_test_knowledge_graph_unique_edge_ids("biolink:Disease")


def test_knowledge_graph_edge_set_for_chemical_substance():
        do_test_knowledge_graph_unique_edge_ids("biolink:ChemicalSubstance")


def test_knowledge_graph_edge_set_for_phenotypic_feature():
        do_test_knowledge_graph_unique_edge_ids("biolink:PhenotypicFeature")


def test_knowledge_graph_edge_set_for_disease():
        do_test_knowledge_graph_unique_edge_ids("biolink:Disease")


def test_get_identifiers_for_ObesityDx():
        do_test_get_identifiers("ObesityDx")


def test_get_identifiers_Sex2():
        do_test_get_identifiers("Sex2")


def test_get_identifiers_OvarianDysfunctionDx():
        do_test_get_identifiers("OvarianDysfunctionDx")


def test_get_identifiers_OvarianCancerDx():
        do_test_get_identifiers("OvarianCancerDx")


age_levels = [
    '0-2',
    '3-17',
    '18-34',
    '35-50',
    '51-69',
    '70-89',
]


def test_feature_association():
    feature_variables = {}
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        data=json.dumps(feature_variables),
        headers=json_headers,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
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
    resp = testclient.post(
        f"/{table}/{year}/cohort/{cohort_id}/feature_association",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_matrix_response(resp_json["return value"])

        
def test_feature_association_explicit():
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature_a": {
            "feature_name": "AgeStudyStart",
            "feature_qualifier": {
                "operator": "=",
                "value": '0-2'
            }
        },
        "feature_b": {
            "feature_name": "AgeStudyStart",
            "feature_qualifier": {
                "operator": "=",
                "value": '0-2'
            }
        }
    }
    resp = testclient.post(
        f"/{table}/{year}/cohort/{cohort_id}/feature_association",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_matrix_response(resp_json["return value"])

        
def test_feature_association2_explicit_check_coverage_is_full():
        feature_variables = {}
        resp = testclient.post(
            "/{0}/{1}/cohort".format(table, year),
            json=feature_variables,
        )
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        atafdata = {
            "feature_a": {
                "feature_name": "AgeStudyStart",
                "feature_qualifiers": list(map(lambda x: {
                    "operator": "=",
                    "value": x
                }, age_levels))
            },
            "feature_b": {
                "feature_name": "AgeStudyStart",
                "feature_qualifiers": [{
                    "operator": ">",
                    "value": '0-2'
                }, {
                    "operator": "<=",
                    "value": '0-2'
                }]
            },
            "check_coverage_is_full": True
        }
        resp = testclient.post(
            f"/{table}/{year}/cohort/{cohort_id}/feature_association2",
            data=json.dumps(atafdata),
            headers=json_headers,
        )
        resp_json = resp.json()
        assert "return value" in resp_json
        do_verify_feature_matrix_response(resp_json["return value"])

        
def test_feature_association2_explicit_check_coverage_is_full_2():
        feature_variables = {}
        resp = testclient.post(
            f"/{table}/{year}/cohort",
            json=feature_variables,
        )
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        atafdata = {
            "feature_a": {
                "feature_name": "AgeStudyStart",
                "feature_qualifiers": list(map(lambda x: {
                    "operator": "=",
                    "value": x
                }, age_levels))
            },
            "feature_b": {
                "feature_name": "AgeStudyStart",
                "feature_qualifiers": [{
                    "operator": ">",
                    "value": '0-2'
                }]
            },
            "check_coverage_is_full": True
        }
        resp = testclient.post(
            f"/{table}/{year}/cohort/{cohort_id}/feature_association2",
            json=atafdata,
        )
        resp_json = resp.json()
        assert "return value" in resp_json
        assert isinstance(resp_json["return value"], str)


def test_feature_association2_explicit_check_coverage_is_full_3():
        feature_variables = {}
        resp = testclient.post(
            "/{0}/{1}/cohort".format(table, year),
            json=feature_variables,
        )
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
        atafdata = {
            "feature_a": {
                "feature_name": "AgeStudyStart",
                "feature_qualifiers": list(map(lambda x: {
                    "operator": "=",
                    "value": x
                }, age_levels))[1:]
            },
            "feature_b": {
                "feature_name": "AgeStudyStart",
                "feature_qualifiers": [{
                    "operator": ">",
                    "value": '0-2'
                }, {
                    "operator": "<=",
                    "value": '0-2'
                }]
            },
            "check_coverage_is_full": True
        }
        resp = testclient.post(
            f"/{table}/{year}/cohort/{cohort_id}/feature_association2",
            data=json.dumps(atafdata),
            headers=json_headers,
        )
        resp_json = resp.json()
        assert "return value" in resp_json
        assert isinstance(resp_json["return value"], str)


def test_feature_association_two_years():
        cohort_year = 2010
        year = 2011
        feature_variables = {}
        resp = testclient.post(
            "/{0}/{1}/cohort".format(table, cohort_year),
            json=feature_variables,
        )
        resp_json = resp.json()
        cohort_id = resp_json["return value"]["cohort_id"]
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
        resp = testclient.post(
            f"/{table}/{year}/cohort/{cohort_id}/feature_association",
            json=atafdata,
        )
        resp_json = resp.json()
        assert "return value" in resp_json
        do_verify_feature_matrix_response(resp_json["return value"])


def test_feature_association_cohort_features_two_years():
    """
    See test/sql_example_2.txt for the SQL query this generates.
    It forms a cohort by finding rows matching each feature variable's
    conditions, then joining.
    This means that we get one row for _each time a patient was Male_ along
    with that patient's 2011 features (per the year parameter on the
    /feature_association call?)
    This seems like probably not the intended behavior? It's computing counts
    over _each time a patient was Male_ rather than over patients...

    Questions:
    1. What is this operation supposed to mean, in English?
    1. What is the cohort_year supposed to do?
    1. Can we include multiple years as distinct sets of columns rather than
       additional rows with duplicate patient ids? Then we could avoid joins
       entirely, I think, and this could be ~50x faster.
       * Possibly joins should not be as slow as they are. SQLite uses only
         nested-loop joins: O(NM).
    """
    cohort_year = 2010
    feature_variables = [
        {
            "feature_name": "Sex",
            "feature_qualifier": {
                "operator": "=",
                "value": "Male"
            }
        },
        {
            "feature_name": "AvgDailyPM2.5Exposure_StudyAvg",
            "feature_qualifier": {
                "operator": ">",
                "value": 1
            },
            "year": 2011
        }
    ]
    resp = testclient.post(
        f"/{table}/{cohort_year}/cohort",
        json=feature_variables,
    )
    year = 2011
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
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
    resp = testclient.post(
        "/{0}/{1}/cohort/{2}/feature_association".format(table, year, cohort_id),
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_matrix_response(resp_json["return value"])


def test_feature_count_cohort_features_two_years():
    cohort_year = 2010
    year = 2011
    feature_variables = [
        {
            "feature_name": "Sex",
            "feature_qualifier": {
                "operator": "=",
                "value": "Male"
            }
        }, {
            "feature_name": "AvgDailyPM2.5Exposure_StudyAvg",
            "feature_qualifier": {
                "operator": "=",
                "value": 1
            },
            "year": 2011
        }
    ]
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, cohort_year),
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]

    resp = testclient.get(
        "/{0}/{1}/cohort/{2}/features".format(table, year, cohort_id),
        headers=json_headers,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_count_response(resp_json["return value"])


def test_associations_to_all_features_explicit():
    feature_variables = {}
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "feature_name": "AgeStudyStart",
            "feature_qualifier": {
                "operator": "=",
                "value": "0-2"
            }
        },
        "maximum_p_value": 1
    }
    resp = testclient.post(
        "/{0}/{1}/cohort/{2}/associations_to_all_features".format(table, year, cohort_id),
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


def test_associations_to_all_features_explicit_non_integer_p_value():
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "feature_name": "AgeStudyStart",
            "feature_qualifier": {
                "operator": "=",
                "value": "0-2"
            }
        },
        "maximum_p_value": 0.5
    }
    resp = testclient.post(
        f"/{table}/{year}/cohort/{cohort_id}/associations_to_all_features",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


def test_associations_to_all_features_with_correction():
    feature_variables = {}
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "AgeStudyStart": {
                "operator": "=",
                "value": "0-2"
            }
        },
        "correction": {
            "method": "bonferroni"
        },
        "maximum_p_value": 1
    }
    resp = testclient.post(
        "/{0}/{1}/cohort/{2}/associations_to_all_features".format(table, year, cohort_id),
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


def test_associations_to_all_features_with_correction_with_alpha():
    feature_variables = {}
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "AgeStudyStart": {
                "operator": "=",
                "value": "0-2"
            }
        },
        "correction": {
            "method": "fdr_tsbh",
            "alpha": 0.1
        },
        "maximum_p_value": 1
    }
    resp = testclient.post(
        "/{0}/{1}/cohort/{2}/associations_to_all_features".format(table, year, cohort_id),
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)

        
def test_associations_to_all_features2_explicit():
    feature_variables = {}
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "feature_name": "AgeStudyStart",
            "feature_qualifiers": list(map(lambda x: {
                "operator": "=",
                "value": x
            }, age_levels))
        },
        "maximum_p_value": 1
    }
    resp = testclient.post(
        "/{0}/{1}/cohort/{2}/associations_to_all_features2".format(table, year, cohort_id),
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


def test_associations_to_all_features2():
    feature_variables = {}
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "AgeStudyStart": list(map(lambda x: {
                "operator": "=",
                "value": x
            }, age_levels))
        },
        "maximum_p_value": 1
    }
    resp = testclient.post(
        "/{0}/{1}/cohort/{2}/associations_to_all_features2".format(table, year, cohort_id),
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


def test_associations_to_all_features2b():
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]
    atafdata = {
        "feature": {
            "AgeStudyStart": [
                {
                    "operator": "=",
                    "value": "0-2"
                }, {
                    "operator": "in",
                    "values": ["3-17", "18-34"]
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
    resp = testclient.post(
        f"/{table}/{year}/cohort/{cohort_id}/associations_to_all_features2",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


def test_cohort_dictionary_explicit_tabular():
    feature_variables = [{
        "feature_name": "AgeStudyStart",
        "feature_qualifier": {
            "operator": "=",
            "value": "0-2"
        }
    }]
    resp = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
    )
    resp2 = testclient.post(
        "/{0}/{1}/cohort".format(table, year),
        json=feature_variables,
        headers={"Content-Type": "application/json", "Accept": "text/tabular"},
    )
    
    assert resp2.status_code == 200


def test_get_features():
    """Test get_features()."""
    print(get_features("patient"))
