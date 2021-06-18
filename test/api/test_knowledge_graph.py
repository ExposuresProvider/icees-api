"""Test /knowledge_graph* endpoints."""
from fastapi.testclient import TestClient
import pytest

from reasoner_validator import validate

from icees_api.app import APP

from ..util import load_data, query, do_verify_response

testclient = TestClient(APP)
year = 2010
kg_options = [
    {
        "table": "patient",
        "year": 2010,
        "cohort_features": {
            "AgeStudyStart": {
                "operator": "=",
                "value": "0-2"
            }
        }
    },
    {
        "table": "patient",
        "cohort_features": {
            "AgeStudyStart": {
                "operator": "=",
                "value": "0-2"
            }
        }
    },
    {
        "year": 2010,
        "cohort_features": {
            "AgeStudyStart": {
                "operator": "=",
                "value": "0-2"
            }
        }
    },
    {
        "table": "patient",
        "year": 2010
    },
    None,
]

TRAPI_VERSION="1.1.0"
def validate_response(resp, trapi_version=None):
    resp_json = resp.json()

    if trapi_version is not None:
        validate(resp_json, "Response", trapi_version=trapi_version)
    return resp_json
    

@pytest.mark.parametrize("query_options", kg_options)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure
    varchar(255),int,varchar(255),varchar(255),int
    1,2010,0-2,0,1
    2,2010,0-2,1,2
    3,2010,0-2,>1,3
""")
def test_knowledge_graph_overlay(query_options):
    """Test knowlege graph overlay."""
    query = {
        "query_options": query_options,
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "PUBCHEM:2083": {
                        "categories": ["biolink:Drug"]
                    },
                    "MESH:D052638": {
                        "categories": ["biolink:ChemicalSubstance"]
                    }
                },
                "edges": {}
            }
        }
    }
    resp_json = validate_response(testclient.post(
        "/knowledge_graph_overlay",
        json=query,
    ))

    do_verify_response(resp_json, results=False)


@pytest.mark.parametrize("query_options", kg_options)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure
    varchar(255),int,varchar(255),varchar(255),int
    1,2010,0-2,0,1
    2,2010,0-2,1,1
    3,2010,0-2,>1,1
    4,2010,0-2,0,2
    5,2010,0-2,1,2
    6,2010,0-2,>1,2
    7,2010,0-2,0,3
    8,2010,0-2,1,3
    9,2010,0-2,>1,3
    10,2010,0-2,0,4
    11,2010,0-2,1,4
    12,2010,0-2,>1,4
""")
def test_knowledge_graph_one_hop(query_options):
    """Test one-hop."""
    source_id = "PUBCHEM:2083"
    query = {
        "query_options": query_options,
        "message": {
            "query_graph": {
                "nodes": {
                    "n00": {
                        "id": source_id
                    },
                    "n01": {
                        "category": "biolink:ChemicalSubstance"
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
    resp_json = validate_response(testclient.post(
        "/knowledge_graph_one_hop",
        json=query,
        params={"reasoner": False},
    ))
    assert "return value" in resp_json
    assert len(resp_json["return value"]["message"]["results"]) == 2

    assert "knowledge_graph" in resp_json["return value"]["message"]
    assert "nodes" in resp_json["return value"]["message"]["knowledge_graph"]
    assert "PUBCHEM:2083" in resp_json["return value"]["message"]["knowledge_graph"]["nodes"]

    assert "message_code" in resp_json["return value"]
    assert "tool_version" in resp_json["return value"]
    assert "datetime" in resp_json["return value"]


@pytest.mark.parametrize("query_options", kg_options)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure
    varchar(255),int,varchar(255),varchar(255),int
    1,2010,0-2,0,1
    2,2010,0-2,1,1
    3,2010,0-2,>1,1
    4,2010,0-2,0,2
    5,2010,0-2,1,2
    6,2010,0-2,>1,2
    7,2010,0-2,0,3
    8,2010,0-2,1,3
    9,2010,0-2,>1,3
    10,2010,0-2,0,4
    11,2010,0-2,1,4
    12,2010,0-2,>1,4
""")
def test_knowledge_graph_one_hop_valid_trapi_1_1_response(query_options):
    """Test one-hop return valid TRAPI 1.1 response."""
    source_id = "PUBCHEM:2083"
    query = {
        "query_options": query_options,
        "message": {
            "query_graph": {
                "nodes": {
                    "n00": {
                        "id": source_id
                    },
                    "n01": {
                        "category": "biolink:ChemicalSubstance"
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
    resp_json = validate_response(testclient.post(
        "/knowledge_graph_one_hop",
        json=query
    ), trapi_version=TRAPI_VERSION)
    assert "message" in resp_json
    assert "results" in resp_json["message"]
    assert len(resp_json["message"]["results"]) == 2

    assert "knowledge_graph" in resp_json["message"]
    assert "nodes" in resp_json["message"]["knowledge_graph"]
    assert "PUBCHEM:2083" in resp_json["message"]["knowledge_graph"]["nodes"]

    assert "message_code" in resp_json
    assert "tool_version" in resp_json
    assert "datetime" in resp_json


@pytest.mark.parametrize("query_options", kg_options)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure
    varchar(255),int,varchar(255),varchar(255),int
    1,2010,0-2,0,1
    2,2010,0-2,1,1
    3,2010,0-2,>1,1
    4,2010,0-2,0,2
    5,2010,0-2,1,2
    6,2010,0-2,>1,2
    7,2010,0-2,0,3
    8,2010,0-2,1,3
    9,2010,0-2,>1,3
    10,2010,0-2,0,4
    11,2010,0-2,1,4
    12,2010,0-2,>1,4
""")
def test_knowledge_graph_one_hop_valid_trapi_1_1_response_on_error(query_options):
    """Test one-hop return valid TRAPI 1.1 response on error ."""
    source_id = "PUBCHEM:2083"
    query = {
        "query_options": query_options,
        "message": {
            "query_graph": {
                "nodes": {
                    "n00": {
                        "id": source_id
                    },
                    "n01": {
                        "category": "biolink:ChemicalSubstance"
                    },
                    "n02": {
                        "category": "biolink:ChemicalSubstance"
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
    resp_json = validate_response(testclient.post(
        "/knowledge_graph_one_hop",
        json=query
    ), trapi_version=TRAPI_VERSION)
    assert "message" in resp_json
    assert "query_graph" in resp_json["message"]

    
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure
    varchar(255),int,varchar(255),varchar(255),int
    1,2010,0-2,0,1
    2,2010,0-2,1,2
    3,2010,0-2,>1,3
""")
def test_knowledge_graph_schema():
    """Test getting the knowledge graph schema."""
    resp_json = validate_response(testclient.get(
        "/knowledge_graph/schema",
    ))
    assert "return value" in resp_json
    assert "biolink:PopulationOfIndividualOrganisms" in resp_json["return value"]
    assert "biolink:ChemicalSubstance" in resp_json["return value"]["biolink:PopulationOfIndividualOrganisms"]
    assert "biolink:correlated_with" in resp_json["return value"]["biolink:PopulationOfIndividualOrganisms"]["biolink:ChemicalSubstance"]


categories = [
    "biolink:ChemicalSubstance",
    "biolink:PhenotypicFeature",
    "biolink:Disease",
]


@pytest.mark.parametrize("category", categories)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
    varchar(255),int,varchar(255),varchar(255),int,int,int
    1,2010,0-2,0,1,0,1
    2,2010,0-2,1,1,0,1
    3,2010,0-2,>1,1,0,1
    4,2010,0-2,0,2,0,1
    5,2010,0-2,1,2,0,1
    6,2010,0-2,>1,2,0,1
    7,2010,0-2,0,3,0,1
    8,2010,0-2,1,3,0,1
    9,2010,0-2,>1,3,0,1
    10,2010,0-2,0,4,0,1
    11,2010,0-2,1,4,0,1
    12,2010,0-2,>1,4,0,1
""")
def test_knowledge_graph(category):
    """Test /knowledge_graph."""
    resp_json = validate_response(testclient.post(
        "/knowledge_graph",
        json=query(year, category),
    ))
    do_verify_response(resp_json)


@pytest.mark.parametrize("category", categories)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
    varchar(255),int,varchar(255),varchar(255),int,int,int
    1,2010,0-2,0,1,0,1
    2,2010,0-2,1,1,0,1
    3,2010,0-2,>1,1,0,1
    4,2010,0-2,0,2,0,1
    5,2010,0-2,1,2,0,1
    6,2010,0-2,>1,2,0,1
    7,2010,0-2,0,3,0,1
    8,2010,0-2,1,3,0,1
    9,2010,0-2,>1,3,0,1
    10,2010,0-2,0,4,0,1
    11,2010,0-2,1,4,0,1
    12,2010,0-2,>1,4,0,1
""")
def test_knowledge_graph_unique_edge_ids(category):
    """Test that the /knowledge_graph edge bindings are unique."""
    resp_json = validate_response(testclient.post(
        "/knowledge_graph",
        json=query(year, category),
    ))
    assert "return value" in resp_json

    assert len(resp_json["return value"]["message"]["results"]) > 0

    for edge_bindings in map(
            lambda x: x["edge_bindings"],
            resp_json["return value"]["message"]["results"]
    ):
        assert "e00" in edge_bindings
        assert len(edge_bindings) == 1
        assert len(edge_bindings["e00"]) == 1

    edge_ids = list(map(
        lambda x: x["edge_bindings"]["e00"][0]["id"],
        resp_json["return value"]["message"]["results"],
    ))
    assert len(edge_ids) == len(set(edge_ids))


@pytest.mark.parametrize("category", categories)
@load_data(APP, """
    PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
    varchar(255),int,varchar(255),varchar(255),int,int,int
    1,2010,0-2,0,1,0,1
    2,2010,0-2,1,1,0,1
    3,2010,0-2,>1,1,0,1
    4,2010,0-2,0,2,0,1
    5,2010,0-2,1,2,0,1
    6,2010,0-2,>1,2,0,1
    7,2010,0-2,0,3,0,1
    8,2010,0-2,1,3,0,1
    9,2010,0-2,>1,3,0,1
    10,2010,0-2,0,4,0,1
    11,2010,0-2,1,4,0,1
    12,2010,0-2,>1,4,0,1
""")
def test_knowledge_graph_edge_set(category):
    """Test that the /knowledge_graph result bindings match the kedges."""
    resp_json = validate_response(testclient.post(
        "/knowledge_graph",
        json=query(year, category),
    ))
    assert "return value" in resp_json

    assert len(resp_json["return value"]["message"]["results"]) > 0

    edge_ids = set(map(
        lambda x: x["edge_bindings"]["e00"][0]["id"],
        resp_json["return value"]["message"]["results"]
    ))
    edge_ids2 = set(resp_json["return value"]["message"]["knowledge_graph"]["edges"].keys())
    assert edge_ids == edge_ids2
