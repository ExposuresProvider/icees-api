"""Test miscellaneous endpoints."""
import json

from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP

from ..util import load_data, do_verify_feature_count_response, escape_quotes

testclient = TestClient(APP)
table = "patient"
year = 2010
names = [
    "AgeStudyStart",
    "Albuterol",
    "AvgDailyPM2.5Exposure",
    "EstResidentialDensity",
    "AsthmaDx",
]


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
def test_post_cohort():
    """Test creating a cohort."""
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "cohort_id" in resp_json["return value"]
    assert "size" in resp_json["return value"]


@load_data(
    APP,
    """
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
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,12,"{}",patient,2010
    """
)
def test_cohort_dictionary():
    """Test getting a cohort definition."""
    resp1 = testclient.get(
        f"/{table}/cohort/dictionary",
    )
    resp_json1 = resp1.json()
    assert {
        "features": {},
        "cohort_id": "COHORT:1",
        "size": 12
    } in resp_json1["return value"]


@pytest.mark.parametrize("name", names)
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
def test_get_identifiers(name):
    """Test getting identifiers."""
    resp = testclient.get(
        f"/{table}/{name}/identifiers",
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "identifiers" in resp_json["return value"]
    for iden in resp_json["return value"]["identifiers"]:
        assert "_" not in iden


feature_variables = [
    {
        "feature_name": "AgeStudyStart",
        "feature_qualifier": {
            "operator": "=",
            "value": "0-2"
        }
    }, {
        "feature_name": "AvgDailyPM2.5Exposure",
        "feature_qualifier": {
            "operator": "=",
            "value": 1
        },
        "year": 2011
    }
]


@load_data(
    APP,
    """
        PatientId,year,unmapped
        varchar(255),int,varchar(255)
        1,2011,0-2
        2,2011,0-2
        3,2011,0-2
        4,2011,0-2
        5,2011,0-2
        6,2011,0-2
        7,2011,0-2
        8,2011,0-2
        9,2011,0-2
        10,2011,0-2
        11,2011,0-2
        12,2011,0-2
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,12,"[]",patient,2011
    """
)
def test_feature_count_unmapped():
    year = 2011
    cohort_id = "COHORT:1"

    resp = testclient.get(
        f"/{table}/cohort/{cohort_id}/features",
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert resp_json["return value"] == "No mappings for unmapped"


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,varchar(255),varchar(255),int,int,int
        1,2011,0-2,0,1,0,1
        2,2011,0-2,1,1,0,1
        3,2011,0-2,>1,1,0,1
        4,2011,0-2,0,1,0,1
        5,2011,0-2,1,1,0,1
        6,2011,0-2,>1,1,0,1
        7,2011,0-2,0,1,0,1
        8,2011,0-2,1,1,0,1
        9,2011,0-2,>1,1,0,1
        10,2011,0-2,0,1,0,1
        11,2011,0-2,1,1,0,1
        12,2011,0-2,>1,1,0,1
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,12,"{0}",patient,2011
    """.format(escape_quotes(json.dumps(feature_variables, sort_keys=True)))
)
def test_feature_count_cohort_features_two_years():
    year = 2011
    cohort_id = "COHORT:1"

    resp = testclient.get(
        f"/{table}/cohort/{cohort_id}/features",
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_count_response(resp_json["return value"])


@load_data(
    APP,
    """
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
    """,
)
def test_cohort_dictionary_explicit_tabular():
    feature_variables = [{
        "feature_name": "AgeStudyStart",
        "feature_qualifier": {
            "operator": "=",
            "value": "0-2"
        }
    }]
    testclient.post(
        f"/{table}/cohort",
        json=feature_variables,
    )
    resp = testclient.post(
        f"/{table}/cohort",
        json=feature_variables,
        headers={"Content-Type": "application/json", "Accept": "text/tabular"},
    )

    assert resp.status_code == 200


def test_meta_knowledge_graph():
    response = testclient.get("/meta_knowledge_graph")

    assert response.status_code == 200

    edges = response.json()["edges"]
    assert len(edges) == 16  # 4 categories in mappings.yml, squared


def test_openapi():
    response = testclient.get("/openapi.json")

    assert response.status_code == 200
