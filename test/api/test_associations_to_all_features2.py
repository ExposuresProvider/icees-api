"""Test API."""
from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP

from ..util import load_data

testclient = TestClient(APP)
table = "patient"
year = 2010
age_levels = [
    2,
    17,
    34,
    50,
    69,
    89,
]


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,int,varchar(255),int,int,int
        1,2010,2,0,1,0,1
        2,2010,2,1,1,0,1
        3,2010,2,1,1,0,1
        4,2010,2,0,2,0,1
        5,2010,2,1,2,0,1
        6,2010,2,1,2,0,1
        7,2010,2,0,3,0,1
        8,2010,2,1,3,0,1
        9,2010,2,1,3,0,1
        10,2010,2,0,4,0,1
        11,2010,2,1,4,0,1
        12,2010,2,1,4,0,1
        13,2010,17,1,4,0,1
        14,2010,34,1,4,0,1
        15,2010,50,1,4,0,1
        16,2010,69,1,4,0,1
        17,2010,89,1,4,0,1        
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,17,"{}",patient,2010
    """
)
def test_associations_to_all_features2_explicit():
    cohort_id = "COHORT:1"
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
        f"/{table}/cohort/{cohort_id}/associations_to_all_features2",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,int,varchar(255),int,int,int
        1,2010,2,0,1,0,1
        2,2010,2,1,1,0,1
        3,2010,2,1,1,0,1
        4,2010,2,0,2,0,1
        5,2010,2,1,2,0,1
        6,2010,2,1,2,0,1
        7,2010,2,0,3,0,1
        8,2010,2,1,3,0,1
        9,2010,2,1,3,0,1
        10,2010,2,0,4,0,1
        11,2010,2,1,4,0,1
        12,2010,2,1,4,0,1
        13,2010,17,1,4,0,1
        14,2010,34,1,4,0,1
        15,2010,50,1,4,0,1
        16,2010,69,1,4,0,1
        17,2010,89,1,4,0,1  
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,17,"{}",patient,2010
    """
)
def test_associations_to_all_features2():
    cohort_id = "COHORT:1"
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
        f"/{table}/cohort/{cohort_id}/associations_to_all_features2",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,int,varchar(255),int,int,int
        1,2010,2,0,1,0,1
        2,2010,2,1,1,0,1
        3,2010,2,1,1,0,1
        4,2010,2,0,2,0,1
        5,2010,2,1,2,0,1
        6,2010,2,1,2,0,1
        7,2010,2,0,3,0,1
        8,2010,2,1,3,0,1
        9,2010,2,1,3,0,1
        10,2010,2,0,4,0,1
        11,2010,2,1,4,0,1
        12,2010,2,1,4,0,1
        13,2010,17,1,4,0,1
        14,2010,34,1,4,0,1
        15,2010,50,1,4,0,1
        16,2010,69,1,4,0,1
        17,2010,89,1,4,0,1  
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,17,"{}",patient,2010
    """
)
def test_associations_to_all_features2b():
    cohort_id = "COHORT:1"
    atafdata = {
        "feature": {
            "AgeStudyStart": [
                {
                    "operator": "=",
                    "value": 2
                }, {
                    "operator": "in",
                    "values": [17, 34]
                }, {
                    "operator": "in",
                    "values": [50, 69]
                }, {
                    "operator": "=",
                    "value": 89
                }
            ]
        },
        "maximum_p_value": 1
    }
    resp = testclient.post(
        f"/{table}/cohort/{cohort_id}/associations_to_all_features2",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)
