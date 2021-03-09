"""Test API."""
from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP

from ..util import load_data

testclient = TestClient(APP)
table = "patient"
year = 2010


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
def test_associations_to_all_features_explicit():
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
        "maximum_p_value": 1
    }
    resp = testclient.post(
        f"/{table}/{year}/cohort/{cohort_id}/associations_to_all_features",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


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
def test_associations_to_all_features_with_correction():
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
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
        f"/{table}/{year}/cohort/{cohort_id}/associations_to_all_features",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)


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
def test_associations_to_all_features_with_correction_with_alpha():
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
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
        f"/{table}/{year}/cohort/{cohort_id}/associations_to_all_features",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)
