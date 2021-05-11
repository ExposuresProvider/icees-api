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
        PatientId,year,AgeStudyStart,Albuterol
        varchar(255),int,varchar(255),varchar(255)
        1,2010,0-2,
        2,2010,0-2,
        3,2010,0-2,
        4,2010,0-2,
        5,2010,0-2,
        6,2010,0-2,
        7,2010,0-2,
        8,2010,0-2,
        9,2010,0-2,
        10,2010,0-2,
        11,2010,0-2,
        12,2010,0-2,
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,12,"{}",patient,2010
    """
)
def test_associations_to_all_features_nulls():
    cohort_id = "COHORT:1"
    atafdata = {
        "feature": {
            "feature_name": "AgeStudyStart",
            "feature_qualifier": {
                "operator": "=",
                "value": "0-2"
            }
        },
        "maximum_p_value": 1,
        "correction": {
            "method": "bonferroni"
        }
    }
    resp = testclient.post(
        f"/{table}/cohort/{cohort_id}/associations_to_all_features",
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
        2,2010,0-2,1,1,0,0
        3,2010,3-7,>1,1,0,0
        4,2010,0-2,0,2,0,1
        5,2010,3-7,1,2,0,1
        6,2010,3-7,>1,2,0,1
        7,2010,0-2,0,3,0,0
        8,2010,0-2,1,3,0,0
        9,2010,0-2,>1,3,0,0
        10,2010,0-2,0,4,0,0
        11,2010,0-2,1,4,0,0
        12,2010,3-7,>1,4,0,1
    """,
    """
        cohort_id,size,features,table,year
        COHORT:1,12,"{}",patient,2010
    """
)
def test_associations_to_all_features_explicit():
    cohort_id = "COHORT:1"
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
        f"/{table}/cohort/{cohort_id}/associations_to_all_features",
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
    cohort_id = "COHORT:1"
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
        f"/{table}/cohort/{cohort_id}/associations_to_all_features",
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
    cohort_id = "COHORT:1"
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
        f"/{table}/cohort/{cohort_id}/associations_to_all_features",
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
    cohort_id = "COHORT:1"
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
        f"/{table}/cohort/{cohort_id}/associations_to_all_features",
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], list)
