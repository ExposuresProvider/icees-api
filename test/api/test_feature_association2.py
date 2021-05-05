"""Test /feature_association."""
from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP

from ..util import load_data, do_verify_feature_matrix_response

testclient = TestClient(APP)
table = "patient"
year = 2010
age_levels = [
    '0-2',
    '3-17',
    '18-34',
    '35-50',
    '51-69',
    '70-89',
]


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,varchar(255),varchar(255),int,int,int
        1,2010,0-2,0,1,0,1
        2,2010,3-17,1,1,0,1
        3,2010,18-34,>1,1,0,1
        4,2010,35-50,0,2,0,1
        5,2010,51-69,1,2,0,1
        6,2010,70-89,>1,2,0,1
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
def test_feature_association2_explicit_check_coverage_is_full_2():
    cohort_id = "COHORT:1"
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


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,varchar(255),varchar(255),int,int,int
        1,2010,0-2,0,1,0,1
        2,2010,3-17,1,1,0,1
        3,2010,18-34,>1,1,0,1
        4,2010,35-50,0,2,0,1
        5,2010,51-69,1,2,0,1
        6,2010,70-89,>1,2,0,1
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
def test_feature_association2_explicit_check_coverage_is_full_3():
    cohort_id = "COHORT:1"
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
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert isinstance(resp_json["return value"], str)


@load_data(
    APP,
    """
        PatientId,year,AgeStudyStart,Albuterol,AvgDailyPM2.5Exposure,EstResidentialDensity,AsthmaDx
        varchar(255),int,varchar(255),varchar(255),int,int,int
        1,2010,0-2,0,1,0,1
        2,2010,3-17,1,1,0,1
        3,2010,18-34,>1,1,0,1
        4,2010,35-50,0,2,0,1
        5,2010,51-69,1,2,0,1
        6,2010,70-89,>1,2,0,1
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
def test_feature_association2_explicit_check_coverage_is_full():
    cohort_id = "COHORT:1"
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
        json=atafdata,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_matrix_response(resp_json["return value"])
