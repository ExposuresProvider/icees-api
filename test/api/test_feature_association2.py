"""Test /feature_association."""
from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP

from ..util import load_data, do_verify_feature_matrix_response

testclient = TestClient(APP)

table = "patient"
year = 2010
tabular_headers = {
    "Content-Type": "application/json",
    "accept": "text/tabular",
}
json_headers = {
    "Content-Type": "application/json",
    "accept": "application/json",
}
age_levels = [
    '0-2',
    '3-17',
    '18-34',
    '35-50',
    '51-69',
    '70-89',
]


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
