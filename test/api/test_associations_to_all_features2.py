"""Test API."""
from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP

from ..util import load_data

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


def test_associations_to_all_features2_explicit():
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
            "feature_qualifiers": list(map(lambda x: {
                "operator": "=",
                "value": x
            }, age_levels))
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


def test_associations_to_all_features2():
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
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
        f"/{table}/{year}/cohort/{cohort_id}/associations_to_all_features2",
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
                    "operator": "in",
                    "values": ["35-50", "51-69"]
                }, {
                    "operator": "=",
                    "value": "70-89"
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
