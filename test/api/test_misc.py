"""Test miscellaneous endpoints."""
from fastapi.testclient import TestClient
import pytest

from icees_api.app import APP
from icees_api.features.sql import get_features

from ..util import load_data, do_verify_feature_count_response

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


names = [
    "ObesityDx",
    "Sex2",
    "OvarianDysfunctionDx",
    "OvarianCancerDx",
]

age_levels = [
    '0-2',
    '3-17',
    '18-34',
    '35-50',
    '51-69',
    '70-89',
]


def test_post_cohort():
    """Test creating a cohort."""
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "cohort_id" in resp_json["return value"]
    assert "size" in resp_json["return value"]


def test_cohort_dictionary():
    """Test getting a cohort definition."""
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()

    resp1 = testclient.get(
        f"/{table}/{year}/cohort/dictionary",
        headers=json_headers,
    )
    resp_json1 = resp1.json()
    assert {
        "features": {},
        "cohort_id": resp_json["return value"]["cohort_id"],
        "size": resp_json["return value"]["size"]
    } in resp_json1["return value"]


@pytest.mark.parametrize("name", names)
def test_get_identifiers(name):
    """Test getting identifiers."""
    resp = testclient.get(
        f"/{table}/{name}/identifiers",
        headers=json_headers,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    assert "identifiers" in resp_json["return value"]
    for iden in resp_json["return value"]["identifiers"]:
        assert "_" not in iden


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
        f"/{table}/{cohort_year}/cohort",
        json=feature_variables,
    )
    resp_json = resp.json()
    cohort_id = resp_json["return value"]["cohort_id"]

    resp = testclient.get(
        f"/{table}/{year}/cohort/{cohort_id}/features",
        headers=json_headers,
    )
    resp_json = resp.json()
    assert "return value" in resp_json
    do_verify_feature_count_response(resp_json["return value"])


def test_cohort_dictionary_explicit_tabular():
    feature_variables = [{
        "feature_name": "AgeStudyStart",
        "feature_qualifier": {
            "operator": "=",
            "value": "0-2"
        }
    }]
    testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
    )
    resp = testclient.post(
        f"/{table}/{year}/cohort",
        json=feature_variables,
        headers={"Content-Type": "application/json", "Accept": "text/tabular"},
    )

    assert resp.status_code == 200


def test_get_features():
    """Test get_features()."""
    print(get_features("patient"))
