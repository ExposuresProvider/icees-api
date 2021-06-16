"""Test security."""
from fastapi.testclient import TestClient

from icees_api.app import APP


testclient = TestClient(APP)


def test_invalid_table():
    """Test creating a cohort from an invalid table."""
    table = "Robert'); DROP TABLES students;--"
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/cohort",
        json=feature_variables,
    )
    assert resp.status_code == 400
    assert "invalid table" in resp.text.lower()


def test_invalid_feature():
    """Test creating a cohort with an invalid feature."""
    table = "patient"
    feature_name = "Robert'); DROP TABLES students;--"
    feature_variables = {feature_name: {"operator": ">", "value": 0}}
    resp = testclient.post(
        f"/{table}/cohort",
        json=feature_variables,
    )
    assert resp.status_code == 422
