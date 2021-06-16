"""Test security."""
from fastapi.testclient import TestClient

from icees_api.app import APP


testclient = TestClient(APP)


def test_invalid_table():
    """Test creating a cohort."""
    table = "Robert'); DROP TABLES students;--"
    feature_variables = {}
    resp = testclient.post(
        f"/{table}/cohort",
        json=feature_variables,
    )
    assert resp.status_code == 400
    assert "invalid table" in resp.text.lower()
