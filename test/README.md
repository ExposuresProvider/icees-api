# Testing the ICEES API

[![Test status via GitHub Actions](https://github.com/ExposuresProvider/icees-api/workflows/test/badge.svg)](https://github.com/ExposuresProvider/icees-api/actions/workflows/testing.yml)

### Content

* [`test_associations_to_all_features.py`](api/test_associations_to_all_features.py):

  We test the endpoint /associations_to_all_features.

* [`test_associations_to_all_features2.py`](api/test_associations_to_all_features2.py):

  We test the endpoint /associations_to_all_features2.

* [`test_feature_association.py`](api/test_feature_association.py):

  We test the endpoint /feature_association.

* [`test_feature_association2.py`](api/test_feature_association2.py):

  We test the endpoint /feature_association2.

* [`test_misc.py`](api/test_misc.py):

  We test the endpoints

  * /cohort
  * /cohort/dictionary
  * /features

### Workflow

Tests are run automatically via GitHub Actions on each pull request and each push to `master`.
