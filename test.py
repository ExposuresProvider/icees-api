import unittest
import requests
import json

version = "2.0.0"
table = "patient"
year = "2010"
tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}

class TestICEESAPI(unittest.TestCase):

    def test_post_cohort(self):
        feature_variables = {}
        resp = requests.post('http://localhost:5000/{0}/{1}/{2}/cohort'.format(version, table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        self.assertTrue(hasattr(resp_json, "cohort_id"))
        self.assertTrue(hasattr(resp_json, "size"))

if __name__ == '__main__':
    unittest.main()
