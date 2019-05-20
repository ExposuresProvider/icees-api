import unittest
import requests
import json

version = "2.0.0"
table = "patient"
year = 2010
tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}

class TestICEESAPI(unittest.TestCase):

    def test_post_cohort(self):
        feature_variables = {}
        resp = requests.post('http://localhost:5000/{0}/{1}/{2}/cohort'.format(version, table, year), data=json.dumps(feature_variables), headers = json_headers, verify = False)
        resp_json = resp.json()
        self.assertTrue("return value" in resp_json)
        self.assertTrue("cohort_id" in resp_json["return value"])
        self.assertTrue("size" in resp_json["return value"])

    def test_knowledge_graph_schema(self):
        resp = requests.get('http://localhost:5000/{0}/knowledge_graph/schema'.format(version), headers = json_headers, verify = False)
        resp_json = resp.json()
        self.assertTrue("return value" in resp_json)
        self.assertTrue("population_of_individual_organisms" in resp_json["return value"])
        self.assertTrue("chemical_substance" in resp_json["return value"]["population_of_individual_organisms"])
        self.assertTrue("association" in resp_json["return value"]["population_of_individual_organisms"]["chemical_substance"])

    def test_knowledge_graph(self):
        query = {
            "query_options": {
                "table": "patient", 
                "year": year, 
                "cohort_features": {
                    "AgeStudyStart": {
                        "operator": "=",
                        "value": "0-2"
                    }
                }, 
                "feature": {
                    "EstResidentialDensity": {
                        "operator": "<",
                        "value": 1
                    }
                }, 
                "maximum_p_value":1
            }, 
            "machine_question": {
                "nodes": [
                    {
                        "id": "n00",
                        "type": "population_of_individual_organisms"
                    },
                    {
                        "id": "n01",
                        "type": "chemical_substance"
                    }   
                ], 
                "edges": [
                    {
                        "id": "e00",
                        "type": "association",
                        "source_id": "n00",
                        "target_id": "n01"
                    } 
                ]
            }
        }

        resp = requests.post('http://localhost:5000/{0}/knowledge_graph'.format(version), data = json.dumps(query), headers = json_headers, verify = False)
        resp_json = resp.json()
        self.assertTrue("return value" in resp_json)
        self.assertTrue("n_results" in resp_json["return value"])
        self.assertTrue("knowledge_graph" in resp_json["return value"])
        self.assertTrue("message_code" in resp_json["return value"])
        self.assertTrue("tool_version" in resp_json["return value"])
        self.assertTrue("datetime" in resp_json["return value"])

if __name__ == '__main__':
    unittest.main()
