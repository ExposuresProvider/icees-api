import unittest
import requests
import json

version = "2.0.0"
table = "patient"
year = 2010
tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}

class TestICEESAPI(unittest.TestCase):
    def do_test_knowledge_graph(self, biolink_class):
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
                        "type": biolink_class
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


    def do_test_get_identifiers(self, i):
        feature_variables = {}
        resp = requests.get('http://localhost:5000/{0}/{1}/{2}/identifiers'.format(version, table, i), headers = json_headers, verify = False)
        resp_json = resp.json()
        self.assertTrue("return value" in resp_json)
        self.assertTrue("identifiers" in resp_json["return value"])

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

    def test_knowledge_graph_for_chemical_substance(self):
        self.do_test_knowledge_graph("chemical_substance")

    def test_knowledge_graph_for_phenotypic_feature(self):
        self.do_test_knowledge_graph("phenotypic_feature")

    def test_knowledge_graph_for_disease(self):
        self.do_test_knowledge_graph("disease")

    def test_get_identifiers_for_(self):
        self.do_test_get_identifiers("ObesityDx")

    def test_get_identifiers_Sex2(self):
        self.do_test_get_identifiers("Sex2")

    def test_get_identifiers_OvarianDysfunctionDx(self):
        self.do_test_get_identifiers("OvarianDysfunctionDx")

if __name__ == '__main__':
    unittest.main()
