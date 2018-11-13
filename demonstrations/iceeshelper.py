import requests
import json
import argparse
requests.packages.urllib3.disable_warnings()

tabular_headers = {"Content-Type" : "application/json", "accept": "text/tabular"}
json_headers = {"Content-Type" : "application/json", "accept": "application/json"}

class DefineCohort ():
    def __init__(self):
        pass
    
    def make_cohort_definition(self, feature, value, operator):
        feature_variables = '{{"{0}": {{ "value": {1}, "operator": "{2}"}}}}'.format(feature, value, operator)
        return feature_variables
    
    def define_cohort_query(self, feature_variables, year=2010, table='patient', version='1.0.0'): # year, table, and version are hardcoded for now
        define_cohort_response = requests.post('https://icees.renci.org/{0}/{1}/{2}/cohort'.format(version, table, year), data=feature_variables, headers = json_headers, verify = False)               
        return define_cohort_response

    def run_define_cohort (self, feature, value, operator):
        feature_variables = self.make_cohort_definition(feature, value, operator)
        define_cohort_query = self.define_cohort_query(feature_variables)
        define_cohort_query_json = define_cohort_query.json()
        return define_cohort_query_json

class GetCohortDefinition():
    def __init__(self):
        pass
    
    def get_cohort_definition_query(self, cohort_id, year=2010, table='patient', version='1.0.0'):
        cohort_definition_response = requests.get('https://icees.renci.org/{0}/{1}/{2}/cohort/{3}'.format(version, table, year, cohort_id), headers = json_headers, verify = False)               
        return cohort_definition_response

    def run_get_cohort_definition(self, cohort_id):
        cohort_definition_query = self.get_cohort_definition_query(cohort_id)
        cohort_definition_query_json = cohort_definition_query.json()
        return cohort_definition_query_json
    
class GetFeatures():
    def __init__(self):
        pass

class FeatureAssociation():
    def __init__(self):
        pass

class AssociationToAllFeatures():
    def __init__(self):
        pass
    
    def make_association_to_all_features(self, feature, value, operator, maximum_p_value):
        feature_variable_and_p_value = '{{"feature":{{"{0}":{{"operator":"{1}","value":{2}}}}},"maximum_p_value":{3}}}'.format(feature, value, operator, maximum_p_value)
        #print(feature_variable_and_p_value)
        return feature_variable_and_p_value

    def assocation_to_all_features_query(self, feature_variable_and_p_value, cohort_id, year=2010, table='patient', version='1.0.0'):
        assoc_to_all_features_response = requests.post('https://icees.renci.org/{0}/{1}/{2}/cohort/{3}/associations_to_all_features'.format(version, table, year, cohort_id), data=feature_variable_and_p_value, headers= json_headers, verify=False)
        return assoc_to_all_features_response

    def run_association_to_all_features(self, feature, value, operator, maximum_p_value, cohort_id):
        feature_variable_and_p_value = self.make_association_to_all_features(feature, value, operator, maximum_p_value)
        assoc_to_all_features_query = self.assocation_to_all_features_query(feature_variable_and_p_value, cohort_id)
        assoc_to_all_features_query_json = assoc_to_all_features_query.json()
        return assoc_to_all_features_query_json

class GetDictionary():
    def __init__(self):
        pass

    def get_dictionary_query(self, year=2010, table='patient', version='1.0.0'):
        dictionary_response = requests.get('https://icees.renci.org/{0}/{1}/{2}/cohort/dictionary'.format(version, table, year), headers = json_headers, verify = False) 
        return dictionary_response

    def run_get_dictionary(self):
        dictionary_query = self.get_dictionary_query()
        dictionary_query_json = dictionary_query.json()
        return dictionary_query_json

parser = argparse.ArgumentParser()
parser.add_argument("-ftr", "--feature", help="feature name")
parser.add_argument("-v", "--value", help="feature value")
parser.add_argument("-op", "--operator", help="feature operator")
args = parser.parse_args()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 3:
        icees_define_cohort = DefineCohort()
        output = icees_define_cohort.run_define_cohort(args.feature, args.value, args.operator)
        #if 'cohort_id' in str(output):
        print()
        print ('Cohort definition accepted')
        print(output['return value'])
        print()
    else:
        print("Expected script call is of the form: $python3 icees_caller.py -ftr <feature> -val <value> -op \<operator>")