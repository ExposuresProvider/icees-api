from flask import Flask, request, make_response
from flask_restful import Resource, Api
import json
from model import get_features_by_id, select_feature_association, select_feature_matrix, get_db_connection, get_ids_by_feature, opposite, cohort_id_in_use, select_cohort, get_cohort_features, get_cohort_dictionary, service_name, get_cohort_by_id
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from jsonschema import validate, ValidationError
from schema import cohort_schema, feature_association_schema, associations_to_all_features_schema
from flasgger import Swagger
import traceback
from format import format_tabular

with open('terms.txt', 'r') as content_file:
    terms_and_conditions = content_file.read()

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=lambda: "client", # get_remote_address,
    default_limits=["10/second"]
)
api = Api(app)

template = {
  "info": {
    "title": "ICEES API",
    "description": "ICEES API [documentation](https://drive.google.com/open?id=12TgOZMFkWQLMhjZeN4RVzdxvlt1VYcO8)",
    "version": "0.0.1"
  },
  "consumes": [
    "application/json"
  ],
  "produces": [
    "application/json",
    "text/tabular"
  ],
  "host": "icees.renci.org",  # overrides localhost:500
  "basePath": "/",  # base bash for blueprint registration
  "schemes": [
    "https"
  ]
}

swag = Swagger(app, template=template)

@api.representation('application/json')
def output_json(data, code, headers=None):
    resp = make_response(json.dumps({"terms and conditions": terms_and_conditions, "return value": data}), code)
    resp.headers.extend(headers or {})
    return resp

@api.representation('text/tabular')
def output_tabular(data, code, headers=None):
    resp = make_response(format_tabular(terms_and_conditions, data), code)
    resp.headers.extend(headers or {})
    return resp

class SERVCohort(Resource):
    def post(self, version, table, year):
        """
        Cohort discovery. Users define a cohort using any number of defined feature variables as input parameters, and the service returns a sample size. If a cohort is already created for this set before, return cohort id and size. Otherwise, generate a new cohort id.
        ---
        parameters:
          - in: body
            name: body
            description: feature variables
            schema: 
              oneOf:
                - import: "definitions/cohort_patient_input.yaml"
                - import: "definitions/cohort_visit_input.yaml"
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
        responses:
          201:
            description: The cohort has been created
            schema:
              oneOf:
                - import: "definitions/cohort_patient_output.yaml"
                - import: "definitions/cohort_visit_output.yaml"
        """
        try:
            conn = get_db_connection(version)
            req_features = request.get_json()
            if req_features is None:
                req_features = {}
            else:
                validate(req_features, cohort_schema(table))

            cohort_id, size = get_ids_by_feature(conn, table, year, req_features)
      
            if size == -1:
                return "Input features invalid. Please try again."
            else:
                return {
                    "cohort_id": cohort_id,
                    "size": size
                }
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)

class SERVCohortId(Resource):
    def put(self, version, table, year, cohort_id):
        """
        Cohort discovery. Users define a cohort using any number of defined feature variables as input parameters, and the service returns a sample size. A new cohort is created even if a cohort was previously created using the same input parameters.
        ---
        parameters:
          - in: body
            name: body
            description: feature variables
            schema: 
              oneOf:
                - import: "definitions/cohort_patient_input.yaml"
                - import: "definitions/cohort_visit_input.yaml"
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
          - in: path
            name: cohort_id
            required: false
            description: the cohort id
            type: string
        responses:
          201:
            description: The cohort has been created
            schema:
              oneOf:
                - import: "definitions/cohort_patient_output.yaml"
                - import: "definitions/cohort_visit_output.yaml"
        """
        try:
            conn = get_db_connection(version)
            req_features = request.get_json()
            if req_features is None:
                req_features = {}
            else:
                validate(req_features, cohort_schema(table))

            cohort_id, size = select_cohort(conn, table, year, req_features, cohort_id)

            if size == -1:
                return "Input features invalid. Please try again."
            else:
                return {
                    "cohort_id": cohort_id,
                    "size": size
                }
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)

    def get(self, version, table, year, cohort_id):
        """
        Get definition of a cohort
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
        responses:
          200:
            description: The features of the cohort
            schema:
              oneOf: 
                - import: "definitions/cohort_patient_input.yaml"
                - import: "definitions/cohort_visit_input.yaml"
        """
        try:
            conn = get_db_connection(version)
            cohort_features = get_cohort_by_id(conn, table, year, cohort_id)
            
            if cohort_features is None:
                return "Input cohort_id invalid. Please try again."
            else:
                return cohort_features
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)


def to_qualifiers(feature):
    k, v = list(feature.items())[0]
    return {
        "feature_name": k,
        "feature_qualifiers": [v, opposite(v)]
    }


class SERVFeatureAssociation(Resource):
    def post(self, version, table, year, cohort_id):
        """
        Hypothesis-driven 2 x 2 feature associations: users select a predefined cohort and two feature variables, and the service returns a 2 x 2 feature table with a corresponding Chi Square statistic and P value.
        ---
        parameters:
          - in: body
            name: body
            description: two feature variables
            schema: 
              oneOf:
                - import: "definitions/feature_association_patient_input.yaml"
                - import: "definitions/feature_association_visit_input.yaml"
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
        responses:
          200:
            description: The feature association
            schema:
              oneOf: 
                - import: "definitions/feature_association_patient_output.yaml"
                - import: "definitions/feature_association_visit_output.yaml"
        """
        try:
            obj = request.get_json()
            validate(obj, feature_association_schema(table))
            feature_a = to_qualifiers(obj["feature_a"])
            feature_b = to_qualifiers(obj["feature_b"])

            conn = get_db_connection(version)
            cohort_features = get_features_by_id(conn, table, year, cohort_id)

            if cohort_features is None:
                return "Input cohort_id invalid. Please try again."
            else:
                return select_feature_matrix(conn, table, year, cohort_features, feature_a, feature_b)
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)


class SERVAssociationsToAllFeatures(Resource):
    def post(self, version, table, year, cohort_id):
        """
        Exploratory 1 X N feature associations: users select a predefined cohort and a feature variable of interest, and the service returns a 1 x N feature table with corrected Chi Square statistics and associated P values.
        ---
        parameters:
          - in: body
            name: body
            description: a feature variable and minimum p value
            schema: 
              oneOf:
                - import: "definitions/associations_to_all_features_patient_input.yaml"
                - import: "definitions/associations_to_all_features_visit_input.yaml"
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
        responses:
          200:
            description: Associations to all features
            schema:
              oneOf:
                - import: "definitions/associations_to_all_features_patient_output.yaml"
                - import: "definitions/associations_to_all_features_visit_output.yaml"
        """
        try:
            obj = request.get_json()
            validate(obj, associations_to_all_features_schema(table))
            feature = to_qualifiers(obj["feature"])
            maximum_p_value = obj["maximum_p_value"]
            conn = get_db_connection(version)
            cohort_features = get_features_by_id(conn, table, year, cohort_id)
            if cohort_features is None:
                return "Input cohort_id invalid. Please try again."
            else:
                return select_feature_association(conn, table, year, cohort_features, feature, maximum_p_value)
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)


class SERVFeatures(Resource):
    def get(self, version, table, year, cohort_id):
        """
        Feature-rich cohort discovery: users select a predefined cohort as the input parameter, and the service returns a profile of that cohort in terms of all feature variables.
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
        responses:
          200:
            description: features
            schema:
              oneOf:
                - import: "definitions/features_patient_output.yaml"
                - import: "definitions/features_visit_output.yaml"
        """
        try:
            conn = get_db_connection(version)
            cohort_features = get_features_by_id(conn, table, year, cohort_id)
            if cohort_features is None:
                return "Input cohort_id invalid. Please try again."
            else:
                return get_cohort_features(conn, table, year, cohort_features)
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)


class SERVCohortDictionary(Resource):
    def get(self, version, table, year):
        """
        Get cohort dictionary
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0
            type: string
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
          - in: path
            name: year
            required: true
            description: the year 2010|2011
            type: integer
        responses:
          200:
            description: cohort dictionray
            schema:
              oneOf:
                - import: "definitions/cohort_dictionary_patient_output.yaml"
                - import: "definitions/cohort_dictionary_visit_output.yaml"
        """
        try:
            conn = get_db_connection(version)
            return get_cohort_dictionary(conn, table, year)
        except ValidationError as e:
            traceback.print_exc()
            return e.message
        except Exception as e:
            traceback.print_exc()
            return str(e)


api.add_resource(SERVCohort, '/<string:version>/<string:table>/<int:year>/cohort')
api.add_resource(SERVCohortId, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>')
api.add_resource(SERVFeatures, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/features')
api.add_resource(SERVCohortDictionary, '/<string:version>/<string:table>/<int:year>/cohort/dictionary')
api.add_resource(SERVFeatureAssociation, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/feature_association')
api.add_resource(SERVAssociationsToAllFeatures, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/associations_to_all_features')

if __name__ == '__main__':
    app.run()
