from flask import Flask, request, make_response
from flask_restful import Resource, Api
import json
from model import get_features_by_id, select_feature_association, select_feature_matrix, get_db_connection, get_ids_by_feature, opposite, cohort_id_in_use, select_cohort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from jsonschema import validate, ValidationError
from schema import cohort_schema, feature_association_schema, associations_to_all_features_schema
from flasgger import Swagger

with open('terms.txt', 'r') as content_file:
    terms_and_conditions = content_file.read()

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=lambda: "client", # get_remote_address,
    default_limits=["10/second"]
)
api = Api(app)

app.config['SWAGGER'] = {
    "title": "DDCR API",
    "uiversion": 2
}
swag = Swagger(app)

@api.representation('application/json')
def output_json(data, code, headers=None):
    resp = make_response(json.dumps({"terms and conditions":terms_and_conditions, "return value":data}), code)
    resp.headers.extend(headers or {})
    return resp

class DDCRCohort(Resource):
    def put(self, version, table, year, cohort_id=None):
        """
        Create a new cohort
        ---
        parameters:
          - in: body
            name: body
            schema: 
              $ref: "#definitions/Features"
          - in: path
            name: version
            required: true
            description: version of data
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
              $ref: "#definitions/Cohort"
        """
        try:
            conn = get_db_connection(version)
            req_features = request.get_json()
            if req_features is None:
                req_features = {}
            else:
                validate(req_features, cohort_schema(table))

            if cohort_id is None:
                cohort_id, lower_bound, upper_bound = get_ids_by_feature(conn, table, year, req_features)
            else:
                cohort_id, lower_bound, upper_bound = select_cohort(conn, table, year, req_features, cohort_id)

            if upper_bound == -1:
                return "Input features invalid. Please try again."
            else:
                return {
                    "cohort_id": cohort_id,
                    "bounds": {
                        "upper_bound": upper_bound,
                        "lower_bound": lower_bound
                    }
                }
        except ValidationError as e:
            return e.message
        except Exception as e:
            return str(e)

    def get(self, version, table, year, cohort_id=None):
        """
        Get features of a cohort
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data
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
          200:
            description: The features of the cohort
            schema:
              $ref: "#definitions/Features"
        """
        try:
            conn = get_db_connection(version)
            if cohort_id is None:
                req_features = request.get_json()
                if req_features is None:
                    req_features = {}
                else:
                    if cohort_id is not None:
                        return "Input cohort_id and features both present. Please try again."
                    validate(req_features, cohort_schema(table))
                cohort_id, lower_bound, upper_bound = get_ids_by_feature(conn, table, year, req_features)

                if upper_bound == -1:
                    return "Input features invalid. Please try again."
                else:
                    return {
                        "cohort_id": cohort_id,
                        "bounds": {
                            "upper_bound": upper_bound,
                            "lower_bound": lower_bound
                        }
                    }
            else:
                cohort_features = get_features_by_id(conn, table, year, cohort_id)

                if cohort_features is None:
                    return "Input cohort_id invalid. Please try again."
                else:
                    return cohort_features
        except ValidationError as e:
            return e.message
        except Exception as e:
            return str(e)


def to_qualifiers(feature):
    k, v = list(feature.items())[0]
    return {
        "feature_name": k,
        "feature_qualifiers": [v, opposite(v)]
    }


class DDCRFeatureAssociation(Resource):
    def get(self, version, table, year, cohort_id):
        self.post(version, table, year, cohort_id)
        
    def post(self, version, table, year, cohort_id):
        """
        Get feature association
        ---
        parameters:
          - in: body
            name: body
            schema: 
              $ref: "#definitions/FeatureAssociationInput"
          - in: path
            name: version
            required: true
            description: version of data
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
          201:
            description: The feature association
            schema:
              $ref: "#definitions/FeatureAssociationOutput"
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
            return e.message
        except Exception as e:
            return str(e)


class DDCRAssociationsToAllFeatures(Resource):
    def get(self, version, table, year, cohort_id):
        self.post(version, table, year, cohort_id)
        
    def post(self, version, table, year, cohort_id):
        """
        Get associations to all features
        ---
        parameters:
          - in: body
            name: body
            schema: 
              $ref: "#definitions/AssociationsToAllFeaturesInput"
          - in: path
            name: version
            required: true
            description: version of data
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
          201:
            description: Associations to all features
            schema:
              $ref: "#definitions/AssociationsToAllFeaturesOutput"
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
            return e.message
        except Exception as e:
            return str(e)


api.add_resource(DDCRCohort, '/<string:version>/<string:table>/<int:year>/cohort', '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>')
api.add_resource(DDCRFeatureAssociation, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/feature_association')
api.add_resource(DDCRAssociationsToAllFeatures, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/associations_to_all_features')

if __name__ == '__main__':
    app.run()
