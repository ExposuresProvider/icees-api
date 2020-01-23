from flask import Flask, request, make_response
from flask_restful import Resource, Api
import simplejson
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from jsonschema import validate, ValidationError
from flasgger import Swagger
import traceback
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from time import strftime
from structlog import wrap_logger
from structlog.processors import JSONRenderer
import db
from features import model, schema, format, knowledgegraph, identifiers
from utils import opposite, to_qualifiers

with open('terms.txt', 'r') as content_file:
    terms_and_conditions = content_file.read()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(os.environ["ICEES_API_LOG_PATH"])

logger.addHandler(handler)
logger = wrap_logger(logger, processors=[JSONRenderer()])

app = Flask(__name__)
limiter = Limiter(
    app,
    key_func=lambda: "client", # get_remote_address,
    default_limits=["10/second"]
)

app.config["SWAGGER"] = {
  "ui_params_text": '''{
    "operationsSorter" : (a,b) => {
        const ordering = [
          "/{version}/{table}/{year}/cohort/{cohort_id}/features", 
          "/{version}/{table}/{year}/cohort/{cohort_id}/feature_association", 
          "/{version}/{table}/{year}/cohort/{cohort_id}/feature_association2", 
          "/{version}/{table}/{year}/cohort/{cohort_id}/associations_to_all_features"
        ]
        const apath = a.get("path")
        const bpath = b.get("path")
        const aorder = ordering.indexOf(apath)
        const border = ordering.indexOf(bpath)
        if(aorder >= 0) {
          if (border >= 0) {
            return aorder - border
          } else {
            return ordering[0].localeCompare(bpath)
          }
        } else if (border >= 0) {
          return apath.localeCompare(ordering[0])
        } else {
          return apath.localeCompare(bpath)
        }
    }
  }'''
}

@app.after_request
def after_request(response):
    timestamp = strftime('%Y-%b-%d %H:%M:%S')
    logger.info(event="request", timestamp=timestamp, remote_addr=request.remote_addr, method=request.method, schema=request.scheme, full_path=request.full_path, data=request.get_data(as_text=True), response_status=response.status, x_forwarded_for=request.headers.getlist("X-Forwarded-For"))
    return response

api = Api(app)

template = {
  "info": {
    "title": "ICEES API",
      "description": """ICEES API 
<br>[ICEES Overview page](https://researchsoftwareinstitute.github.io/data-translator/apps/icees)
<br>[documentation](https://github.com/NCATS-Tangerine/icees-api/tree/master/docs) 
<br>[source](https://github.com/NCATS-Tangerine/icees-api/tree/master/) 
<br>[ICEES API example queries](https://github.com/NCATS-Tangerine/icees-api/tree/master/#examples) <br>dictionary for versioning of tables<br><table><tr><th>version</th><th>table content</th></tr><tr><td>1.0.0</td><td>cdw, acs, nearest road, and cmaq from 2010</td></tr><tr><td>2.0.0</td><td>cdw in FHIR format, acs, nearest road, and cmaq from 2010</td></tr></table>""",
    "version": "0.0.2"
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
    resp = make_response(simplejson.dumps({"terms and conditions": terms_and_conditions, "version": data["version"], "return value": data["return value"]}, ignore_nan=True), code)
    resp.headers.extend(headers or {})
    return resp

@api.representation('text/tabular')
def output_tabular(data, code, headers=None):
    resp = make_response(format[data["version"]].format_tabular(terms_and_conditions, data["return value"]), code)
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
            required: true
            example: {}
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
        responses:
          201:
            description: The cohort has been created
        """
        try:
            with db.DBConnection(version) as conn:
                req_features = request.get_json()
                if req_features is None:
                    req_features = {}
                else:
                    validate(req_features, schema[version].cohort_schema(table))

                cohort_id, size = model[version].get_ids_by_feature(conn, table, year, req_features)
      
                if size == -1:
                    return_value = "Input features invalid or cohort ≤10 patients. Please try again."
                else:
                    return_value = {
                        "cohort_id": cohort_id,
                        "size": size
                    }
                
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


def versioned(version, data):
    return {
        "version": version,
        "return value": data
    }

class SERVCohortId(Resource):
    def put(self, version, table, year, cohort_id):
        """
        Cohort discovery. Users define a cohort using any number of defined feature variables as input parameters, and the service returns a sample size. A new cohort is created even if a cohort was previously created using the same input parameters.
        ---
        parameters:
          - in: body
            name: body
            description: feature variables
            required: true
            example: {}
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: false
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          201:
            description: The cohort has been created
        """
        try:
            with db.DBConnection(version) as conn:
                req_features = request.get_json()
                if req_features is None:
                    req_features = {}
                else:
                    validate(req_features, schema[version].cohort_schema(table))

                cohort_id, size = model[version].select_cohort(conn, table, year, req_features, cohort_id)

                if size == -1:
                    return_value = "Input features invalid or cohort ≤10 patients. Please try again."
                else:
                    return_value = {
                        "cohort_id": cohort_id,
                        "size": size
                    }
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)

    def get(self, version, table, year, cohort_id):
        """
        Get definition of a cohort
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          200:
            description: The features of the cohort
        """
        try:
            with db.DBConnection(version) as conn:
                cohort_features = model[version].get_cohort_by_id(conn, table, year, cohort_id)
            
                if cohort_features is None:
                    return_value = "Input cohort_id invalid. Please try again."
                else:
                    return_value = cohort_features
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVFeatureAssociation(Resource):
    def post(self, version, table, year, cohort_id):
        """
        Hypothesis-driven 2 x 2 feature associations: users select a predefined cohort and two feature variables, and the service returns a 2 x 2 feature table with a corresponding Chi Square statistic and P value.
        ---
        parameters:
          - in: body
            name: body
            description: two feature variables
            required: true
            example:
              feature_a:
                Sex:
                  operator: "="
                  value: "Female"
              feature_b:
                AsthmaDx:
                  operator: "="
                  value: 1
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          200:
            description: The feature association
        """
        try:
            obj = request.get_json()
            validate(obj, schema[version].feature_association_schema(table))
            feature_a = to_qualifiers(obj["feature_a"])
            feature_b = to_qualifiers(obj["feature_b"])

            with db.DBConnection(version) as conn:
                cohort_features = model[version].get_features_by_id(conn, table, year, cohort_id)

                if cohort_features is None:
                    return_value = "Input cohort_id invalid. Please try again."
                else:
                    return_value = model[version].select_feature_matrix(conn, table, year, cohort_features, feature_a, feature_b)
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


def to_qualifiers2(feature):
    k, v = list(feature.items())[0]
    return {
        "feature_name": k,
        "feature_qualifiers": v
    }


class SERVFeatureAssociation2(Resource):
    def post(self, version, table, year, cohort_id):
        """
        Hypothesis-driven N x N feature associations: users select a predefined cohort, two feature variables, and bins, which can be combined, and the service returns a N x N feature table with a corresponding Chi Square statistic and P value.
        ---
        parameters:
          - in: body
            name: body
            description: two feature variables
            required: true
            example:
              feature_a:
                Sex:
                  - operator: "="
                    value: "Female"
                  - operator: "="
                    value: "Male"
              feature_b:
                AsthmaDx:
                  - operator: "="
                    value: 1
                  - operator: "="
                    value: 0
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          200:
            description: The feature association
        """
        try:
            obj = request.get_json()
            validate(obj, schema[version].feature_association2_schema(table))
            feature_a = to_qualifiers2(obj["feature_a"])
            feature_b = to_qualifiers2(obj["feature_b"])
            to_validate_range = ("check_coverage_is_full" in obj) and obj["check_coverage_is_full"]
            if to_validate_range:
                validate_range(table, feature_a)
                validate_range(table, feature_b)

            with db.DBConnection(version) as conn:
                cohort_features = model[version].get_features_by_id(conn, table, year, cohort_id)

                if cohort_features is None:
                    return_value = "Input cohort_id invalid. Please try again."
                else:
                    return_value = model[version].select_feature_matrix(conn, table, year, cohort_features, feature_a, feature_b)

        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVAssociationsToAllFeatures(Resource):
    def post(self, version, table, year, cohort_id):
        """
        Exploratory 1 X N feature associations: users select a predefined cohort and a feature variable of interest, and the service returns a 1 x N feature table with corrected Chi Square statistics and associated P values.
        ---
        parameters:
          - in: body
            name: body
            description: a feature variable and minimum p value
            required: true
            example:
              feature:
                Sex:
                  operator: "="
                  value: "Female"
              maximum_p_value: 1
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          200:
            description: Associations to all features
        """
        try:
            obj = request.get_json()
            validate(obj, schema[version].associations_to_all_features_schema(table))
            feature = to_qualifiers(obj["feature"])
            maximum_p_value = obj["maximum_p_value"]
            with db.DBConnection(version) as conn:
                return_value = model[version].select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value)
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVAssociationsToAllFeatures2(Resource):
    def post(self, version, table, year, cohort_id):
        """
        Exploratory 1 X N feature associations: users select a predefined cohort and a feature variable of interest and bins, which can be combined, and the service returns a 1 x N feature table with corrected Chi Square statistics and associated P values.
        ---
        parameters:
          - in: body
            name: body
            description: a feature variable and minimum p value
            example:
              feature:
                Sex:
                  - operator: "="
                    value: "Female"
                  - operator: "="
                    value: "Male"
              maximum_p_value: 1
            required: true
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          200:
            description: Associations to all features
        """
        try:
            obj = request.get_json()
            validate(obj, schema[version].associations_to_all_features2_schema(table))
            feature = to_qualifiers2(obj["feature"])
            to_validate_range = ("check_coverage_is_full" in obj) and obj["check_coverage_is_full"]
            if to_validate_range:
                validate_range(table, feature)
            maximum_p_value = obj["maximum_p_value"]
            with db.DBConnection(version) as conn:
                return_value = model[version].select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value)
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVFeatures(Resource):
    def get(self, version, table, year, cohort_id):
        """
        Feature-rich cohort discovery: users select a predefined cohort as the input parameter, and the service returns a profile of that cohort in terms of all feature variables.
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
          - in: path
            name: cohort_id
            required: true
            description: the cohort id
            type: string
            default: COHORT:22
        responses:
          200:
            description: features
        """
        try:
            with db.DBConnection(version) as conn:
                cohort_features = model[version].get_features_by_id(conn, table, year, cohort_id)
                if cohort_features is None:
                    return_value = "Input cohort_id invalid. Please try again."
                else:
                    return_value = model[version].get_cohort_features(conn, table, year, cohort_features)
 
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVCohortDictionary(Resource):
    def get(self, version, table, year):
        """
        Get cohort dictionary
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: year
            required: true
            description: the year 2010
            type: integer
            default: 2010
        responses:
          200:
            description: cohort dictionray
        """
        try:
            with db.DBConnection(version) as conn:
                return_value = model[version].get_cohort_dictionary(conn, table, year)
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVIdentifiers(Resource):
    def get(self, version, table, feature):
        """
        Feature identifiers.
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: feature
            required: true
            description: feature
            type: string
            default: 
        responses:
          200:
            description: feature identifiers
        """
        try:
            return versioned(version, {
                "identifiers": identifiers[version].get_identifiers(table, feature)
            })
        except Exception as e:
            traceback.print_exc()
            return versioned(version, str(e))


class SERVName(Resource):
    def get(self, version, table, name):
        """
        Return cohort id associated with name.
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: name
            required: true
            description: name
            type: string
        responses:
          200:
            description: cohort id and name
        """
        try:
            with model[version].get_db_connection() as conn:
                return_value = model[version].get_id_by_name(conn, table, name)
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


    def post(self, version, table, name):
        """
        Associate name with cohort id.
        ---
        parameters:
          - in: body
            name: body
            description: cohort id
            example:
              cohort_id: COHORT:22
            required: true
          - in: path
            name: version
            required: true
            description: version of data 1.0.0|2.0.0
            type: string
            default: 1.0.0
          - in: path
            name: table
            required: true
            description: the table patient|visit
            type: string
            default: patient
          - in: path
            name: name
            required: true
            description: name
            type: string
        responses:
          200:
            description: cohort id and name
        """
        try:
            obj = request.get_json()
            validate(obj, schema[version].add_name_by_id_schema())
            with db.DBConnection(version) as conn:
                return_value = model[version].add_name_by_id(conn, table, name, obj["cohort_id"])
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVKnowledgeGraph(Resource):
    def post(self, version):
        """
        Query the ICEES clinical reasoner for knowledge graph associations between concepts.
        ---
        definitions:
          import: "TranslatorReasonersAPI.yaml"
        parameters:
          - in: body
            name: body
            description: Input message
            required: true
            schema:
                $ref: '#/definitions/Query'
          - in: path
            name: version
            required: true
            description: version of data 2.0.0
            type: string
            default: 2.0.0
        responses:
            200:
                description: Success
                schema:
                    $ref: '#/definitions/Message'
        """
        try:
            obj = request.get_json()
            # validate(obj, schema[version].add_name_by_id_schema())
            with db.DBConnection(version) as conn:
                return_value = knowledgegraph[version].get(conn, obj)
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


class SERVKnowledgeGraphSchema(Resource):
    def get(self, version):
        """
        Query the ICEES clinical reasoner for knowledge graph schema.
        ---
        parameters:
          - in: path
            name: version
            required: true
            description: version of data 2.0.0
            type: string
            default: 2.0.0
        responses:
            200:
                description: Success
        """
        try:
            return_value = knowledgegraph[version].get_schema()
        except ValidationError as e:
            traceback.print_exc()
            return_value = e.message
        except Exception as e:
            traceback.print_exc()
            return_value = str(e)
        return versioned(version, return_value)


api.add_resource(SERVCohort, '/<string:version>/<string:table>/<int:year>/cohort')
api.add_resource(SERVCohortId, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>')
api.add_resource(SERVFeatures, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/features')
api.add_resource(SERVCohortDictionary, '/<string:version>/<string:table>/<int:year>/cohort/dictionary')
api.add_resource(SERVFeatureAssociation, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/feature_association')
api.add_resource(SERVFeatureAssociation2, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/feature_association2')
api.add_resource(SERVAssociationsToAllFeatures, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/associations_to_all_features')
api.add_resource(SERVAssociationsToAllFeatures2, '/<string:version>/<string:table>/<int:year>/cohort/<string:cohort_id>/associations_to_all_features2')
api.add_resource(SERVIdentifiers, "/<string:version>/<string:table>/<string:feature>/identifiers")
api.add_resource(SERVName, "/<string:version>/<string:table>/name/<string:name>")
api.add_resource(SERVKnowledgeGraph, "/<string:version>/knowledge_graph")
api.add_resource(SERVKnowledgeGraphSchema, "/<string:version>/knowledge_graph/schema")

if __name__ == '__main__':
    app.run()
