from flask import Flask, request
from flask_restful import Resource, Api
import json
from model import get_features_by_id, select_feature_association, select_feature_matrix, get_db_connection, get_ids_by_feature

app = Flask(__name__)
api = Api(app)


class DDCRCohort(Resource):
    def get(self, cohort_id = None):
        if cohort_id is None:
            req_features = request.get_json()
            print(req_features)
            conn = get_db_connection()
            cohort_id, lower_bound, upper_bound = get_ids_by_feature(conn, req_features)

            if upper_bound == -1:
                return "Input features invalid. Please try again."
            else:
                return json.dumps({
                    "cohort_id": cohort_id,
                    "bounds": {
                        "upper_bound": upper_bound,
                        "lower_bound": lower_bound
                    }
                }, sort_keys=True)
        else:
            conn = get_db_connection()
            cohort_features = get_features_by_id(conn, cohort_id)

            if cohort_features is None:
                return "Input cohort_id invalid. Please try again."
            else:
                return json.dumps(cohort_features, sort_keys=True)


class DDCRFeatureAssociation(Resource):
    def get(self):
        obj = request.get_json()
        cohort_id = obj["cohort_id"]
        feature_a = obj["feature_a"]
        feature_b = obj["feature_b"]
        conn = get_db_connection()
        cohort_features = get_features_by_id(conn, cohort_id)
        return json.dumps(
            select_feature_matrix(conn, cohort_features, feature_a, feature_b),
            sort_keys=True
        )


class DDCRAssociationsToAllFeatures(Resource):
    def get(self):
        obj = request.get_json()
        cohort_id = obj["cohort_id"]
        feature = obj["feature"]
        maximum_p_value = obj["maximum_p_value"]
        conn = get_db_connection()
        cohort_features = get_features_by_id(conn, cohort_id)
        return json.dumps(
            select_feature_association(conn, cohort_features, feature, maximum_p_value),
            sort_keys=True
        )


api.add_resource(DDCRCohort, '/cohort', '/cohort/<string:cohort_id>')
api.add_resource(DDCRFeatureAssociation, '/feature_association')
api.add_resource(DDCRAssociationsToAllFeatures, '/associations_to_all_features')

if __name__ == '__main__':
    app.run()
