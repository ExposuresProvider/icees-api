from flask import Flask, request
from flask_restful import Resource, Api
import json

app = Flask(__name__)
api = Api(app)

class DDCR_get_ids_by_feature(Resource):
    def get(self):
        feature = request.form
        return json.dumps({
            "cohort_id": "A:1",
            "bounds": {
                "upper_bound" : 101,
                "lower_bound" : 100
            }
        })

class DDCR_get_features_by_id(Resource):
    def get(self, cohort_id):
        return json.dumps({"A":"b","C":"d"})

class DDCR_get_feature_association(Resource):
    def get(self,cohort_id, feature_a, feature_b):
        return json.dump({
            "chi_squared" : .1,
            "p_value" : .9,
            "feature_matrix": [[.1,.9],[.9,.1]]
        })

class DDCR_get_associations_to_all_features(Resource):
    def get(self,cohort_id, feature, maximum_p_value):
        return json.dump([("A", "100", "200", ".4", ".1")])


api.add_resource(DDCR_get_ids_by_feature, '/get_ids_by_feature')
api.add_resource(DDCR_get_features_by_id, '/get_features_by_id/<string:cohort_id>')
api.add_resource(DDCR_get_feature_association, '/get_feature_association/<string:cohort_id>/<string:feature_a>/<string:feature_b>')
api.add_resource(DDCR_get_associations_to_all_features, '/get_associations_to_all_features/<string:cohort_id>/<string:feature>/<float:maximum_p_value>')

if __name__ == '__main__':
    app.run()