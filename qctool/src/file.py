import sys
from ruamel.yaml import YAML
from itertools import chain
import asyncio
from tx.functional.either import Left, Right
from io import StringIO
import logging

def update_key(d, ok, nk):
    curr_keys = list(d.keys())
    if ok in curr_keys:
        i = curr_keys.index(ok)
        d.insert(i, nk, d.pop(ok))
        return Right(())
    else:
        return Left(f"variable {ok} no longer exists")


# from https://stackoverflow.com/a/63179923
def object_to_yaml_str(yaml, obj, options=None):
    # show null
    def my_represent_none(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:null', u'null')
    yaml.representer.add_representer(type(None), my_represent_none)
    
    # 
    # the to-string part
    # 
    if options == None: options = {}
    string_stream = StringIO()
    yaml.dump(obj, string_stream, **options)
    output_str = string_stream.getvalue()
    string_stream.close()
    return output_str

    
class YAMLFile:
    def __init__(self, filename):
        self.yaml = YAML(typ="rt")
        with open(filename) as inf:
            self.obj = self.yaml.load(inf)

    async def dump(self, filename):
        with open(filename, "w+") as of:
            self.yaml.dump(self.obj, of)

    def dump_get(self, table, key):
        return self.get(table, key).rec(lambda y: y, lambda x: object_to_yaml_str(self.yaml, {key: x}))


class FeaturesFile(YAMLFile):

    def __init__(self, filename):
        super().__init__(filename)

    def get_keys(self, table):
        return self.obj[table].keys()
        
    def update_key(self, table, old_key, new_key):
        update_key(self.obj[table], old_key, new_key)

    def get(self, table, key):
        val = self.obj[table].get(key)
        if val is None:
            return Left(f"variable {key} no longer exists")
        else:
            return Right(val)

        
class IdentifiersFile(YAMLFile):

    def __init__(self, filename):
        super().__init__(filename)

    def get_keys(self, table):
        return self.obj[table].keys()
        
    def update_key(self, table, old_key, new_key):
        update_key(self.obj[table], old_key, new_key)

    def get(self, table, key):
        val = self.obj[table].get(key)
        if val is None:
            return Left(f"variable {key} no longer exists")
        else:
            return Right(val)

logger = logging.getLogger(__name__)

class MappingFile(YAMLFile):

    def __init__(self, filename):
        super().__init__(filename)

    def get_sub_objects(self):
        FHIR = self.obj.get("FHIR", {})
        GEOID = self.obj.get("GEOID", {})
        NearestRoad = self.obj.get("NearestRoad", {})
        NearestPoint = self.obj.get("NearestPoint", {})
        Visit = self.obj.get("Visit", {})
        return FHIR, GEOID, NearestRoad, NearestPoint, Visit

    def get_sub_keys(self, FHIR, GEOID, NearestRoad, NearestPoint, Visit):
        FHIR_keys = list(FHIR.keys())
        GEOID_keys = {name: list(dataset["columns"].values()) for name, dataset in GEOID.items()}
        NearestRoad_keys = {name: {"distance_feature_name": dataset["distance_feature_name"], "attributes_to_features_map": list(map(lambda x: x["feature_name"], dataset["attributes_to_features_map"].values()))} for name, dataset in NearestRoad.items()}
        NearestPoint_keys = {name: {"distance_feature_name": dataset["distance_feature_name"], "attributes_to_features_map": list(map(lambda x: x["feature_name"], dataset["attributes_to_features_map"].values()))} for name, dataset in NearestPoint.items()}
        Visit_keys = []
        return FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys
    
    def get_keys(self, table):
        FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys = self.get_sub_keys(*self.get_sub_objects())
        def list_keys(x):
            keys = []
            for _, a in x.items():
                keys.append(a["distance_feature_name"])
                keys.extend(a["attributes_to_features_map"])
            return keys
        return FHIR_keys + list(chain(*GEOID_keys.values())) + list_keys(NearestRoad_keys) + list_keys(NearestPoint_keys) + Visit_keys
        
    def update_key(self, table, old_key, new_key):
        FHIR, GEOID, NearestRoad, NearestPoint, Visit = self.get_sub_objects()
        FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys = self.get_sub_keys(FHIR, GEOID, NearestRoad, NearestPoint, Visit)
        if old_key in FHIR_keys:
            update_key(self.obj["FHIR"], old_key, new_key)
            return Right(())

        for name, keys in GEOID_keys.items():
            if old_key in keys:
                columns = GEOID[name]["columns"]
                for column_name, var_name in columns.items():
                    if var_name == old_key:
                        columns[column_name] = new_key
                        return Right(())

        for name, keys in NearestRoad_keys.items():
            if old_key in keys:
                nearest_road = NearestRoad[name]
                if old_key == nearest_road["distance_feature_name"]:
                    nearest_road["distance_feature_name"] = new_key
                else:
                    for attribute_name, feature in nearest_road["attributes_to_features_map"].items():
                        if feature["feature_name"] == old_key:
                            feature["feature_name"] = new_key
                            return Right(())
        
        for name, keys in NearestPoint_keys.items():
            if old_key in keys:
                nearest_point = NearestPoint[name]
                if old_key == nearest_point["distance_feature_name"]:
                    nearest_point["distance_feature_name"] = new_key
                else:
                    for attribute_name, feature in nearest_point["attributes_to_features_map"].items():
                        if feature["feature_name"] == old_key:
                            feature["feature_name"] = new_key
                            return Right(())
        
        return Left(f"variable {old_key} no longer exists")

    def get(self, table, key):
        FHIR, GEOID, NearestRoad, NearestPoint, Visit = self.get_sub_objects()
        FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys = self.get_sub_keys(FHIR, GEOID, NearestRoad, NearestPoint, Visit)
        if key in FHIR_keys:
            return Right(self.obj["FHIR"][key])

        for name, keys in GEOID_keys.items():
            if key in keys:
                return Right(GEOID[name])

        for name, keys in NearestRoad_keys.items():
            if old_key in keys:
                return Right(NearestRoad[name])
        
        for name, keys in NearestPoint_keys.items():
            if old_key in keys:
                return Right(NearestPoint[name])
        
        return Left(f"variable {old_key} no longer exists")


def make_file(ty, filename):
    if ty == "features":
        return FeaturesFile(filename)
    elif ty == "mapping":
        return MappingFile(filename)
    elif ty == "identifiers":
        return IdentifiersFile(filename)

    
