import sys
from ruamel.yaml import YAML
import Levenshtein
import argparse
from argparse import RawTextHelpFormatter
from prettytable import PrettyTable
import difflib
from colorama import init, Fore, Back, Style
from itertools import chain
import asyncio

init()


def update_key(d, ok, nk):
    i = list(d.keys()).index(ok)
    d.insert(i, nk, d.pop(ok))


class File:
    async def dump(self, filename):
        async def write_file():
            with open(filename, "w+") as of:
                self.yaml.dump(self.obj, of)

        await write_file()
        


class FeaturesFile(File):

    def __init__(self, filename):
        self.yaml = YAML(typ="rt")
        with open(filename) as inf:
            self.obj = self.yaml.load(inf)

    def get_keys(self, table):
        return self.obj[table].keys()
        
    def update_key(self, table, old_key, new_key):
        update_key(self.obj[table], old_key, new_key)

        
class IdentifiersFile(File):

    def __init__(self, filename):
        self.yaml = YAML(typ="rt")
        with open(filename) as inf:
            self.obj = self.yaml.load(inf)

    def get_keys(self, table):
        return self.obj[table].keys()
        
    def update_key(self, table, old_key, new_key):
        update_key(self.obj[table], old_key, new_key)

    def dump(self, filename):
        with open(filename, "w+") as of:
            self.yaml.dump(self.obj, of)
        

class MappingFile(File):

    def __init__(self, filename):
        self.yaml = YAML(typ="rt")
        with open(filename) as inf:
            self.obj = self.yaml.load(inf)

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
        NearestRoad_keys = {name: list(map(lambda x: x["feature_name"], dataset["attributes_to_features_map"].values())) for name, dataset in NearestRoad.items()}
        NearestPoint_keys = {name: list(map(lambda x: x["feature_name"], dataset["attributes_to_features_map"].values())) for name, dataset in NearestPoint.items()}
        Visit_keys = []
        return FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys
    
    def get_keys(self, table):
        FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys = self.get_sub_keys(*self.get_sub_objects())
        return FHIR_keys + list(chain(GEOID_keys.values())) + list(chain(NearestRoad_keys.values())) + list(chain(NearestPoint_keys.values())) + Visit_keys
        
    def update_key(self, table, old_key, new_key):
        FHIR, GEOID, NearestRoad, NearestPoint, Visit = self.get_sub_objects()
        FHIR_keys, GEOID_keys, NearestRoad_keys, NearestPoint_keys, Visit_keys = self.get_sub_keys(FHIR, GEOID, NearestRoad, NearestPoint, Visit)
        if old_key in FHIR_keys:
            update_key(self.obj["FHIR"], old_key, new_key)
            return

        for name, keys in GEOID_keys.items():
            if old_key in keys:
                for column_name, var_name in GEOID[name]["columns"].items():
                    if var_name == old_key:
                        GEOID[name]["columns"][column_name] = new_key
                        return

        for name, keys in NearestRoad_keys.items():
            if old_key in keys:
                for attribute_name, feature in NearestRoad[name]["attributes_to_features_map"].items():
                    if feature["feature_name"] == old_key:
                        NearestRoad[name]["attributes_to_features_map"][attribute_name]["feature_name"] = new_key
                        return
        
        for name, keys in NearestPoint_keys.items():
            if old_key in keys:
                for attribute_name, feature in NearestPoint[name]["attributes_to_features_map"].items():
                    if feature["feature_name"] == old_key:
                        NearestPoint[name]["attributes_to_features_map"][attribute_name]["feature_name"] = new_key
                        return
        
        print("variable {old_key} no longer exists")


def make_file(ty, filename):
    if ty == "features":
        return FeaturesFile(filename)
    elif ty == "mapping":
        return MappingFile(filename)
    elif ty == "identifiers":
        return IdentifiersFile(filename)

    
def difference_ignore_suffix(a, b, ignore_suffix):
    diff = []
    for an in a:
        found = False
        for bn in b:
            if an == bn or an == bn + ignore_suffix or an + ignore_suffix == bn:
                found = True
                break
        if not found:
            diff.append(an)
    return diff
            
    
def truncate_set(a, b, similarity_threshold, n, ignore_suffix):
    diff_a = difference_ignore_suffix(a, b, ignore_suffix)
    diff_b = difference_ignore_suffix(b, a, ignore_suffix)

    def find_match(b, an):
        bn, ratio = max([(bn, Levenshtein.ratio(an, bn)) for bn in b], key=lambda x: x[1])
        return (an, bn, ratio)

    diff_a_match = [find_match(b, an) for an in diff_a]
    diff_b_match = [find_match(a, bn) for bn in diff_b]

    diff_b_match_switched = [(an, bn, ratio) for bn, an, ratio in diff_b_match]

    diff_match = sorted(list(set(diff_a_match) | set(diff_b_match_switched)), reverse=True, key=lambda t: t[2])
    
    ls = [(an, bn, ratio) if ratio >= similarity_threshold else (an, None, None) for an, bn, ratio in diff_match]

    if n == -1:
        topn = ls
    else:
        topn = ls[:n]
        
    return topn, n >= 0 and len(ls) > n


def colorize_diff(a, b):
    sm = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
    opcodes = sm.get_opcodes()
    a_colorized = ""
    b_colorized = ""
    ab = Back.BLUE
    bb = Back.BLUE
    for tag, i1, i2, j1, j2 in opcodes:
        a_segment = a[i1:i2]
        b_segment = b[j1:j2]
        if tag == "equal":
            a_colorized += a_segment
            b_colorized += b_segment
        elif tag == "delete":
            a_colorized += Fore.WHITE + ab + a_segment + Style.RESET_ALL
        elif tag == "insert":
            b_colorized += Fore.WHITE + bb + b_segment + Style.RESET_ALL
        elif tag == "replace":
            a_colorized += Fore.WHITE + ab + a_segment + Style.RESET_ALL
            b_colorized += Fore.WHITE + bb + b_segment + Style.RESET_ALL
    return (a_colorized, b_colorized)
    
    
def f(l):
    if l[1] is None:
        return [l[0], "", ""]
    else:
        return list(colorize_diff(l[0], l[1])) + [f"{l[2]:.2f}"]


def print_matches(left, right, table, ellipsis):
    table_copy = list(table)
    if ellipsis:
        table_copy.append(["...", None, None])

    x = PrettyTable()
    x.field_names = [left, right, "ratio"]
        
    x.add_rows(map(f, table_copy))
    print(x)
    
    
async def interactive_update(left, right, a_file, b_file, a_update, b_update, table_name, table, ellipsis):
    done = False
    n = len(table)
    aws = []
    for i, row in enumerate(table):

        print(f"{i+1} / {n}")
        x = PrettyTable()
        x.field_names = [left, right, "ratio"]
        x.add_row(f(row))
        
        print(x)

        if row[0] is not None and row[1] is not None:
            while True:
                if b_file is not None:
                    print("a) use a")
                if a_file is not None:
                    print("b) use b")
                    if b_file is not None:
                        print("c) customize")
                print("""s) skip
e) exit""")
                action = input()

                if action == "a":
                    b_file.update_key(table_name, row[1], row[0])
                    if b_update is not None:
                        aws.append(b_file.dump(b_update))
                    break
                elif action == "b":
                    a_file.update_key(table_name, row[0], row[1])
                    if a_update is not None:
                        aws.append(a_file.dump(a_update))
                    break
                elif action == "c":
                    var_name = input("input variable name: ")
                    a_file.update_key(table_name, row[0], var_name)
                    b_file.update_key(table_name, row[1], var_name)
                    if a_update is not None:
                        aws.append(a_file.dump(a_update))
                    if b_update is not None:
                        aws.append(b_file.dump(b_update))
                    break
                elif action == "s":
                    break
                elif action == "e":
                    done = True
                    break
                else:
                    print("unsupported action, try again")
            if done:
                break
                
    if ellipsis:
        print("more diff remaining")

    await asyncio.gather(*aws)
    
    
async def main():
    parser = argparse.ArgumentParser(description="""ICEES FHIR-PIT QC Tool

Compare feature variable names in two files. Use --a and --b to specify filenames, --a_type and --b_type to specify file types, --update_a and --update_b to specify output files. Files types are one of features, mapping, and identifiers. If --update_a or --update_b is not specified then the files cannot be updated.""", formatter_class=RawTextHelpFormatter)
    parser.add_argument('--a', metavar='A', type=str, required=True,
                        help='file a')
    parser.add_argument('--b', metavar='B', type=str, required=True,
                        help='file b')
    parser.add_argument('--a_type', metavar='A_TYPE', choices=["features", "mapping", "identifiers"], required=True,
                        help='type of file a')
    parser.add_argument('--b_type', metavar='B_TYPE', choices=["features", "mapping", "identifiers"], required=True,
                        help='type of file b')
    parser.add_argument('--number_entries', metavar='NUMBER_ENTRIES', type=int, default=-1,
                        help='number of entries to display, -1 for unlimited')
    parser.add_argument('--ignore_suffix', metavar='IGNORE_SUFFIX', type=str, default="",
                        help='the suffix to ignore')
    parser.add_argument("--similarity_threshold", metavar="SIMILARITY_THRESHOLD", type=float, default=0.5,
                        help="the threshold for similarity suggestions")
    parser.add_argument('--table', metavar='TABLE', type=str, required=True, nargs="+",
                        help='tables')
    parser.add_argument("--update_a", metavar="UPDATE_A", type=str,
                        help="YAML file for the updated a. If this file is not specified then a cannot be updated")
    parser.add_argument("--update_b", metavar="UPDATE_B", type=str,
                        help="YAML file for the updated b. If this file is not specified then b cannot be updated")

    args = parser.parse_args()

    a_filename = args.a
    a_type = args.a_type
    a_update = args.update_a

    b_filename = args.b
    b_type = args.b_type
    b_update = args.update_b

    tables = args.table
    n = args.number_entries
    ignore_suffix = args.ignore_suffix
    similarity_threshold = args.similarity_threshold

    interactive = a_update is not None or b_update is not None

    try:
        a_file = make_file(a_type, a_filename)
    except Exception as e:
        print(f"error loading {a_filename}: {e}")
        sys.exit(-1)

    try:
        b_file = make_file(b_type, b_filename)
    except Exception as e:
        print(f"error loading {b_filename}: {e}")
        sys.exit(-1)

    for table in tables:
        a_var_names = a_file.get_keys(table)
        b_var_names = b_file.get_keys(table) 
        print(f"feature vars table {table}:")
        print(f"{a_filename} diff {b_filename}:")
        if interactive:
            await interactive_update(a_filename, b_filename, a_file, b_file, a_update, b_update, table, *truncate_set(a_var_names, b_var_names, similarity_threshold, n, ignore_suffix))
        else:
            print_matches(a_filename, b_filename, *truncate_set(a_var_names, b_var_names, similarity_threshold, n, ignore_suffix))

asyncio.run(main())    

