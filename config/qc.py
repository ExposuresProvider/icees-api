import sys
from ruamel.yaml import YAML
import Levenshtein
import argparse
from prettytable import PrettyTable
import difflib
from colorama import init, Fore, Back, Style

init()

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
            
    
def truncate_set(a, b, n, ignore_suffix):
    diff = difference_ignore_suffix(a, b, ignore_suffix)
    l = list(diff)

    levenshtein = lambda x: x[1]
    ls0 = sorted([(an, max([(bn, Levenshtein.ratio(an, bn)) for bn in b], key=levenshtein)) for an in l], reverse=True, key=lambda t: levenshtein(t[1]))
    
    ls = [(an, (bn, ratio)) if ratio >= similarity_threshold else (an, None) for an, (bn, ratio) in ls0]

    if n == -1:
        topn = ls
    else:
        topn = ls[:n]
        
    return [[t[0], t[1][0], t[1][1]] if t[1] is not None else [t[0], "", ""] for t in topn], n >= 0 and len(ls) > n


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
    
    
def print_matches(left, right, table, ellipsis):
    table_copy = list(table)
    if ellipsis:
        table_copy.append(["...", "", ""])

    x = PrettyTable()
    x.field_names = [left, right, "ratio"]
    def f(l):
        if l[1] == "":
            return [l[0], l[1], l[2]]
        else:
            return list(colorize_diff(l[0], l[1])) + [f"{l[2]:.2f}"]
        
    x.add_rows(map(f, table_copy))
    print(x)
    
    
parser = argparse.ArgumentParser(description='ICEES FHIR-PIT QC Tool')
parser.add_argument('--features', metavar='FEATURES', type=str, required=True,
                    help='yaml file for ICEES features')
parser.add_argument('--mapping', metavar='MAPPING', type=str, required=True,
                    help='yaml file for mapping from FHIR, etc. to ICEES features')
parser.add_argument('--identifiers', metavar='IDENTIFIERS', type=str, required=True,
                    help='yaml file for identifiers for ICEES features')
parser.add_argument('-n', metavar='N', type=int, default=-1,
                    help='number of entries to display, -1 for unlimited')
parser.add_argument('--ignore_suffix', metavar='IGNORE_SUFFIX', type=str, default="",
                    help='the suffix to ignore')
parser.add_argument("--similarity_threshold", metavar="SIMILARITY_THRESHOLD", type=float, default=0.5,
                    help="the threshold for similarity suggestions")
parser.add_argument("--update_features", metavar="UPDATE_FEATURES", type=str,
                    help="yaml file for the updated features. if this file is not specified then the features cannot be updated")
parser.add_argument("--update_mapping", metavar="UPDATE_MAPPING", type=str,
                    help="yaml file for the updated mapping. if this file is not specified then the mapping cannot be updated")
parser.add_argument("--update_identifiers", metavar="update_identifiers", type=str,
                    help="yaml file for the updated identifiers. if this file is not specified then the identifiers cannot be updated")

args = parser.parse_args()

features = args.features
mapping = args.mapping
identifiers = args.identifiers
n = args.n
ignore_suffix = args.ignore_suffix
similarity_threshold = args.similarity_threshold
update_features = args.update_features
update_mapping = args.update_mapping
update_identifiers = args.update_identifiers

interactive_mapping = update_features is not None or update_mapping is not None
interacitve_identifiers = update_features is not None or update_identifiers is not None

yaml = YAML(typ="safe")

try:
    with open(features) as ff:
        features_obj = yaml.load(ff)
except Exception as e:
    print(f"error loading features yaml: {e}")
    sys.exit(-1)

try:
    with open(mapping) as fm:
        mapping_obj = yaml.load(fm)
except Exception as e:
    print(f"error loading mapping yaml: {e}")
    sys.exit(-1)

try:
    with open(identifiers) as fi:
        identifiers_obj = yaml.load(fi)
except Exception as e:
    print(f"error loading identifiers yaml: {e}")
    sys.exit(-1)

try:
    mapping_var_names = set(mapping_obj["FHIR"].keys())
except Exception as e:
    print(f"error reading mapping yaml: {e}")
    sys.exit(-1)
    
for table, table_features in features_obj.items():
    table_features_var_names = set(table_features.keys())
    print(f"feature vars table {table}:")
    print(f"mapping diff features:")
    print_matches("mapping", "features", *truncate_set(mapping_var_names, table_features_var_names, n, ignore_suffix))
#    print(f"features diff mapping:")
#    print_matches("features", "mapping", *truncate_set(table_features_var_names, mapping_var_names, n, ignore_suffix))

    try:
        identifiers_var_names = set(identifiers_obj[table].keys())
    except Exception as e:
        print(f"error reading identifiers yaml: {e}")
        sys.exit(-1)

    print(f"identifiers diff features:")
    print_matches("identifiers", "features", *truncate_set(identifiers_var_names, table_features_var_names, n, ignore_suffix))
#    print(f"features diff identifiers:")
#    print_matches("features", "identifiers", *truncate_set(table_features_var_names, identifiers_var_names, n, ignore_suffix))

    

