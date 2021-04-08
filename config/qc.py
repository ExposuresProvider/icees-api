import sys
from yaml import safe_load
import Levenshtein
import argparse

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
        
    for t in topn:
        print(f"{t[0]} " + (f"possible match {t[1][0]}: {t[1][1]}" if t[1] is not None else "no match"))
    
    if n >= 0 and len(ls) > n:
        return print("...")
    
parser = argparse.ArgumentParser(description='ICEES FHIR-PIT QC Tool')
parser.add_argument('all_features', metavar='ALL_FEATURES', type=str, 
                    help='yaml file for all ICEES features')
parser.add_argument('mapping', metavar='MAPPING', type=str, 
                    help='yaml file for mapping from FHIR, etc. to ICEES features')
parser.add_argument('-n', metavar='N', type=int, default=-1,
                    help='number of entries to display, -1 for unlimited')
parser.add_argument('--ignore_suffix', metavar='IGNORE_SUFFIX', type=str, default="",
                    help='the suffix to ignore')
parser.add_argument("--similarity_threshold", metavar="SIMILARITY_THRESHOLD", type=float, default=0.5,
                    help="the threshold for similarity suggestions")

args = parser.parse_args()

all_features = args.all_features
mapping = args.mapping
n = args.n
ignore_suffix = args.ignore_suffix
similarity_threshold = args.similarity_threshold

with open(all_features) as ff:
    all_features_obj = safe_load(ff)

with open(mapping) as fm:
    mapping_obj = safe_load(fm)

mapping_var_names = set(mapping_obj["FHIR"].keys())

for table, table_features in all_features_obj.items():
    table_features_var_names = set(table_features.keys())
    print(f"feature vars table {table}")
    print(f"in mapping only:")
    truncate_set(mapping_var_names, table_features_var_names, n, ignore_suffix)
    print(f"in all_features only:")
    truncate_set(table_features_var_names, mapping_var_names, n, ignore_suffix)

    

