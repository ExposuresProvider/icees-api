import csv
import os
import logging

logger = logging.getLogger(__name__)

pat_dict = {}
visit_dict = {}
input_file = os.path.join(os.path.dirname(__file__), "ICEES_Identifiers_v7 06.03.19.csv")
with open(input_file, newline="") as f:
    csvreader = csv.reader(f, delimiter=",", quotechar="\"")
    next(csvreader)
    for row in csvreader:
        row2 = filter(lambda x : x != "", map(lambda x : x.strip(), row))
        pat = next(row2)
        visit = next(row2)
        ids = list(row2)
        if pat != "N/A":
            pat_dict[pat] = ids
        if visit != "N/A":
            visit_dict[visit] = ids

def get_identifiers(table, feature, return_empty_list=False):
    if table == "patient":
        identifier_dict = pat_dict
    elif table == "visit":
        identifier_dict = visit_dict
    else:
        raise RuntimeError("Cannot find table " + table)
    feature2 = feature.split("_")[0]
    if feature2.endswith("Exposure"):
        feature2 = feature2[:-8]
        i = len(feature2) - 1
        while i >= 0 and not feature2[i].isupper():
            i -= 1
        while i - 1 >= 0:
            if feature2[i - 1].isupper():
                i -= 1
            else:
                break
        feature2 = feature2[i:]

    if feature2 == "Sex2":
        feature2 = "Sex"
    if feature2 in identifier_dict:
        return identifier_dict[feature2]
    else:
        errmsg = "Cannot find identifiers for feature " + feature
        logger.error(errmsg)
        if return_empty_list:
            return []
        else:
            raise RuntimeError(errmsg)
