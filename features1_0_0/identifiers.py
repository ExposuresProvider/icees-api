import csv

pat_dict = {}
visit_dict = {}
input_file = "ICEES_Identifiers_12.04.18.csv"
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

def get_identifiers(table, feature):
    if table == "patient":
        identifier_dict = pat_dict
    elif table == "visit":
        identifier_dict = visit_dict
    else:
        raise RuntimeError("Cannot find table " + table)
    if feature in identifier_dict:
        return identifier_dict[feature]
    else:
        raise RuntimeError("Cannot find identifiers for feature " + feature)
