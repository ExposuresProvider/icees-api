from tabulate import tabulate

def feature_to_text(feature_name, feature_qualifier):
    op_form = {
        ">": lambda x: str(x["value"]),
        ">=": lambda x: str(x["value"]),
        "<": lambda x: str(x["value"]),
        "<=": lambda x: str(x["value"]),
        "=": lambda x: str(x["value"]),
        "<>": lambda x: str(x["value"]),
        "between": lambda x: "("+str(x["value_a"]) + "," + str(x["value_b"])+")",
        "in": lambda x: "["+",".join(map(str, x["values"]))+"]"
    }
    return feature_name + " " + feature_qualifier["operator"] + " " + op_form[feature_qualifier["operator"]](feature_qualifier)


def table_to_text(columns, rows):
    return tabulate(rows, columns, tablefmt="grid")


def format_tabular(term, data):
    tables = []
    format_tables(data, tables)
    string = term
    string += "\n"
    for table in tables:
        string += table_to_text(table[0], table[1])
        string += "\n"
    return string


def percentage_to_text(cell):
    return "{:0.2f}%".format(cell * 100)


def total_to_text(cell):
    return tabulate([
        [cell["frequency"]
    ], [
        percentage_to_text(cell["percentage"])
    ]], tablefmt="plain")


def cell_to_text(cell):
    return tabulate([
        [
            cell["frequency"], percentage_to_text(cell["row_percentage"])
        ], [
            percentage_to_text(cell["column_percentage"]), percentage_to_text(cell["total_percentage"])
        ]
    ], tablefmt="plain")


def format_tables(data, tables):
    if isinstance(data, str):
        columns = ["error"]
        rows = [[str(data)]]
        tables.append([columns, rows])
    elif "name" in data:
        columns = ["cohort_id", "name"]
        rows = [[data["cohort_id"], data["name"]]]
        tables.append([columns, rows])
    elif "features" in data:
        o = data["features"]
        features = ",".join([feature_to_text(a, b) for (a, b) in (o.items() if isinstance(o, dict)  else ((a["feature_name"], a["feature_qualifier"]) for a in o))])
        if "cohort_id" in data:
            columns = ["cohort_id", "size", "features"]
            rows = [[data["cohort_id"], data["size"], features]]
        else:
            columns = ["size", "features"]
            rows = [[data["size"], features]]
        tables.append([columns, rows])
    elif "identifiers" in data:
        tables.append([["identifier"], map(lambda x:[x], data["identifiers"])])
    elif "cohort_id" in data:
        columns = ["cohort_id", "size"]
        rows = [[data["cohort_id"], data["size"]]]
        tables.append([columns, rows])
    elif "feature_a" in data:
        feature_a = data["feature_a"]
        feature_b = data["feature_b"]
        feature_a_feature_name = feature_a["feature_name"]
        feature_a_feature_qualifiers = feature_a["feature_qualifiers"]
        feature_b_feature_name = feature_b["feature_name"]
        feature_b_feature_qualifiers = feature_b["feature_qualifiers"]

        columns = ["feature"] + list(map(lambda x: feature_to_text(feature_a_feature_name, x), feature_a_feature_qualifiers)) + [""]
        rows = [[a] + list(map(cell_to_text, b)) + [total_to_text(c)] for (a, b, c) in zip(list(map(lambda x: feature_to_text(feature_b_feature_name, x), feature_b_feature_qualifiers)), data["feature_matrix"], data["rows"])] + [[""] + list(map(total_to_text, data["columns"])) + [total_to_text({"frequency": data["total"], "percentage": 1})]]
        tables.append([columns, rows])

        columns = ["p_value", "chi_squared"]
        rows = [[data["p_value"], data["chi_squared"]]]
        p_value_corrected = data.get("p_value_corrected")
        if p_value_corrected is not None:
            columns.append("p_value_corrected")
            rows[0].append(p_value_corrected)
        tables.append([columns, rows])
    elif "feature" in data:
        feature = data["feature"]
        feature_feature_name = feature["feature_name"]
        feature_feature_qualifiers = feature["feature_qualifiers"]

        columns = ["feature", "count"]
        rows = [[a, b] for (a, b) in
                zip(list(map(lambda x: feature_to_text(feature_feature_name, x), feature_feature_qualifiers)), map(total_to_text, data["feature_matrix"]))]
        tables.append([columns, rows])

    elif isinstance(data, list):
        for d in data:
            format_tables(d, tables)
    elif isinstance(data, dict):
        columns = ["features"]
        rows = [[feature_to_text(a, b)] for (a, b) in data.items()]
        tables.append([columns, rows])
    else:
        columns = ["error"]
        rows = [[str(data)]]
        tables.append([columns, rows])






