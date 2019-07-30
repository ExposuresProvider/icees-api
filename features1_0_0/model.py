from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, func, Sequence, between
from sqlalchemy.sql import select
from scipy.stats import chi2_contingency
import json
import os
from .features import features
import numpy as np

eps = np.finfo(float).eps

service_name = "ICEES"

serv_user = os.environ[service_name + "_DBUSER"]
serv_password = os.environ[service_name + "_DBPASS"]
serv_host = os.environ[service_name + "_HOST"]
serv_port = os.environ[service_name + "_PORT"]
serv_database = json.loads(os.environ[service_name + "_DATABASE"])


metadata = MetaData()

pat_cols = [Column("PatientId", String, primary_key=True), Column("year", Integer)] + list(map(lambda feature: Column(feature[0], feature[1]), features["patient"]))

visit_cols = [Column("VisitId", String, primary_key=True), Column("year", Integer)] + list(map(lambda feature: Column(feature[0], feature[1]), features["visit"]))

tables = {
    "patient": Table("patient", metadata, *pat_cols),
    "visit": Table("visit", metadata, *visit_cols)
}

name_table = Table("name", metadata, Column("name", String, primary_key=True), Column("cohort_id", String), Column("table", String))

cohort_cols = [
    Column("cohort_id", String),
    Column("table", String),
    Column("year", Integer),
    Column("size", Integer),
    Column("features", String)
]

cohort = Table("cohort", metadata, *cohort_cols)


engine_map = {}

def get_db_connection(version="1.0.0"):
    if hasattr(engine_map, version):
        engine = engine_map[version]
    else:
        engine = create_engine("postgresql+psycopg2://"+serv_user+":"+serv_password+"@"+serv_host+":"+serv_port+"/"+serv_database[version])
        engine_map[version] = engine

    return engine.connect()


def filter_select(s, table, k, v):
    return {
        ">": lambda: s.where(table.c[k] > v["value"]),
        "<": lambda: s.where(table.c[k] < v["value"]),
        ">=": lambda: s.where(table.c[k] >= v["value"]),
        "<=": lambda: s.where(table.c[k] <= v["value"]),
        "=": lambda: s.where(table.c[k] == v["value"]),
        "<>": lambda: s.where(table.c[k] != v["value"]),
        "between": lambda: s.where(between(table.c[k], v["value_a"], v["value_b"])),
        "in": lambda: s.where(table.c[k].in_(v["values"]))
    }[v["operator"]]()


def select_cohort(conn, table_name, year, cohort_features, cohort_id=None):
    table = tables[table_name]
    s = select([func.count()]).select_from(table).where(table.c.year == year)
    for k, v in cohort_features.items():
        s = filter_select(s, table, k, v)

    n = conn.execute(s).scalar()
    if n <= 10:
        return None, -1
    else:
        size = n
        while cohort_id is None:
            next_val = conn.execute(Sequence("cohort_id"))
            cohort_id = "COHORT:" + str(next_val)
            if cohort_id_in_use(conn, cohort_id):
                cohort_id = None

        if cohort_id_in_use(conn, cohort_id):
            raise RuntimeError("Cohort id is in use.")
            ins = cohort.update().where(cohort.c.cohort_id == cohort_id).values(size=size,
                                                                    features=json.dumps(cohort_features,
                                                                                        sort_keys=True),
                                                                    table=table_name, year=year)
        else:
            ins = cohort.insert().values(cohort_id=cohort_id, size=size,
                                         features=json.dumps(cohort_features, sort_keys=True), table=table_name,
                                         year=year)

        conn.execute(ins)
        return cohort_id, size


def get_ids_by_feature(conn, table_name, year, cohort_features):
    s = select([cohort.c.cohort_id, cohort.c.size]).where(cohort.c.table == table_name).where(cohort.c.year == year).where(
        cohort.c.features == json.dumps(cohort_features, sort_keys=True))
    rs = list(conn.execute(s))
    if len(rs) == 0:
        cohort_id, size = select_cohort(conn, table_name, year, cohort_features)
    else:
        [cohort_id, size] = rs[0]
    return cohort_id, size


def get_features_by_id(conn, table_name, year, cohort_id):
    s = select([cohort.c.features]).where(cohort.c.cohort_id == cohort_id).where(cohort.c.table == table_name).where(cohort.c.year == year)
    rs = list(conn.execute(s))
    if len(rs) == 0:
        return None
    else:
        return json.loads(rs[0][0])


def get_cohort_by_id(conn, table_name, year, cohort_id):
    s = select([cohort.c.features,cohort.c.size]).where(cohort.c.cohort_id == cohort_id).where(cohort.c.table == table_name).where(cohort.c.year == year)
    rs = list(conn.execute(s))
    if len(rs) == 0:
        return None
    else:
        return {
            "size": rs[0][1],
            "features": json.loads(rs[0][0])
        }


def get_cohort_features(conn, table_name, year, cohort_features):
    table = tables[table_name]
    rs = []
    for k, v, levels in features[table_name]:
        if levels is None:
            levels = get_feature_levels(conn, table, year, k)
        ret = select_feature_count(conn, table_name, year, cohort_features, {"feature_name": k, "feature_qualifiers": list(map(lambda level: {"operator": "=", "value": level}, levels))})
        rs.append(ret)
    return rs


def get_cohort_dictionary(conn, table_name, year):
    s = select([cohort.c.cohort_id,cohort.c.features,cohort.c.size]).where(cohort.c.table == table_name).where(cohort.c.year == year)
    rs = []
    for cohort_id, features, size in conn.execute(s):
        rs.append({
            "cohort_id": cohort_id,
            "size": size,
            "features": json.loads(features)
        })
    return rs


def cohort_id_in_use(conn, cohort_id):
    return conn.execute(select([func.count()]).select_from(cohort).where(cohort.c.cohort_id == cohort_id)).scalar() > 0


def join_lists(lists):
    return [x for l in lists for x in l]


def div(a,b):
    if b != 0:
        return a / b
    else:
        return float("NaN")


def add_eps(a):
    return a + eps


def select_feature_matrix(conn, table_name, year, cohort_features, feature_a, feature_b):
    table = tables[table_name]
    s = select([func.count()]).select_from(table).where(table.c.year == year)
    for k, v in cohort_features.items():
        s = filter_select(s, table, k, v)

    ka = feature_a["feature_name"]
    vas = feature_a["feature_qualifiers"]
    kb = feature_b["feature_name"]
    vbs = feature_b["feature_qualifiers"]

    feature_matrix = [
        [conn.execute(filter_select(filter_select(s, table, kb, vb), table, ka, va)).scalar() for va in vas] for vb in vbs
    ]

    total_cols = [conn.execute(filter_select(s, table, ka, va)).scalar() for va in vas]
    total_rows = [conn.execute(filter_select(s, table, kb, vb)).scalar() for vb in vbs]

    total = conn.execute(s).scalar()

    chi_squared, p, *_ = chi2_contingency(list(map(lambda x : list(map(add_eps, x)), feature_matrix)), correction=False)

    feature_matrix2 = [
        [
            {
                "frequency": cell,
                "row_percentage": div(cell, total_rows[i]),
                "column_percentage": div(cell, total_cols[j]),
                "total_percentage": div(cell, total)
            } for j, cell in enumerate(row)
        ] for i, row in enumerate(feature_matrix)
    ]

    return {
        "feature_a": feature_a,
        "feature_b": feature_b,
        "feature_matrix": feature_matrix2,
        "rows": [{"frequency": a, "percentage": b} for (a,b) in zip(total_rows, map(lambda x: x/total, total_rows))],
        "columns": [{"frequency": a, "percentage": b} for (a,b) in zip(total_cols, map(lambda x: x/total, total_cols))],
        "total": total,
        "p_value": p,
        "chi_squared": chi_squared
    }


def select_feature_count(conn, table_name, year, cohort_features, feature_a):
    table = tables[table_name]
    s = select([func.count()]).select_from(table).where(table.c.year == year)
    for k, v in cohort_features.items():
        s = filter_select(s, table, k, v)

    ka = feature_a["feature_name"]
    vas = feature_a["feature_qualifiers"]

    feature_matrix = [conn.execute(filter_select(s, table, ka, va)).scalar() for va in vas]
    
    total = conn.execute(s).scalar()

    feature_percentage = map(lambda x: x/total, feature_matrix)

    return {
        "feature": feature_a,
        "feature_matrix": [{"frequency": a, "percentage": b} for (a, b) in zip(feature_matrix, feature_percentage)]
    }


def get_feature_levels(conn, table, year, feature):
    s = select([table.c[feature]]).where(table.c.year == year).distinct().order_by(table.c[feature])
    return list(map(lambda row: row[0], conn.execute(s)))


def select_feature_association(conn, table_name, year, cohort_features, feature, maximum_p_value):
    table = tables[table_name]
    rs = []
    for k, v, levels in features[table_name]:
        if levels is None:
            levels = get_feature_levels(conn, table, year, k)
        ret = select_feature_matrix(conn, table_name, year, cohort_features, feature, {"feature_name": k, "feature_qualifiers": list(map(lambda level: {"operator": "=", "value": level}, levels))})
        if ret["p_value"] < maximum_p_value:
            rs.append(ret)
    return rs


def select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value):
    cohort_features = get_features_by_id(conn, table, year, cohort_id)
    if cohort_features is None:
        return "Input cohort_id invalid. Please try again."
    else:
        return select_feature_association(conn, table, year, cohort_features, feature, maximum_p_value)


def validate_range(table_name, feature):
    feature_name = feature["feature_name"]
    values = feature["feature_qualifiers"]
    _, ty, levels = next(filter(lambda x: x[0] == feature_name, features[table_name]))
    if levels:
        n = len(levels)
        coverMap = [False for _ in levels]
        vMap = {
            ">" : lambda x: (lambda i: [False] * (i + 1) + [True] * (n - i - 1))(levels.index(x["value"])),
            "<" : lambda x: (lambda i: [True] * i + [False] * (n - i))(levels.index(x["value"])),
            ">=" : lambda x: (lambda i: [False] * i + [True] * (n - i))(levels.index(x["value"])),
            "<=" : lambda x: (lambda i: [True] * (i + 1) + [False] * (n - i - 1))(levels.index(x["value"])),
            "=" : lambda x: (lambda i: [False] * i + [True] + [False] * (n - i - 1))(levels.index(x["value"])),
            "<>" : lambda x: (lambda i: [True] * i + [False] + [True] * (n - i - 1))(levels.index(x["value"])),
            "between" : lambda x: (lambda ia, ib: [False] * ia + [True] * (ib - ia + 1) + [False] * (n - ib - 1))(levels.index(x["value_a"]), levels.index(x["value_b"])),
            "in" : lambda x: map(lambda a: a in x["values"], levels)
        }
        for v in values:
            updateMap = vMap[v["operator"]](v)
            coverMapUpdate = []
            for i, (c, u) in enumerate(zip(coverMap, updateMap)):
                if c and u:
                    raise RuntimeError("over lapping value " + str(levels[i]) + ", input feature qualifiers " + str(feature))
                else:
                    coverMapUpdate.append(c or u)
            coverMap = coverMapUpdate
            print(coverMap)
        for i, c in enumerate(coverMap):
            if not c:
                raise RuntimeError("incomplete value coverage " + str(levels[i]) + ", input feature qualifiers " + str(feature))
    else:
        print("warning: cannot validate feature " + feature_name + " in table " + table_name + " because its levels are not provided")


def get_id_by_name(conn, table, name):
    s = select([func.count()]).select_from(name_table).where((name_table.c.name == name) & (name_table.c.table == table))
    n = conn.execute(s).scalar()
    if n == 0:
        raise RuntimeError("Input name invalid. Please try again.")
    else:
        s = select([name_table.c.cohort_id]).select_from(name_table).where((name_table.c.name == name) & (name_table.c.table == table))
        cohort_id = conn.execute(s).scalar()

        return {
            "cohort_id": cohort_id,
            "name" : name
        }


def add_name_by_id(conn, table, name, cohort_id):
    s = select([func.count()]).select_from(name_table).where((name_table.c.name == name) & (name_table.c.table == table))
    n = conn.execute(s).scalar()
    if n == 1:
        raise RuntimeError("Name is already taken. Please choose another name.")
    else:
        i = name_table.insert().values(name=name, table=table, cohort_id=cohort_id)
        conn.execute(i)

        return {
            "cohort_id": cohort_id,
            "name" : name
        }

