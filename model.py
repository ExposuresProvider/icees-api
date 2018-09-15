from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, func, Sequence
from sqlalchemy.sql import select
from scipy.stats import chisquare
import json
import os
from features import features

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

cohort_cols = [
    Column("cohort_id", String),
    Column("table", String),
    Column("year", Integer),
    Column("size", Integer),
    Column("features", String)
]

cohort = Table("cohort", metadata, *cohort_cols)


def get_db_connection(version):
    engine = create_engine("postgresql+psycopg2://"+serv_user+":"+serv_password+"@"+serv_host+":"+serv_port+"/"+serv_database[version])
    return engine.connect()


def filter_select(s, table, k, v):
    return {
        ">": lambda: s.where(table.c[k] > v["value"]),
        "<": lambda: s.where(table.c[k] < v["value"]),
        ">=": lambda: s.where(table.c[k] >= v["value"]),
        "<=": lambda: s.where(table.c[k] <= v["value"]),
        "=": lambda: s.where(table.c[k] == v["value"]),
        "<>": lambda: s.where(table.c[k] != v["value"])
    }[v["operator"]]()


def opposite(qualifier):
    return {
        "operator": {
            ">": "<=",
            "<": ">=",
            ">=": "<",
            "<=": ">",
            "=": "<>",
            "<>": "="
        }[qualifier["operator"]],
        "value": qualifier["value"]
    }


def select_cohort(conn, table_name, year, cohort_features, cohort_id=None):
    table = tables[table_name]
    s = select([func.count()]).select_from(table).where(table.c.year == year)
    for k, v in cohort_features.items():
        s = filter_select(s, table, k, v)

    n = conn.execute(s).scalar()
    if n <= 10:
        return None, -1, -1
    else:
        size = n
        while cohort_id is None:
            next_val = conn.execute(Sequence("cohort_id"))
            cohort_id = "COHORT:" + str(next_val)
            if cohort_id_in_use(conn, cohort_id):
                cohort_id = None

        if cohort_id_in_use(conn, cohort_id):
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

    null_matrix = [[div(r * c, total) for c in total_cols] for r in total_rows]

    [chi_squared, p] = chisquare(join_lists(feature_matrix), join_lists(null_matrix))

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

    bins = len(feature_matrix)
    
    null_matrix = [total / bins for _ in range(bins)]

    [chi_squared, p] = chisquare(feature_matrix, null_matrix)

    return {
        "feature": feature_a,
        "feature_matrix": [{"frequency": a, "percentage": b} for (a, b) in zip(feature_matrix, feature_percentage)],
        "p_value": p,
        "chi_squared": chi_squared
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


