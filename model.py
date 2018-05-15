from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, Boolean, func, Float, Sequence
from sqlalchemy.sql import select
from scipy.stats import chisquare
import json
import os
from features import features

ddcr_user = os.environ["DDCR_DBUSER"]
ddcr_password = os.environ["DDCR_DBPASS"]
ddcr_host = os.environ["DDCR_HOST"]
ddcr_port = os.environ["DDCR_PORT"]
ddcr_database = os.environ["DDCR_DATABASE"]


metadata = MetaData()

pat_cols = [Column("patient_sk", Integer, primary_key=True)] + list(map(lambda feature: Column(feature[0], feature[1]), features))
pat = Table("patient", metadata, *pat_cols)

visit_cols = [Column("visit_sk", Integer, primary_key=True)] + list(map(lambda feature: Column(feature[0], feature[1]), features))
visit = Table("visit", metadata, *visit_cols)

cohort_cols = [
    Column("cohort_id", String),
    Column("upper_bound", Integer),
    Column("lower_bound", Integer),
    Column("features", String)
]

cohort = Table("cohort", metadata, *cohort_cols)


def get_db_connection():
    engine = create_engine("postgresql+psycopg2://"+ddcr_user+":"+ddcr_password+"@"+ddcr_host+":"+ddcr_port+"/"+ddcr_database)
    return engine.connect()


def filter_select(s, k, v):
    return {
        ">": lambda: s.where(pat.c[k] > v["value"]),
        "<": lambda: s.where(pat.c[k] < v["value"]),
        ">=": lambda: s.where(pat.c[k] >= v["value"]),
        "<=": lambda: s.where(pat.c[k] <= v["value"]),
        "=": lambda: s.where(pat.c[k] == v["value"]),
        "<>": lambda: s.where(pat.c[k] != v["value"])
    }[v["operator"]]()


def select_cohort(conn, cohort_features):
    s = select([pat.c.patient_sk])
    for k, v in cohort_features.items():
        s = filter_select(s, k, v)
    rs = list(conn.execute(s))
    next_val = conn.execute(Sequence("cohort_id"))
    cohort_id = "COHORT:"+str(next_val)
    if len(rs) <= 10:
        lower_bound = -1
        upper_bound = -1
    else:
        lower_bound = len(rs) // 10 * 10
        upper_bound = lower_bound + 9

    ins = cohort.insert().values(cohort_id=cohort_id, lower_bound=lower_bound, upper_bound=upper_bound, features=json.dumps(cohort_features, sort_keys=True))
    conn.execute(ins)
    return cohort_id, lower_bound, upper_bound


def get_ids_by_feature(conn, cohort_features):
    s = select([cohort.c.cohort_id, cohort.c.upper_bound, cohort.c.lower_bound]).where(
        cohort.c.features == json.dumps(cohort_features, sort_keys=True))
    rs = list(conn.execute(s))
    if len(rs) == 0:
        cohort_id, lower_bound, upper_bound = select_cohort(conn, cohort_features)
    else:
        [cohort_id, upper_bound, lower_bound] = rs[0]
    return cohort_id, lower_bound, upper_bound


def get_features_by_id(conn, cohort_id):
    s = select([cohort.c.features]).where(cohort.c.cohort_id == cohort_id)
    rs = list(conn.execute(s))
    if len(rs) == 0:
        return None
    else:
        return json.loads(rs[0][0])


def select_feature_matrix(conn, features, feature_a, feature_b):
    s = select([func.count(pat.c.patient_sk)])
    for k, v in features.items():
        s = filter_select(s, k, v)

    ka = feature_a["feature_name"]
    va = feature_a["feature_qualifier"]
    kb = feature_b["feature_name"]
    vb = feature_b["feature_qualifier"]
    
    sa = filter_select(s, ka, va)
    sb = filter_select(s, kb, vb)
    sab = filter_select(sa, kb, vb)
    
    total = conn.execute(s).scalar()
    a = conn.execute(sa).scalar()
    b = conn.execute(sb).scalar()
    ab = conn.execute(sab).scalar()

    feature_matrix = [[ab, a - ab],[b - ab, total - a - b + ab]]

    null = total / 2

    [chi_squared, p] = chisquare([feature_matrix[0][0], feature_matrix[0][1], feature_matrix[1][0], feature_matrix[1][1]], [null, null, null, null])

    return {
        "feature_matrix": feature_matrix,
        "p_value": p,
        "chi_squared": chi_squared
    }
    

def select_feature_association(conn, cohort_features, feature, maximum_p_value):
    rs = []
    for k, v in features:
        if v is Boolean:
            ret = select_feature_matrix(conn, cohort_features, feature, {"feature_name": k, "feature_qualifier": {"operator": "=", "value": True}})
            print(ret)
            if ret["p_value"] < maximum_p_value:
                rs.append(ret["feature_matrix"][0] + [ret["chi_squared"], ret["p_value"]])
    return rs


