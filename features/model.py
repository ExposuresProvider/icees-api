from sqlalchemy import Table, Column, Integer, String, MetaData, func, Sequence, between, Index, text, case, and_, DateTime, Text, LargeBinary
from sqlalchemy.orm import aliased
from sqlalchemy.sql import select, func
from scipy.stats import chi2_contingency
import json
import os
from itertools import product, chain
import inflection
import numpy as np
import time
from .features import features, lookUpFeatureClass, features_dict
from tx.functional.maybe import Nothing, Just
import logging
from datetime import datetime, timezone
from collections import defaultdict
from functools import cmp_to_key
from hashlib import md5

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

eps = np.finfo(float).eps

metadata = MetaData()

def table_id(table):
    return table[0].upper() + table[1:] + "Id"

table_cols = {
    table: [Column(table_id(table), Integer), Column("year", Integer)] + list(map(lambda feature: Column(feature[0], feature[1]), table_features)) for table, table_features in features.items()
}

tables = {
    table : Table(table, metadata, *tab_cols) for table, tab_cols in table_cols.items()
}

name_table = Table("name", metadata, Column("name", String, primary_key=True), Column("cohort_id", String), Column("table", String))

cohort_cols = [
    Column("cohort_id", String, primary_key=True),
    Column("table", String),
    Column("year", Integer),
    Column("size", Integer),
    Column("features", String)
]

cohort = Table("cohort", metadata, *cohort_cols)

cohort_id_seq = Sequence('cohort_id_seq', metadata=metadata)


association_cols = [
    Column("digest", LargeBinary),
    Column("table", String),
    Column("cohort_features", String),
    Column("cohort_year", Integer),
    Column("feature_a", String),
    Column("feature_b", String),
    Column("association", Text),
    Column("access_time", DateTime)
]

cache = Table("cache", metadata, *association_cols)

Index("cache_index", cache.c.digest)

count_cols = [
    Column("digest", LargeBinary),
    Column("table", String),
    Column("cohort_features", String),
    Column("cohort_year", Integer),
    Column("feature_a", String),
    Column("count", Text),
    Column("access_time", DateTime)
]

cache_count = Table("cache_count", metadata, *count_cols)

Index("cache_count_index", cache_count.c.digest)


def get_digest(*args):
    c = md5()
    for arg in args:
        c.update(arg.encode("utf-8"))
    return c.digest()

    
def op_dict(table, k, v):
    return {
        ">": lambda: table.c[k] > v["value"],
        "<": lambda: table.c[k] < v["value"],
        ">=": lambda: table.c[k] >= v["value"],
        "<=": lambda: table.c[k] <= v["value"],
        "=": lambda: table.c[k] == v["value"],
        "<>": lambda: table.c[k] != v["value"],
        "between": lambda: between(table.c[k], v["value_a"], v["value_b"]),
        "in": lambda: table.c[k].in_(v["values"])
    }[v["operator"]]()


def filter_select(s, table, k, v):
    return s.where(op_dict(table, k, v))


def case_select(table, k, v):
    return func.coalesce(func.sum(case([(op_dict(table, k, v), 1)], else_=0)), 0)


def case_select2(table, table2, k, v, k2, v2):
    return func.coalesce(func.sum(case([(and_(op_dict(table, k, v), op_dict(table2, k2, v2)), 1)], else_=0)), 0)


def select_cohort(conn, table_name, year, cohort_features, cohort_id=None):

    cohort_features_norm = normalize_features(year, cohort_features)
    
    table, _ = generate_tables_from_features(table_name, cohort_features_norm, year, [])
    
    s = select([func.count()]).select_from(table)

    n = conn.execute((s)).scalar()
    if n <= 10:
        return None, -1
    else:
        size = n
        while cohort_id is None:
            next_val = conn.execute(cohort_id_seq)
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

        conn.execute((ins))
        return cohort_id, size


def get_ids_by_feature(conn, table_name, year, cohort_features):
    s = select([cohort.c.cohort_id, cohort.c.size]).where(cohort.c.table == table_name).where(cohort.c.year == year).where(
        cohort.c.features == json.dumps(cohort_features, sort_keys=True))
    rs = list(conn.execute((s)))
    if len(rs) == 0:
        cohort_id, size = select_cohort(conn, table_name, year, cohort_features)
    else:
        [cohort_id, size] = rs[0]
    return cohort_id, size


def get_features_by_id(conn, table_name, cohort_id):
    s = select([cohort.c.features, cohort.c.year]).where(cohort.c.cohort_id == cohort_id).where(cohort.c.table == table_name)
    rs = list(conn.execute((s)))
    if len(rs) == 0:
        return None
    else:
        return json.loads(rs[0][0]), rs[0][1]


def get_cohort_by_id(conn, table_name, year, cohort_id):
    s = select([cohort.c.features,cohort.c.size]).where(cohort.c.cohort_id == cohort_id).where(cohort.c.table == table_name).where(cohort.c.year == year)
    rs = list(conn.execute((s)))
    if len(rs) == 0:
        return None
    else:
        return {
            "size": rs[0][1],
            "features": json.loads(rs[0][0])
        }


def get_cohort_features(conn, table_name, year, cohort_features, cohort_year):
    table = tables[table_name]
    rs = []
    for k, v, levels, _ in features[table_name]:
        if levels is None:
            levels = get_feature_levels(conn, table, year, k)
        ret = select_feature_count(conn, table_name, year, cohort_features, cohort_year, {"feature_name": k, "feature_qualifiers": list(map(lambda level: {"operator": "=", "value": level}, levels))})
        rs.append(ret)
    return rs


def get_cohort_dictionary(conn, table_name, year):
    s = select([cohort.c.cohort_id,cohort.c.features,cohort.c.size]).where(cohort.c.table == table_name).where(cohort.c.year == year)
    rs = []
    for cohort_id, features, size in conn.execute((s)):
        rs.append({
            "cohort_id": cohort_id,
            "size": size,
            "features": json.loads(features)
        })
    return rs


def get_cohort_definition_by_id(conn, cohort_id):
    s = select([cohort.c.cohort_id,cohort.c.features,cohort.c.size,cohort.c.table,cohort.c.year]).where(cohort.c.cohort_id == cohort_id)
    for cohort_id, features, size, table, year in conn.execute((s)):
        return Just({
            "cohort_id": cohort_id,
            "size": size,
            "features": json.loads(features),
            "table": table,
            "year": year
        })
    return Nothing 


def cohort_id_in_use(conn, cohort_id):
    return conn.execute((select([func.count()]).select_from(cohort).where(cohort.c.cohort_id == cohort_id))).scalar() > 0


def join_lists(lists):
    return [x for l in lists for x in l]


def div(a,b):
    if b != 0:
        return a / b
    else:
        return float("NaN")


def add_eps(a):
    return a + eps


MAX_ENTRIES_PER_ROW = int(os.environ.get("MAX_ENTRIES_PER_ROW", "1664"))

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logger.info(f"{method.__name__} {args} {kw} {te - ts}s")
        return result
    return timed


def generate_tables_from_features(table_name, cohort_features, cohort_year, columns):
    table = tables[table_name]
    primary_key = table_id(table_name)
        
    table_cohorts = []

    cohort_feature_groups = defaultdict(list)
    for cohort_feature in cohort_features:
        k = cohort_feature["feature_name"]
        v = cohort_feature["feature_qualifier"]
        feature_year = cohort_feature["year"]
        cohort_feature_groups[feature_year].append((k, v))

    for feature_year, features in cohort_feature_groups.items():
            
        table_cohort_feature_group = select([table.c[primary_key]]).select_from(table)
        if feature_year is not None:
            table_cohort_feature_group = table_cohort_feature_group.where(table.c.year == feature_year)

        for k, v in features:
            table_cohort_feature_group = filter_select(table_cohort_feature_group, table, k, v)
                
        table_cohort_feature_group = table_cohort_feature_group.alias()
        table_cohorts.append(table_cohort_feature_group)

    if len(cohort_feature_groups) == 0:
        table_cohort_feature_group = select([table.c[primary_key]]).select_from(table)
        if cohort_year is not None:
            table_cohort_feature_group = table_cohort_feature_group.where(table.c.year == cohort_year)

        table_cohort_feature_group = table_cohort_feature_group.alias()
        table_cohorts.append(table_cohort_feature_group)

    table_matrices = {}
    
    column_groups = defaultdict(list)

    for column_name, year in columns:
        column_groups[year].append(column_name)

    for year, column_names in column_groups.items():
        
        table_matrix = select([table.c[x] for x in chain([primary_key], column_names)]).select_from(table)

        if year is not None:
            table_matrix = table_matrix.where(table.c.year == year)
            
        table_matrix = table_matrix.alias()
        table_matrices[year] = table_matrix

    tables_all = [*table_matrices.values(), *table_cohorts]
    table_cohort = tables_all[0]
    table_filtered = table_cohort
    for table in tables_all[1:]:
        table_filtered = table_filtered.join(table, onclause=table_cohort.c[primary_key] == table.c[primary_key])

    return table_filtered, table_matrices


def selection(conn, table, selections):
    result = []

    for i in range(0, len(selections), MAX_ENTRIES_PER_ROW):
        subs = selections[i:min(i+MAX_ENTRIES_PER_ROW, len(selections))]
        
        s = select(subs).select_from(table)

        result.extend(list(conn.execute(s).first()))

    return result


def normalize_features(year, cohort_features):
    if isinstance(cohort_features, dict):
        cohort_features = [{
            "feature_name": k,
            "feature_qualifier": v
        } for k, v in cohort_features.items()]

    cohort_features = [{
        "feature_name": f["feature_name"],
        "feature_qualifier": {
            "operator": f["feature_qualifier"]["operator"],
            "value": f["feature_qualifier"]["value"]
        },
        "year": f["feature_qualifier"].get("year", year)
    } for f in cohort_features]

    def feature_key(f):
        return f["feature_name"], f["feature_qualifier"]["operator"], json.dumps(f["feature_qualifier"]["value"], sort_keys=True), f["year"]
    
    return sorted(cohort_features, key=feature_key)


def normalize_feature(year, feature):

    feature = {
        "feature_name": feature["feature_name"],
        "feature_qualifiers": feature["feature_qualifiers"],
        "year": feature.get("year", year)
    }

    return feature


@timeit
def select_feature_matrix(conn, table_name, year, cohort_features, cohort_year, feature_a, feature_b):

    cohort_features_norm = normalize_features(cohort_year, cohort_features)
    feature_a_norm = normalize_feature(year, feature_a)
    feature_b_norm = normalize_feature(year, feature_b)
    
    cohort_features_json = json.dumps(cohort_features_norm, sort_keys=True)
    feature_a_json = json.dumps(feature_a_norm, sort_keys=True)
    feature_b_json = json.dumps(feature_b_norm, sort_keys=True)

    cohort_year = cohort_year if len(cohort_features_norm) == 0 else None

    digest = get_digest(json.dumps(table_name), cohort_features_json, json.dumps(cohort_year), feature_a_json, feature_b_json) 
    
    result = conn.execute(select([cache.c.association]).select_from(cache).where(cache.c.digest == digest).where(cache.c.table == table_name).where(cache.c.cohort_features == cohort_features_json).where(cache.c.cohort_year == cohort_year).where(cache.c.feature_a == feature_a_json).where(cache.c.feature_b == feature_b_json)).first()

    timestamp = datetime.now(timezone.utc)
    
    if result is None:
        
        ka = feature_a_norm["feature_name"]
        vas = feature_a_norm["feature_qualifiers"]
        ya = feature_a_norm["year"]
        kb = feature_b_norm["feature_name"]
        vbs = feature_b_norm["feature_qualifiers"]
        yb = feature_b_norm["year"]

        table, table_matrices = generate_tables_from_features(table_name, cohort_features_norm, cohort_year, [(ka, ya), (kb, yb)])
        
        selections = [
            case_select2(table_matrices[yb], table_matrices[ya], kb, vb, ka, va) for vb, va in product(vbs, vas)
        ] + [
            case_select(table_matrices[ya], ka, va) for va in vas
        ] + [
            case_select(table_matrices[yb], kb, vb) for vb in vbs
        ] + [
            func.count()
        ]
        
        result = selection(conn, table, selections)

        nvas = len(vas)
        nvbs = len(vbs)
        mat_size = nvas * nvbs

        feature_matrix = np.reshape(result[:mat_size], (nvbs, nvas)).tolist()

        
        total_cols = result[mat_size : mat_size + nvas]
        total_rows = result[mat_size + nvas : mat_size + nvas + nvbs]

        total = result[mat_size + nvas + nvbs]
        
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

        feature_a_norm_with_biolink_class = {
            **feature_a_norm,
            "biolink_class": inflection.underscore(lookUpFeatureClass(table_name, ka))
        }
        
        feature_b_norm_with_biolink_class = {
            **feature_b_norm,
            "biolink_class": inflection.underscore(lookUpFeatureClass(table_name, kb))
        }
        
        association = {
            "feature_a": feature_a_norm_with_biolink_class,
            "feature_b": feature_b_norm_with_biolink_class,
            "feature_matrix": feature_matrix2,
            "rows": [{"frequency": a, "percentage": b} for (a,b) in zip(total_rows, map(lambda x: div(x, total), total_rows))],
            "columns": [{"frequency": a, "percentage": b} for (a,b) in zip(total_cols, map(lambda x: div(x, total), total_cols))],
            "total": total,
            "p_value": p,
            "chi_squared": chi_squared
        }

        association_json = json.dumps(association, sort_keys=True)

        conn.execute(cache.insert().values(digest=digest, association=association_json, table=table_name, cohort_features=cohort_features_json, feature_a=feature_a_json, feature_b=feature_b_json, access_time=timestamp))

    else:
        association_json = result[0]
        association = json.loads(association_json)
        conn.execute(cache.update().where(cache.c.digest == digest).where(cache.c.table == table_name).where(cache.c.cohort_features == cohort_features_json).where(cache.c.cohort_year == cohort_year).where(cache.c.feature_a == feature_a_json).where(cache.c.feature_b == feature_b_json).values(access_time=timestamp))

    return association


def select_feature_count(conn, table_name, year, cohort_features, cohort_year, feature_a):

    cohort_features_norm = normalize_features(cohort_year, cohort_features)
    feature_a_norm = normalize_feature(year, feature_a)
    
    cohort_features_json = json.dumps(cohort_features_norm, sort_keys=True)
    feature_a_json = json.dumps(feature_a_norm, sort_keys=True)

    cohort_year = cohort_year if len(cohort_features_norm) == 0 else None

    digest = get_digest(json.dumps(table_name), cohort_features_json, json.dumps(cohort_year), feature_a_json)
    
    result = conn.execute(select([cache_count.c.count]).select_from(cache_count).where(cache_count.c.digest == digest).where(cache_count.c.table == table_name).where(cache_count.c.cohort_features == cohort_features_json).where(cache_count.c.cohort_year == cohort_year).where(cache_count.c.feature_a == feature_a_json)).first()

    timestamp = datetime.now(timezone.utc)
    
    if result is None:
        
        ka = feature_a_norm["feature_name"]
        vas = feature_a_norm["feature_qualifiers"]
        ya = feature_a_norm["year"]
        
        table, table_count = generate_tables_from_features(table_name, cohort_features_norm, cohort_year, [(ka, ya)])
        
        selections = [
            func.count(),
            *(case_select(table_count[ya], ka, va) for va in vas)
        ]

        result = selection(conn, table, selections)
        
        total = result[0]
        feature_matrix = result[1:]
        
        feature_percentage = map(lambda x: x/total, feature_matrix)
        
        feature_a_norm_with_biolink_class = {
            **feature_a_norm,
            "biolink_class": inflection.underscore(lookUpFeatureClass(table_name, ka))
        }
    
        count = {
            "feature": feature_a_norm_with_biolink_class,
            "feature_matrix": [{"frequency": a, "percentage": b} for (a, b) in zip(feature_matrix, feature_percentage)]
        }

        count_json = json.dumps(count, sort_keys=True)

        conn.execute(cache_count.insert().values(digest=digest, count=count_json, table=table_name, cohort_features=cohort_features_json, feature_a=feature_a_json, access_time=timestamp))
        
    else:
        count_json = result[0]
        count = json.loads(count_json)
        conn.execute(cache_count.update().where(cache_count.c.digest == digest).where(cache_count.c.table == table_name).where(cache_count.c.cohort_features == cohort_features_json).where(cache_count.c.cohort_year == cohort_year).where(cache_count.c.feature_a == feature_a_json).values(access_time=timestamp))

    return count


def get_feature_levels(conn, table, year, feature):
    s = select([table.c[feature]]).where(table.c.year == year).distinct().order_by(table.c[feature])
    return list(map(lambda row: row[0], conn.execute((s))))


def select_feature_association(conn, table_name, year, cohort_features, cohort_year, feature, maximum_p_value, feature_set):
    table = tables[table_name]
    rs = []
    for k, v, levels, _ in filter(feature_set, features[table_name]):
        if levels is None:
            levels = get_feature_levels(conn, table, year, k)
        ret = select_feature_matrix(conn, table_name, year, cohort_features, cohort_year, feature, {"feature_name": k, "feature_qualifiers": list(map(lambda level: {"operator": "=", "value": level}, levels))})
        if ret["p_value"] < maximum_p_value:
            rs.append(ret)
    return rs


def select_associations_to_all_features(conn, table, year, cohort_id, feature, maximum_p_value, feature_set=lambda x: True):
    cohort_meta = get_features_by_id(conn, table, cohort_id)
    if cohort_meta is None:
        return "Input cohort_id invalid. Please try again."
    else:
        cohort_features, cohort_year = cohort_meta
        return select_feature_association(conn, table, year, cohort_features, cohort_year, feature, maximum_p_value, feature_set)


def validate_range(table_name, feature):
    feature_name = feature["feature_name"]
    values = feature["feature_qualifiers"]
    _, ty, levels, _ = next(filter(lambda x: x[0] == feature_name, features[table_name]))
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
    n = conn.execute((s)).scalar()
    if n == 0:
        raise RuntimeError("Input name invalid. Please try again.")
    else:
        s = select([name_table.c.cohort_id]).select_from(name_table).where((name_table.c.name == name) & (name_table.c.table == table))
        cohort_id = conn.execute((s)).scalar()

        return {
            "cohort_id": cohort_id,
            "name" : name
        }


def add_name_by_id(conn, table, name, cohort_id):
    s = select([func.count()]).select_from(name_table).where((name_table.c.name == name) & (name_table.c.table == table))
    n = conn.execute((s)).scalar()
    if n == 1:
        raise RuntimeError("Name is already taken. Please choose another name.")
    else:
        i = name_table.insert().values(name=name, table=table, cohort_id=cohort_id)
        conn.execute((i))

        return {
            "cohort_id": cohort_id,
            "name" : name
        }

