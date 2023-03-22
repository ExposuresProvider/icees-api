"""SQL access functions."""
from collections import defaultdict
from functools import wraps
from hashlib import md5
from itertools import product, chain
import json
import logging
import re
import operator
import os
import time
from decimal import Decimal
from typing import Any, Callable, Dict, List, Union
from fastapi import HTTPException
import numpy as np
import redis
from scipy.stats import chi2_contingency, fisher_exact, contingency
from sqlalchemy import and_, between, case, column, table, Float
from sqlalchemy.sql.expression import cast

from sqlalchemy.sql import select, func, distinct
from statsmodels.stats.multitest import multipletests
from tx.functional.maybe import Nothing, Just

from .mappings import get_value_sets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

eps = np.finfo(float).eps


def get_digest(*args):
    """Get digest."""
    c = md5()
    for arg in args:
        c.update(arg.encode("utf-8"))
    return c.digest()


def op_dict(k, v, table_):
    try:
        value = table_.c[k]
    except KeyError:
        raise HTTPException(status_code=400, detail=f"No feature named '{k}'")
    # python_type = value.type.python_type
    # if v["operator"] == "in":
    #     values = v["values"]
    # elif v["operator"] == "between":
    #     values = [v["value_a"], v["value_b"]]
    # else:
    #     values = [v["value"]]
    # options = features_dict[table_name][k].get("enum", None)
    # for value in values:
    #     if not isinstance(value, python_type):
    #         raise HTTPException(
    #             status_code=400,
    #             detail="'{feature}' should be of type {type}, but {value} is not".format(
    #                 value=value,
    #                 feature=k,
    #                 type=python_type,
    #             )
    #         )
    #     if options is not None and value not in options:
    #         raise HTTPException(
    #             status_code=400,
    #             detail="{value} is not in {options}".format(
    #                 value=value,
    #                 options=options
    #             )
    #         )

    operations = {
        ">": lambda: value > v["value"],
        "<": lambda: value < v["value"],
        ">=": lambda: value >= v["value"],
        "<=": lambda: value <= v["value"],
        "=": lambda: value == v["value"],
        "<>": lambda: value != v["value"],
        "between": lambda: between(value, v["value_a"], v["value_b"]),
        "in": lambda: value.in_(v["values"])
    }
    return operations[v["operator"]]()


def filter_select(s, k, v, table_):
    """Add WHERE clause to selection."""
    return s.where(
        op_dict(
            k, v, table_,
        )
    )


def case_select(table, k, v, table_name=None):
    return func.coalesce(func.sum(case([(
        op_dict(
            k, v, table,
        ), 1
    )], else_=0)), 0)


def case_select2(table, table2, k, v, k2, v2, table_name=None):
    return func.coalesce(func.sum(case([(and_(
        op_dict(
            k, v, table,
        ),
        op_dict(
            k2, v2, table2,
        )
    ), 1)], else_=0)), 0)


def select_cohort(conn, table_name, year, cohort_features, cohort_id=None):
    """Select cohort."""
    cohort_features_norm = normalize_features(year, cohort_features)

    gen_table, _, pk = generate_tables_from_features(table_name, cohort_features_norm, year, [])
    s = select([func.count(column(pk).distinct())]).select_from(gen_table)
    n = conn.execute(s).scalar()
    if n <= 10:
        return None, -1
    else:
        size = n
        next_val = 0
        while cohort_id is None:
            next_val += 1
            cohort_id = "COHORT:" + str(next_val)
            if cohort_id_in_use(conn, cohort_id):
                cohort_id = None

        if cohort_id_in_use(conn, cohort_id):
            raise HTTPException(status_code=400, detail="Cohort id is in use.")

        query = "INSERT INTO cohort (cohort_id, size, features, \"table\", year)"
        if os.environ.get("ICEES_DB", "sqlite") == "sqlite":
            query += " VALUES (?, ?, ?, ?, ?)"
        else:
            query += " VALUES (%s, %s, %s, %s, %s)"
        conn.execute(query, (
            cohort_id,
            size,
            json.dumps(cohort_features, sort_keys=True),
            table_name,
            year,
        ))
        return cohort_id, size


def get_ids_by_feature(conn, table_name, year, cohort_features):
    """Get ids by feature."""
    s = select([column("cohort_id"), column("size")])\
        .select_from(table("cohort"))\
        .where(column("table") == table_name)\
        .where(column("year") == year)\
        .where(column("features") == json.dumps(cohort_features, sort_keys=True))
    rs = list(conn.execute(s))
    if len(rs) == 0:
        cohort_id, size = select_cohort(conn, table_name, year, cohort_features)
    else:
        [cohort_id, size] = rs[0]
    return cohort_id, size


def get_features(conn, table_name: str) -> List[str]:
    """Get features by id."""
    table = conn.tables[table_name]
    return [
        col for col in table.columns.keys()
        if col.lower() not in [table_name.lower() + "id", "year"]
    ]


def get_features_by_id(conn, table_name, cohort_id):
    """Get features by id."""
    s = select([column("features"), column("year")])\
        .select_from(table("cohort"))\
        .where(column("cohort_id") == cohort_id)\
        .where(column("table") == table_name)
    rs = list(conn.execute((s)))
    if len(rs) == 0:
        return None
    return json.loads(rs[0][0]), rs[0][1]


def get_cohort_by_id(conn, table_name, year, cohort_id):
    """Get cohort by id."""
    s = select([column("features"), column("size")])\
        .select_from(table("cohort"))\
        .where(column("cohort_id") == cohort_id)\
        .where(column("table") == table_name)\
        .where(column("year") == year)
    rs = list(conn.execute((s)))
    if len(rs) == 0:
        return None
    return {
        "size": rs[0][1],
        "features": json.loads(rs[0][0])
    }


def get_cohort_features(conn, table_name, year, cohort_features, cohort_year):
    """Get cohort features."""
    rs = []
    for k in get_features(conn, table_name):
        # k = f.name
        # levels = f.options
        # if levels is None:
        levels = get_feature_levels(k)
        ret = select_feature_count_all_values(
            conn,
            table_name,
            year,
            cohort_features,
            cohort_year,
            k,
            levels
        )
        rs.append(ret)
    return rs


def get_cohort_dictionary(conn, table_name, year):
    """Get cohort dictionary."""
    s = select([column("cohort_id"), column("features"), column("size")])\
        .select_from(table("cohort"))\
        .where(column("table") == table_name)
    if year is not None:
        s = s.where(column("year") == year)
    rs = []
    for cohort_id, features, size in conn.execute((s)):
        rs.append({
            "cohort_id": cohort_id,
            "size": size,
            "features": json.loads(features)
        })
    return rs


def get_cohort_definition_by_id(conn, cohort_id):
    s = select([
        column("cohort_id"),
        column("features"),
        column("size"),
        column("table"),
        column("year"),
    ]).select_from(table("cohort"))\
        .where(column("cohort_id") == cohort_id)
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
    """Determine whether cohort is in use."""
    return conn.execute((
        select([func.count()])
        .select_from(table("cohort"))\
        .where(column("cohort_id") == cohort_id)
    )).scalar() > 0


def join_lists(lists):
    """Joins lists."""
    return [x for l in lists for x in l]


def div(a,b):
    if b != 0:
        return a / b
    else:
        return float("NaN")


def add_eps(a):
    """Add epsilon."""
    return a + eps


MAX_ENTRIES_PER_ROW = int(os.environ.get("MAX_ENTRIES_PER_ROW", "1664"))

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logger.debug(f"{method.__name__} {args} {kw} {te - ts}s")
        return result
    return timed


def generate_tables_from_features(
        table_name,
        cohort_features,
        cohort_year,
        columns,
):
    """Generate tables from features."""
    primary_key = table_name[0].upper() + table_name[1:] + "Id"
    table_ = table(table_name, column(primary_key))

    table_cohorts = []

    cohort_feature_groups = defaultdict(list)
    for cohort_feature in cohort_features:
        k = cohort_feature["feature_name"]
        v = cohort_feature["feature_qualifier"]
        feature_year = cohort_feature["year"]
        cohort_feature_groups[feature_year].append((k, v))

    for feature_year, features in cohort_feature_groups.items():

        table_cohort_feature_group = (
            select([table_.c[primary_key]])\
            .select_from(table_)
        )  # SELECT "PatientId" FROM patient
        if feature_year is not None:
            table_cohort_feature_group = (
                table_cohort_feature_group\
                .where(column("year") == feature_year)
            )  # SELECT "PatientId" FROM patient WHERE year = 2010

        for k, v in features:
            table_.append_column(column(k))
            table_cohort_feature_group = filter_select(
                table_cohort_feature_group,
                k,
                v,
                table_,
            )  # SELECT "PatientId" FROM patient WHERE patient."AgeStudyStart" = '0-2'

        table_cohort_feature_group = table_cohort_feature_group.alias()
        table_cohorts.append(table_cohort_feature_group)

    if len(cohort_feature_groups) == 0:
        table_cohort_feature_group = (
            select([table_.c[primary_key]])\
            .select_from(table_)
        )  # SELECT "PatientId" FROM patient
        if cohort_year is not None:
            table_cohort_feature_group = (
                table_cohort_feature_group\
                .where(column("year") == cohort_year)
            )  # SELECT "PatientId" FROM patient WHERE year = 2010

        table_cohort_feature_group = table_cohort_feature_group.alias()
        table_cohorts.append(table_cohort_feature_group)

    table_matrices = {}

    column_groups = defaultdict(list)

    for column_name, year in columns:
        column_groups[year].append(column_name)

    table_ = table(table_name)
    for year, column_names in column_groups.items():
        for column_name in chain([primary_key], column_names):
            table_.append_column(column(column_name))

    for year, column_names in column_groups.items():

        table_matrix = select([
            table_.c[x]
            for x in chain([primary_key], column_names)
        ]).select_from(table_)

        if year is not None:
            table_matrix = table_matrix.where(column("year") == year)

        table_matrix = table_matrix.alias()
        table_matrices[year] = table_matrix

    tables_all = [*table_matrices.values(), *table_cohorts]
    table_cohort = tables_all[0]
    table_filtered = table_cohort
    for _table in tables_all[1:]:
        table_filtered = table_filtered.join(
            _table,
            onclause=table_cohort.c[primary_key] == _table.c[primary_key],
        )

    return table_filtered, table_matrices, primary_key


def selection(conn, table, selections):
    result = []

    for i in range(0, len(selections), MAX_ENTRIES_PER_ROW):
        subs = selections[i:min(i+MAX_ENTRIES_PER_ROW, len(selections))]

        s = select(subs).select_from(table)

        start_time = time.time()
        query = str(s)
        # print(s.compile().params)
        for key, value in s.compile().params.items():
            query = re.sub(rf":{key}\b", repr(value), query)
        result.extend(list(conn.execute(s).first()))
        print(f"{time.time() - start_time} seconds spent executing a subset of selections")
        # print(result)

    return result


def feature_key(f):
    """Generate feature key."""
    return json.dumps(f, sort_keys=True)


def normalize_features(year, cohort_features):
    """Normalize features."""
    if isinstance(cohort_features, dict):
        cohort_features = [{
            "feature_name": k,
            "feature_qualifier": v
        } for k, v in cohort_features.items()]

    cohort_features = [normalize_feature(year, f) for f in cohort_features]

    return sorted(cohort_features, key=feature_key)


def normalize_feature(year, feature):
    """Normalize feature."""

    feature = {**feature, "year": feature.get("year", year)}

    return feature


OP_MAP = {
    "=": operator.eq,
    "<>": operator.ne,
    "<": operator.lt,
    ">": operator.gt,
    "<=": operator.le,
    ">=": operator.ge,
    "in": lambda x, y: x in y,
}


def simplify_value(val_str):
    """
    simplify value string to integer string if appropriate, e.g., from 1.0 to 1
    """
    try:
        value_float = float(val_str)
    except (ValueError, TypeError):
        return val_str
    value_int = int(value_float)
    if value_int == value_float:
        return str(value_int)
    return val_str


def get_count(results, **constraints):
    """Get sum of result counts that meet constraints."""
    count = 0

    for result in results:
        if all(
            OP_MAP[constraint["operator"]](
                simplify_value(result.get(feature, None)),
                simplify_value(constraint.get("value", constraint.get("values"))),
            )
            for feature, constraint in constraints.items()
        ):
            count += result["count"]
    return count


REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
r = redis.Redis(host=REDIS_HOST)


def cached(key=lambda *args: hash(tuple(args))):
    """Generate a decorator to cache results."""
    def decorator(func):
        """Decorate a function to cache results."""
        @wraps(func)
        def wrapper(*args):
            key_ = key(*args)
            cached_result = r.get(key_)
            if cached_result is not None:
                return json.loads(cached_result)
            result = func(*args)
            r.set(key_, json.dumps(result))
            return result
        return wrapper
    return decorator


@cached(key=lambda db, *args: json.dumps(args))
def count_unique(conn, table_name, year, *columns):
    """Count each unique combination of column values.

    For example, for columns = ["a", "b"] and data
    a  b  c  d
    ----------
    1  1  1  0
    1  1  2  0
    1  1  3  0
    1  2  4  0
    1  2  5  0
    2  2  6  0
    result = [
        # a, b, count
        [1, 1, 3],
        [1, 2, 2],
        [2, 2, 1]
    ]
    """
    if not year:
        return [list(row) for row in conn.execute(
            "SELECT {cols}, count(*) FROM {table_name} WHERE {cols_not_null} GROUP BY {cols}".format(
                cols=", ".join(f"\"{col}\"" for col in columns),
                cols_not_null=" AND ".join(f"\"{col}\" is not null" for col in columns),
                table_name=table_name,
            )
        ).fetchall()]
    else:
        return [list(row) for row in conn.execute(
            "SELECT {cols}, count(*) FROM {table_name} WHERE \"year\" = {year} AND {cols_not_null} GROUP BY {cols}".format(
                cols=", ".join(f"\"{col}\"" for col in columns),
                table_name=table_name,
                year=year,
                cols_not_null=" AND ".join(f"\"{col}\" is not null" for col in columns),
            )
        ).fetchall()]


def select_feature_matrix(
        conn,
        table_name,
        year,
        cohort_features,
        cohort_year,
        feature_a,
        feature_b,
):
    """Select feature matrix."""
    if not isinstance(cohort_features, list):
        cohort_features = [
            {
                "feature_name": key,
                "feature_qualifier": {
                    "operator": value["operator"],
                    "value": value["value"],
                },
            }
            for key, value in cohort_features.items()
        ]

    if cohort_features:
        condition_str = " AND ".join(
                "\"{}\" {} {}".format(
                    feature["feature_name"],
                    feature["feature_qualifier"]["operator"],
                    feature["feature_qualifier"]["value"],
                )
                for feature in cohort_features)

        view_query = (
            "CREATE VIEW tmp AS "
            "SELECT * FROM {} "
            "WHERE {}"
        ).format(
            table_name,
            condition_str
        )
        conn.execute(view_query)
        table_name = "tmp"

    # start_time = time.time()
    # cohort_features_norm = normalize_features(cohort_year, cohort_features)
    feature_a_norm = normalize_feature(year, feature_a)
    feature_b_norm = normalize_feature(year, feature_b)

    # print(f"{time.time() - start_time} seconds spent normalizing")

    # start_time = time.time()
    # cohort_features_json = json.dumps(cohort_features_norm, sort_keys=True)
    # feature_a_json = json.dumps(feature_a_norm, sort_keys=True)
    # feature_b_json = json.dumps(feature_b_norm, sort_keys=True)
    # print(f"{time.time() - start_time} seconds spent json.dumping")

    # cohort_year = cohort_year if len(cohort_features_norm) == 0 else None

    # start_time = time.time()
    # digest = get_digest(
    #     json.dumps(table_name),
    #     cohort_features_json,
    #     json.dumps(cohort_year),
    #     feature_a_json,
    #     feature_b_json,
    # )
    # print(f"{time.time() - start_time} seconds spent getting digest")

    # start_time = time.time()
    # result = conn.execute(
    #     select([cache.c.association])\
    #     .select_from(cache)\
    #     .where(cache.c.digest == digest)\
    #     .where(cache.c.table == table_name)\
    #     .where(cache.c.cohort_features == cohort_features_json)\
    #     .where(cache.c.cohort_year == cohort_year)\
    #     .where(cache.c.feature_a == feature_a_json)\
    #     .where(cache.c.feature_b == feature_b_json)
    # ).first()
    # print(f"{time.time() - start_time} seconds spent checking cache")
    # if result is None:
    #     print("cache miss...")
    # else:
    #     print("cache hit!")

    # timestamp = datetime.now(timezone.utc)
    ka = feature_a_norm["feature_name"]
    vas = feature_a_norm["feature_qualifiers"]
    kb = feature_b_norm["feature_name"]
    vbs = feature_b_norm["feature_qualifiers"]
    # start_time = time.time()
    result = count_unique(conn, table_name, year, ka, kb)

    _ka = "0_" + ka
    _kb = "1_" + kb
    result = [
        {
            _ka: el[0],
            _kb: el[1],
            "count": el[2],
        }
        for el in result
    ]
    #print(f"{time.time() - start_time} seconds spent doing it the fast way")

    feature_matrix = [
        [
            get_count(result, **{
                _ka: va,
                _kb: vb,
            })
            for va in vas
        ]
        for vb in vbs
    ]
    total_cols = [
        get_count(result, **{_ka: va}) for va in vas
    ]
    total_rows = [
        get_count(result, **{_kb: vb}) for vb in vbs
    ]
    total = get_count(result)
    observed = list(map(
        lambda x: list(map(add_eps, x)),
        feature_matrix
    ))

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
        **feature_a_norm
    }

    feature_b_norm_with_biolink_class = {
        **feature_b_norm
    }
    if observed:
        chi_squared, chi_squared_p, chi_squared_dof, _ = chi2_contingency(observed, correction=False)
        feature_matrix = np.array(feature_matrix)
        if feature_matrix.shape == (2, 2) and not np.any(feature_matrix == 0):
            fisher_exact_odds_ratio, fisher_exact_p = fisher_exact(feature_matrix, alternative='two-sided')
            or_result = contingency.odds_ratio(feature_matrix, kind='sample')
            log_odds_ratio = np.log(or_result.statistic)
            log_odds_ratio_conf_interval_95 = or_result.confidence_interval(confidence_level=0.95)
        else:
            fisher_exact_odds_ratio = fisher_exact_p = log_odds_ratio = log_odds_ratio_conf_interval_95 = None

        association = {
            "feature_a": feature_a_norm_with_biolink_class,
            "feature_b": feature_b_norm_with_biolink_class,
            "feature_matrix": feature_matrix2 if result else [],
            "rows": [
                {"frequency": a, "percentage": b}
                for (a,b) in zip(total_rows, map(lambda x: div(x, total), total_rows))
            ],
            "columns": [
                {"frequency": a, "percentage": b}
                for (a,b) in zip(total_cols, map(lambda x: div(x, total), total_cols))
            ],
            "total": total,
            "chi_squared_statistic": chi_squared,
            "chi_squared_dof": chi_squared_dof,
            "chi_squared_p": chi_squared_p,
            "fisher_exact_odds_ratio": fisher_exact_odds_ratio,
            "fisher_exact_p": fisher_exact_p,
            "log_odds_ratio": log_odds_ratio,
            "log_odds_ratio_95_confidence_interval": log_odds_ratio_conf_interval_95,
        }
    else:
        association = {
            "feature_a": feature_a_norm_with_biolink_class,
            "feature_b": feature_b_norm_with_biolink_class,
            "feature_matrix": feature_matrix2 if result else [],
            "rows": [
                {"frequency": a, "percentage": b}
                for (a,b) in zip(total_rows, map(lambda x: div(x, total), total_rows))
            ],
            "columns": [
                {"frequency": a, "percentage": b}
                for (a,b) in zip(total_cols, map(lambda x: div(x, total), total_cols))
            ],
            "total": total,
            "chi_squared_statistic": None,
            "chi_squared_dof": None,
            "chi_squared_p": None,
            "fisher_exact_odds_ratio": None,
            "fisher_exact_p": None,
            "log_odds_ratio": None,
            "log_odds_ratio_95_confidence_interval": None
        }

    # association_json = json.dumps(association, sort_keys=True)

    if cohort_features:
        conn.execute("DROP VIEW tmp")

    # start_time = time.time()
    # conn.execute(cache.insert().values(digest=digest, association=association_json, table=table_name, cohort_features=cohort_features_json, feature_a=feature_a_json, feature_b=feature_b_json, access_time=timestamp))
    # print(f"{time.time() - start_time} seconds spent writing to cache")
    return association


def select_feature_count_all_values(
        conn,
        table_name,
        year,
        cohort_features,
        cohort_year,
        feature_name,
        levels
):
    """Select feature count."""
    cohort_features_norm = normalize_features(cohort_year, cohort_features)
    cohort_year = cohort_year if len(cohort_features_norm) == 0 else None
    gen_table, _, _ = generate_tables_from_features(
        table_name,
        cohort_features_norm,
        cohort_year,
        [(feature_name, year)],
    )
    sqlcolumn = column(feature_name)
    result = conn.execute(select([sqlcolumn, func.count()]).select_from(gen_table).group_by(sqlcolumn)).fetchall()
    values = defaultdict(int)
    for value, count in result:
        values[value] = count
    total = sum(values.values())
    levels = list(levels)
    for value in values.keys():
        if value not in levels:
            levels.append(value)

    feature_a_norm_with_biolink_class = {
        "feature_name": feature_name,
        "feature_qualifiers": [{
            "operator": "=",
            "value": level
        } for level in levels],
        "year": year
    }
    count = {
        "feature": feature_a_norm_with_biolink_class,
        "feature_matrix": [
            {"frequency": a, "percentage": div(a, total)}
            for level in levels if (a := values[level]) is not None
        ]
    }
    return count


def get_feature_levels(feature):
    """Get feature levels."""
    return get_value_sets().get(feature, [])


def apply_correction(ret, correction=None):
    """Apply p-value correction."""
    if correction is not None:
        method = correction["method"]
        alpha = correction.get("alpha", 1)
        if ret["chi_squared_p"] is None:
            ret["chi_squared_p_corrected"] = None
            return ret
        rsp = [ret["chi_squared_p"]]
        _, pvals, _, _ = multipletests(rsp, alpha, method)
        ret["chi_squared_p_corrected"] = pvals[0]
    return ret


class PValueError(Exception):
    """p-value is too high."""


def select_feature_association(
        conn,
        table_name,
        year,
        cohort_features,
        cohort_year,
        feature_a,
        maximum_p_value,
        feature_b,
        correction,
):
    """Select feature association."""
    ret = apply_correction(
        select_feature_matrix(
            conn, table_name, year,
            cohort_features, cohort_year, feature_a, feature_b,
        ),
        correction,
    )

    if (pval := ret.get("chi_squared_p_corrected", ret.get("chi_squared_p", None))) is None or pval > maximum_p_value:
        raise PValueError(f"chi_squared_p {pval} > {maximum_p_value}")
    return ret


def select_associations_to_all_features(
        conn,
        table,
        year,
        cohort_id,
        feature_filter_a: Union[Callable[[str], bool], Dict[str, Any]],
        maximum_p_value,
        feature_filter_b: Callable[[str], bool] = lambda x: True,
        correction=None,
):
    cohort_meta = get_features_by_id(conn, table, cohort_id)
    if cohort_meta is None:
        raise ValueError("Input cohort_id invalid. Please try again.")

    cohort_features, cohort_year = cohort_meta
    if isinstance(feature_filter_a, Callable):
        feature_as = [
            {
                "feature_name": feature_name,
                "feature_qualifiers": list(map(
                    lambda level: {"operator": "=", "value": level},
                    get_feature_levels(feature_name),
                ))
            }
            for feature_name in filter(feature_filter_a, get_features(conn, table))
        ]
    else:
        feature_as = [feature_filter_a]

    feature_bs = [
        {
            "feature_name": feature_name,
            "feature_qualifiers": list(map(
                lambda level: {"operator": "=", "value": level},
                get_feature_levels(feature_name),
            ))
        }
        for feature_name in filter(feature_filter_b, get_features(conn, table))
    ]

    associations = []
    done = set()
    for feature_a, feature_b in product(feature_as, feature_bs):
        hashable = tuple(sorted((feature_a["feature_name"], feature_b["feature_name"])))
        if hashable in done:
            continue
        done.add(hashable)
        try:
            associations.append(select_feature_association(
                conn,
                table,
                year,
                cohort_features,
                cohort_year,
                feature_a,
                maximum_p_value,
                feature_b,
                correction,
            ))
        except PValueError:
            continue

    return associations


def validate_range(conn, table_name, feature):
    feature_name = feature["feature_name"]
    values = feature["feature_qualifiers"]
    # x = next(filter(lambda x: x.name == feature_name, features[table_name]))
    # levels = x.options
    year = None
    levels = get_feature_levels(feature_name)
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
                    raise RuntimeError(f"over lapping value {levels[i]}, input feature qualifiers {feature}")
                else:
                    coverMapUpdate.append(c or u)
            coverMap = coverMapUpdate
        for i, c in enumerate(coverMap):
            if not c:
                raise RuntimeError(f"incomplete value coverage {levels[i]}, input feature qualifiers {feature}")
    else:
        print(f"warning: cannot validate feature {feature_name} in table {table_name} because its levels are not provided")


def validate_feature_value_in_table_column_for_equal_operator(conn, table_name, feature):
    feature_name = feature["feature_name"]
    qualifiers = feature["feature_qualifiers"]
    # qualifiers could be a dict or a list of dicts
    if isinstance(qualifiers, dict):
        qualifiers = [qualifiers]
    for q in qualifiers:
        if q['operator'] == '=':
            val = str(q["value"])
            err_msg = f"Invalid input value {val} for feature {feature_name}. Please try again."
            if val.replace('.', '', 1).isdigit():
                # check whether the query value is actually in the column if comparing numerically, e.g.,
                # query value is 1 while the column value in the database is 1.0
                try:
                    # note that non-number strings are cast as 0
                    num_s = select([column(feature_name)]).select_from(table(table_name)).where(
                        cast(column(feature_name), Float) == Decimal(val)).distinct()
                    rs = list(conn.execute(num_s))
                    if not rs:
                        raise RuntimeError(err_msg)
                    # check whether all results in rs are not digits, and if yes, raise error
                    for result in rs:
                        if str(result[0]).replace('.', '', 1).isdigit():
                            return
                    # all results in rs are not digits
                    raise RuntimeError(err_msg)
                except Exception:
                    raise RuntimeError(err_msg)
            else:
                # val string is not number, just do string comparison in database query
                s = select([column(feature_name)]).select_from(table(table_name)).where(column(feature_name) == val)
                rs = list(conn.execute(s))
                if not rs:
                    raise RuntimeError(err_msg)
    return


def get_id_by_name(conn, table, name):
    """Get cohort id by name."""
    s = select([func.count()])\
        .select_from(table("name"))\
        .where((column("name") == name) & (column("table") == table))
    n = conn.execute((s)).scalar()
    if n == 0:
        raise RuntimeError("Input name invalid. Please try again.")
    else:
        s = select([column("cohort_id")])\
            .select_from(table("name"))\
            .where((column("name") == name) & (column("table") == table))
        cohort_id = conn.execute((s)).scalar()

        return {
            "cohort_id": cohort_id,
            "name" : name
        }


def add_name_by_id(conn, table, name, cohort_id):
    """Add cohort name by id."""
    s = select([func.count()])\
        .select_from(table("name"))\
        .where((column("name") == name) & (column("table") == table))
    n = conn.execute(s).scalar()
    if n == 1:
        raise RuntimeError("Name is already taken. Please choose another name.")
    conn.execute(
        "INSERT INTO name (name, \"table\", cohort_id) VALUES (?, ?, ?)",
        (name, table, cohort_id),
    )

    return {
        "cohort_id": cohort_id,
        "name" : name
    }
