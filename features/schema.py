import os
from enum import Enum
from typing import Union, Any, Literal, List, Optional
import yaml
from functools import reduce
from pydantic import BaseModel, create_model
from .features import features


def jsonschema_type(ty, levels):
    if levels is not None:
        yamltype = Literal.__getitem__(tuple(levels))
    elif ty is str:
        yamltype = str
    elif ty is int:
        yamltype = int
    elif ty is float:
        yamltype = float
    else:
        yamltype = Any

    return yamltype


def qualifier_schema(name, ty, levels):
    return create_model(
        f"{name}_qualifier_model",
        operator=(Literal["<", ">", "<=", ">=", "=", "<>"], ...),
        value=(jsonschema_type(ty, levels), ...)
    )


def feature_qualifier_schema_explicit(table_name, f, ty, levels):
    name = f"{table_name}_{f}_model_explicit"
    return create_model(
        f"{name}_model",
        feature_name = (Literal[f], ...),
        feature_qualifier = (qualifier_schema(name, ty, levels), ...),
        year = (Optional[int], None)
    )


def feature_schema_implicit(table_name):
    fields = {f.name: (qualifier_schema(f.name, f._type, f.options), ...) for f in features[table_name]}
    return create_model(
        f"{table_name}_model_implicit",
        **fields
    )

    
def union(schemas):
    # can't specify empty list
    return reduce(lambda x,y: Union[x, y], map(lambda x: Union[x], schemas))

    
def feature_schema_explicit(table_name):
    return union([feature_qualifier_schema_explicit(table_name, f.name, f._type, f.options) for f in features[table_name]])


def feature_schema(table_name):
    return Union[
        feature_schema_implicit(table_name),
        feature_schema_explicit(table_name)
    ]


def features_schema(table_name):
    return Union[
        feature_schema_implicit(table_name),
        List[feature_schema_explicit(table_name)]
    ]


class name_schema_output(BaseModel):
    name: str
    cohort_id: str


class add_name_by_id_schema:
    cohort_id: str


def bin_qualifier_schema(name, ty, levels):

    yamltype = jsonschema_type(ty, levels)
    
    return Union[
        create_model(
            f"{name}_bin_qualifier_model_comparision_operator",
            operator=(Literal["<", ">", "<=", ">=", "=", "<>"], ...),
            value=(yamltype, ...)
        ),
        create_model(
            f"{name}_bin_qualifier_model_between",
            operator=(Literal["between"], ...),
            value_a=(yamltype, ...),
            value_b=(yamltype, ...)
        ),
        create_model(
            f"{name}_bin_qualifier_model_in",
            operator=(Literal["in"], ...),
            values=(List[yamltype], ...)
        )
    ]


def bin_feature_qualifier_schema_explicit(table_name, f, ty, levels):
    name = f"{table_name}_{f}"
    
    return create_model(
        f"{name}_bin_model",
        feature_name = (Literal[f], ...),
        feature_qualifiers = (List[bin_qualifier_schema(name, ty, levels)], ...),
        year = (Optional[int], None)
    )


def feature2_schema_explicit(table_name):
    return union([bin_feature_qualifier_schema_explicit(f.name, f._type, f.options) for f in features[table_name]])


def feature2_schema_implicit(table_name):
    fields = {f.name: (List[bin_qualifier_schema(f._type, f.options)], ...) for f in features[table_name]}
    return create_model(
        f"{table_name}_bin_model",
        **fields
    )


def feature2_schema(table_name):
    return Union[
        feature2_schema_implicit(table_name),
        feature2_schema_explicit(table_name)
    ]


def features2_schema(table_name):
    return Union[
        feature2_schema_implicit(table_name),
        List[feature2_schema_explicit(table_name)]
    ]


def feature_association_schema(table_name):
    return feature_association_schema_common("feature_association", table_name, feature_schema(table_name))


def feature_association2_schema(table_name):
    return feature_association_schema_common("feature_association2", table_name, feature2_schema(table_name))

    
def feature_association_schema_common(model_name, table_name, feature_schema):
    return create_model(
        f"{model_name}_{table_name}_model",
        feature_a = (feature_schema, ...),
        feature_b = (feature_schema, ...),
        check_coverage_is_full = (bool, False),
    )


def associations_to_all_features_schema(table_name):
    return associations_to_all_features_schema_common("associations_to_all_features", table_name, feature_schema(table_name))


def associations_to_all_features2_schema(table_name):
    return associations_to_all_features_schema_common("associations_to_all_features2", table_name, feature2_schema(table_name))


class Correction(BaseModel):
    method: Literal[
        "bonferroni",
        "sidak",
        "holm-sidak",
        "holm",
        "simes-hochberg",
        "hommel",
        "fdr_bh",
        "fdr_by" 
    ]

    
class CorrectionWithAlpha(BaseModel):
    method: Literal[
        "fdr_tsbh" ,
        "fdr_tsbky"
    ]
    alpha: float
    

def associations_to_all_features_schema_common(model_name, table_name, feature_schema):
    return create_model(
        f"{model_name}_{table_name}_model",
        feature = (feature_schema, ...),
        maximum_p_value = (int, ...),
        correction = (Optional[Union[Correction, CorrectionWithAlpha]], None),
        check_coverage_is_full = (bool, False)
    )


def features_schema_output(table_name):
    return {
    }


def cohort_dictionary_schema_output(table_name):
    return {
    }


def cohort_schema_output(table_name):
    return {
    }


def feature_association_schema_output(table_name):
    return {
    }


def feature_association2_schema_output(table_name):
    return {
    }


def associations_to_all_features_schema_output(table_name):
    return {
    }


def identifiers_output():
    return {
    }


