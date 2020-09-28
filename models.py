"""Data models."""
from typing import Union, Literal, List, Optional, Dict
from pydantic import BaseModel

_Any = Union[str, int, float]
Number = Union[int, float]

InequalityComparator = Literal[
    "<",
    ">",
    "<=",
    ">=",
]

EqualityComparator = Literal[
    "=",
    "<>",
]


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
        "fdr_tsbh",
        "fdr_tsbky"
    ]
    alpha: float


class EqualityComparison(BaseModel):
    operator: EqualityComparator
    value: _Any


class InequalityComparison(BaseModel):
    operator: InequalityComparator
    value: Number


Comparison = Union[EqualityComparison, InequalityComparison]


class Between(BaseModel):
    operator: Literal["between"]
    value_a: Number
    value_b: Number


class In(BaseModel):
    operator: Literal["in"]
    values: List[_Any]


Qualifier = Union[Comparison, Between, In]


FeaturesImplicit = Dict[str, Comparison]


class FeatureExplicit(BaseModel):
    feature_name: str
    feature_qualifier: Comparison
    year: Optional[int]


Feature = Union[FeaturesImplicit, FeatureExplicit]
Features = Union[FeaturesImplicit, List[FeatureExplicit]]


class FeatureExplicit2(BaseModel):
    feature_name: str
    feature_qualifier: List[Qualifier]
    year: Optional[int]


FeaturesImplicit2 = Dict[str, List[Comparison]]


Feature2 = Union[FeaturesImplicit2, FeatureExplicit2]
Features2 = Union[FeaturesImplicit, List[FeatureExplicit2]]


class FeatureAssociation(BaseModel):
    feature_a: Feature
    feature_b: Feature
    check_coverage_is_full: bool = False


class FeatureAssociation2(BaseModel):
    feature_a: Feature2
    feature_b: Feature2
    check_coverage_is_full: bool = False


class AllFeaturesAssociation(BaseModel):
    feature: Feature
    maximum_p_value: float
    correction: Optional[Union[Correction, CorrectionWithAlpha]]
    check_coverage_is_full: bool = False


class AllFeaturesAssociation2(BaseModel):
    feature: Feature2
    maximum_p_value: float
    correction: Optional[Union[Correction, CorrectionWithAlpha]]
    check_coverage_is_full: bool = False


class AddNameById(BaseModel):
    cohort_id: str
