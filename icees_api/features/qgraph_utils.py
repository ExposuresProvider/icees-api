"""Query graph utilities."""
import re

from bmt import Toolkit

BMT = Toolkit()


def get_subcategories(category):
    """Get sub-categories, according to the Biolink model."""
    return BMT.get_descendants(category, formatted=True, reflexive=True)


def camelcase_to_snakecase(string):
    """Convert CamelCase to snake_case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def get_subpredicates(predicate):
    """Get sub-predicates, according to the Biolink model."""
    curies = BMT.get_descendants(predicate, formatted=True, reflexive=True)
    return [
        "biolink:" + camelcase_to_snakecase(curie[8:])
        for curie in curies
    ]


def normalize_qgraph(qgraph):
    """Normalize query graph."""
    for node in qgraph["nodes"].values():
        node["categories"] = [
            descendant
            for category in node.get("categories", None) or ["biolink:NamedThing"]
            for descendant in get_subcategories(category)
        ]
        node.pop("is_set", None)
    for edge in qgraph["edges"].values():
        edge["predicates"] = [
            descendant
            for predicate in node.get("predicates", None) or ["biolink:related_to"]
            for descendant in get_subpredicates(predicate)
        ]
