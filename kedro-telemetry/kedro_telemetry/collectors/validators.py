"""Collector for validator related project statistics."""

from __future__ import annotations

from typing import Any

from kedro.io.data_catalog import DataCatalog

# Validator class paths from these top-level packages are public library names;
# anything else is user-defined and reported as "custom".
_PUBLIC_VALIDATOR_NAMESPACES = frozenset(
    {"pandera", "pydantic", "great_expectations", "kedro", "kedro_datasets"}
)


def collect_validator_properties(catalog: DataCatalog) -> dict[str, Any]:
    """Count the validator declarations of the catalog.

    Validator declarations are exposed by the public ``validator_specs`` property
    on ``kedro >= 1.6`` catalogs. Returns ``number_of_validated_datasets`` and one
    flat ``validator_type_count.<library>`` property per library. Only public
    library names are reported; user-defined validators are bucketed as ``custom``.
    Nothing is returned for catalogs without validators.
    """
    properties: dict[str, Any] = {}
    validator_specs = getattr(catalog, "validator_specs", None)
    if not validator_specs:
        return properties

    properties["number_of_validated_datasets"] = len(validator_specs)
    validator_type_counts: dict[str, int] = {}
    for spec in validator_specs.values():
        class_path = getattr(spec, "class_path", "") or ""
        top_level = class_path.split(".")[0]
        key = top_level if top_level in _PUBLIC_VALIDATOR_NAMESPACES else "custom"
        validator_type_counts[key] = validator_type_counts.get(key, 0) + 1
    for type_name, count in validator_type_counts.items():
        properties[f"validator_type_count.{type_name}"] = count
    return properties
