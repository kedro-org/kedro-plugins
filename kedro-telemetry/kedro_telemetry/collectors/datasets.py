"""Collector for dataset related project statistics."""

from __future__ import annotations

from typing import Any

from kedro.io.data_catalog import DataCatalog

_PARAMETER_PREFIXES = ("parameters", "params:")
_PUBLIC_DATASET_PREFIXES = (
    "kedro_datasets.",
    "kedro.io.",
    "kedro_datasets_experimental.",
)


def collect_dataset_properties(catalog: DataCatalog) -> dict[str, Any]:
    """Count the catalog datasets, in total and per dataset type.

    Returns ``number_of_datasets`` and, for ``kedro >= 1.0`` catalogs, one flat
    ``dataset_type_count.<type>`` property per dataset type. User-defined dataset
    types are reported as ``custom``.
    """
    # Support both catalog.list() for `kedro < 1.0` and catalog.keys() for `kedro >= 1.0`
    dataset_type_counts: dict[str, int] = {}
    if hasattr(catalog, "keys") and callable(catalog.keys):
        # Only collect dataset types for kedro >= 1.0 because `get_type` method is not available in earlier versions
        dataset_names = catalog.keys()
        for ds_name in dataset_names:
            if ds_name.startswith(_PARAMETER_PREFIXES):
                continue
            ds_type = catalog.get_type(ds_name) or ""
            key = ds_type if ds_type.startswith(_PUBLIC_DATASET_PREFIXES) else "custom"
            dataset_type_counts[key] = dataset_type_counts.get(key, 0) + 1
    else:
        dataset_names = catalog.list()  # type: ignore

    properties: dict[str, Any] = {
        "number_of_datasets": sum(
            1 for c in dataset_names if not c.startswith(_PARAMETER_PREFIXES)
        ),
    }
    # Flatten per-type counts into individual scalar properties so they are
    # accepted by Heap (which only allows string/number property values) and
    # can be aggregated/grouped in Heap dashboards.
    for type_name, count in dataset_type_counts.items():
        properties[f"dataset_type_count.{type_name}"] = count
    return properties
