"""``AbstractDataset`` implementations that produce pandas DataFrames."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataSet: type[CSVDataset]
CSVDataset: Any
GenericDataSet: type[EagerPolarsDataset]
GenericDataset: type[EagerPolarsDataset]
EagerPolarsDataset: Any
LazyPolarsDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "csv_dataset": ["CSVDataSet", "CSVDataset"],
        "eager_polars_dataset": [
            "EagerPolarsDataset",
            "GenericDataSet",
            "GenericDataset",
        ],
        "lazy_polars_dataset": ["LazyPolarsDataset"],
    },
)
