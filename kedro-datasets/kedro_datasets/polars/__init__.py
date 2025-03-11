"""``AbstractDataset`` implementations that produce pandas DataFrames."""

from typing import Any

import lazy_loader as lazy

# Import the PolarsExcelDataset
from .polars_excel_dataset import PolarsExcelDataset

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataset: Any
EagerPolarsDataset: Any
LazyPolarsDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "csv_dataset": ["CSVDataset"],
        "eager_polars_dataset": [
            "EagerPolarsDataset",
        ],
        "lazy_polars_dataset": ["LazyPolarsDataset"],
    },
)
