"""``AbstractDataset`` implementations that produce pandas DataFrames."""

from typing import Any

import lazy_loader as lazy

try:
    from .csv_dataset import CSVDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    CSVDataset: Any

try:
    from .eager_polars_dataset import EagerPolarsDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    EagerPolarsDataset: Any

try:
    from .lazy_polars_dataset import LazyPolarsDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
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
