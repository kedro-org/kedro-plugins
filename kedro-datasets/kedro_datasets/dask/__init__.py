"""Provides I/O modules using dask dataframe."""

from typing import Any

import lazy_loader as lazy

try:
    from .csv_dataset import CSVDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    CSVDataset: Any

try:
    from .parquet_dataset import ParquetDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ParquetDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"parquet_dataset": ["ParquetDataset"], "csv_dataset": ["CSVDataset"]},
)
