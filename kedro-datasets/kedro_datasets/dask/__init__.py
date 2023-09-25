"""Provides I/O modules using dask dataframe."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
ParquetDataSet: type[ParquetDataset]
ParquetDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"parquet_dataset": ["ParquetDataSet", "ParquetDataset"]}
)
