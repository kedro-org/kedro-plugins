"""``AbstractDataset`` implementations that produce pandas DataFrames."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataSet: type[CSVDataset]
CSVDataset: Any
GenericDataSet: type[GenericDataset]
GenericDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "csv_dataset": ["CSVDataSet", "CSVDataset"],
        "generic_dataset": ["GenericDataSet", "GenericDataset"],
    },
)
