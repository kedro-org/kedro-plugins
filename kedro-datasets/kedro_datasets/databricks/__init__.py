"""Provides interface to Unity Catalog Tables."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
ManagedTableDataSet: type[ManagedTableDataset]
ManagedTableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "managed_table_dataset": ["ManagedTableDataSet", "ManagedTableDataset"]
    },
)
