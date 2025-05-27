"""Provides interface to Unity Catalog Tables."""

from typing import Any

import lazy_loader as lazy

try:
    from .managed_table_dataset import ManagedTableDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ManagedTableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"managed_table_dataset": ["ManagedTableDataset"]},
)
