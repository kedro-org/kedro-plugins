"""Provides an interface to Unity Catalog External Tables."""

from typing import Any

import lazy_loader as lazy

try:
    from .external_table_dataset import ExternalTableDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ExternalTableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"external_table_dataset": ["ExternalTableDataset"]}
)
