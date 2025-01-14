"""Provides an interface to Unity Catalog External Tables."""

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
ExternalTableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"external_table_dataset": ["ExternalTableDataset"]}
)
