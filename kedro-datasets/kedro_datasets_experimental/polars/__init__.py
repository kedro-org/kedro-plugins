"""``AbstractDataset`` implementation to load/save to databases using the Polars library."""

from typing import Any

import lazy_loader as lazy

try:
    from .polars_database_dataset import PolarsDatabaseDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    PolarsDatabaseDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"polars_database_dataset": ["PolarsDatabaseDataset"]}
)
