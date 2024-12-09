"""``AbstractDataset`` implementation to load/save to databases using the Polars library."""

from typing import Any

import lazy_loader as lazy

PolarsDatabaseDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"polars_database_dataset": ["PolarsDatabaseDataset"]}
)
