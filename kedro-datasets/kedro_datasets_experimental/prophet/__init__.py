"""``JSONDataset`` implementation to load/save data from/to a Prophet model file."""

from typing import Any

import lazy_loader as lazy

ProphetDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"prophet_dataset": ["ProphetModelDataset"]}
)
