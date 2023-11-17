"""``AbstractDataset`` implementation to load/save data from/to a Matlab file."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

PickleDataSet: type[PickleDataset]
PickleDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"pickle_dataset": ["PickleDataset"]}
)
