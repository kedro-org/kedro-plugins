"""``AbstractDataset`` implementations that produce pandas DataFrames."""

from typing import Any

import lazy_loader as lazy

DeltaSharingDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "delta_sharing_dataset": ["DeltaSharingDataset"],
    },
)
