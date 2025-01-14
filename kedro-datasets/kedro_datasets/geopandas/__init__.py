"""``GenericDataset`` is an ``AbstractVersionedDataset`` to save and load GeoDataFrames."""

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
GenericDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"generic_dataset": ["GenericDataset"]}
)
