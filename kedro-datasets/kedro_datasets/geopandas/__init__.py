"""``GenericDataset`` is an ``AbstractVersionedDataset`` to save and load GeoDataFrames."""

from typing import Any

import lazy_loader as lazy

try:
    from .generic_dataset import GenericDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    GenericDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"generic_dataset": ["GenericDataset"]}
)
