"""``AbstractDataset`` implementation to load/save data from/to a Pickle file."""

from typing import Any

import lazy_loader as lazy

try:
    from .pickle_dataset import PickleDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    PickleDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"pickle_dataset": ["PickleDataset"]}
)
