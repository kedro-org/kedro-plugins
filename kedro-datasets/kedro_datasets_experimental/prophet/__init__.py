"""``JSONDataset`` implementation to load/save data from/to a Prophet model file."""

from typing import Any

import lazy_loader as lazy

try:
    from .prophet_dataset import ProphetModelDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ProphetModelDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"prophet_dataset": ["ProphetModelDataset"]}
)
