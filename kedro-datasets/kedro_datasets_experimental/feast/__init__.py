"""``FeastDataset`` provides a Kedro integration for Feast.
"""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

try:
    from .feast_dataset import FeastDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    FeastDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"feast_dataset": ["FeastDataset"]}
)
