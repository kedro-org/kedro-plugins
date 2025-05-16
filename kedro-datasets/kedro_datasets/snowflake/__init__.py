"""Provides I/O modules for Snowflake."""

from typing import Any

import lazy_loader as lazy

try:
    from .snowpark_dataset import SnowparkTableDataset
except (ImportError, RuntimeError):
    SnowparkTableDataset: Any

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"snowpark_dataset": ["SnowparkTableDataset"]},
)
