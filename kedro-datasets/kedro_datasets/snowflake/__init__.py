"""Provides I/O modules for Snowflake."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
SnowparkTableDataSet: type[SnowparkTableDataset]
SnowparkTableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"snowpark_dataset": ["SnowparkTableDataSet", "SnowparkTableDataset"]},
)
