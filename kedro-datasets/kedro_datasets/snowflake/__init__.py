"""Provides I/O modules for Snowflake."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
SnowparkTableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"snowpark_dataset": ["SnowparkTableDataset"]},
)
