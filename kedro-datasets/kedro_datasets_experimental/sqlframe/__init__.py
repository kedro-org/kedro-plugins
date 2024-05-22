"""Provide data loading and saving functionality for SqlFrame's backends."""

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
TableDataset: Any
FileDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"sqlframe": ["TableDataset", "FileDataset"]}
)
