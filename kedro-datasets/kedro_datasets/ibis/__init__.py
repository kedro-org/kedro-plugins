"""Provide data loading and saving functionality for Ibis's backends."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
FileDataset: Any
TableDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"file_dataset": ["FileDataset"], "table_dataset": ["TableDataset"]},
)
