"""``AbstractDataset`` implementation to load/save data from/to a text file."""

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
try:
    from .text_dataset import TextDataset
except (ImportError, RuntimeError):
    TextDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"text_dataset": ["TextDataset"]}
)
