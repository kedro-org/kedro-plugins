"""``AbstractDataset`` implementation to read/write from/to a sequence file."""

from typing import Any

import lazy_loader as lazy

try:
    from .biosequence_dataset import BioSequenceDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    BioSequenceDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"biosequence_dataset": ["BioSequenceDataset"]},
)
