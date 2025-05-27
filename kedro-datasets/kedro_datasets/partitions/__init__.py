"""``AbstractDataset`` implementations to load/save data in partitions
from/to any underlying dataset format.
"""

from typing import Any

import lazy_loader as lazy

try:
    from .incremental_dataset import IncrementalDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    IncrementalDataset: Any

try:
    from .partitioned_dataset import PartitionedDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    PartitionedDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "incremental_dataset": ["IncrementalDataset"],
        "partitioned_dataset": ["PartitionedDataset"],
    },
)
