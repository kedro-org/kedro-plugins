"""``AbstractDataset`` implementation to load/save data in partitions
from/to any underlying Dataset format.
"""

__all__ = ["PartitionedDataset", "IncrementalDataset"]

from contextlib import suppress

with suppress(ImportError):
    from .incremental_dataset import IncrementalDataset
    from .partitioned_dataset import PartitionedDataset
