"""``AbstractDataSet`` implementation to load/save data in partitions
from/to any underlying DataSet format.
"""

__all__ = ["PartitionedDataSet", "IncrementalDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .incremental_dataset import IncrementalDataSet
    from .partitioned_dataset import PartitionedDataSet
