"""``AbstractDataSet`` implementation to load/save data in partitions from/to any DataSet format."""

__all__ = ["PartitionedDataSet", "IncrementalDataSet"]

from contextlib import suppress

KEY_PROPAGATION_WARNING = (
    "Top-level %(keys)s will not propagate into the %(target)s since "
    "%(keys)s were explicitly defined in the %(target)s config."
)

with suppress(ImportError):
    from .incremental_dataset import IncrementalDataSet
    from .partitioned_dataset import PartitionedDataSet
