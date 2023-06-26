"""``AbstractDataSet`` implementation to load/save data in partitions from/to any DataSet format."""

__all__ = ["PartitionedDataSet", "IncrementalDataSet"]

KEY_PROPAGATION_WARNING = (
    "Top-level %(keys)s will not propagate into the %(target)s since "
    "%(keys)s were explicitly defined in the %(target)s config."
)

from contextlib import suppress

with suppress(ImportError):
    from .partitioned_dataset import PartitionedDataSet
    from .incremental_dataset import IncrementalDataSet
