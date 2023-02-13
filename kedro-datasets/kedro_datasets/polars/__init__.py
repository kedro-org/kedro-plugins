"""``AbstractDataSet`` implementations that produce pandas DataFrames."""

__all__ = ["CSVDataSet", "GenericDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .csv_dataset import CSVDataSet
    from .generic_dataset import GenericDataSet
