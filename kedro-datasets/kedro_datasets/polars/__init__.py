"""``AbstractDataSet`` implementations that produce pandas DataFrames."""

__all__ = ["CSVDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .csv_dataset import CSVDataSet
