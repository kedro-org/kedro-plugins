"""``AbstractDataSet`` implementations that reads from Dremio pyarrow flight."""

__all__ = ["DremioFlightDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .flight_dataset import DremioFlightDataSet
