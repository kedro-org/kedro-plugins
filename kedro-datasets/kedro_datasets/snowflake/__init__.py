"""Provides I/O modules for Snowflake."""

__all__ = ["SnowparkDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .snowpark_dataset import SnowparkDataSet
