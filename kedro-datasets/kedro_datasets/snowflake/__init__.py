"""Provides I/O modules for Snowflake."""

__all__ = ["SnowParkDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .snowpark_dataset import SnowParkDataSet
