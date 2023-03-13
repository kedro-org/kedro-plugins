"""Provides interface to Unity Catalog Tables."""

__all__ = ["ManagedTableDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .managed_table_dataset import ManagedTableDataSet
