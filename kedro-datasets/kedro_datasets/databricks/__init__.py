"""Provides interface to Unity Catalog Tables."""

__all__ = ["ManagedTableDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .unity import ManagedTableDataSet
