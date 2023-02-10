"""Provides interface to Unity Catalog Tables."""

__all__ = ["UnityTableDataSet"]

from contextlib import suppress

with suppress(ImportError):
    from .unity import UnityTableDataSet
