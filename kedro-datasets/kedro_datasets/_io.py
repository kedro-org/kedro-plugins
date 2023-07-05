"""
Adapter for kedro.io.core for backwards compatibility.
"""

try:
    # kedro 0.18.11 onwards
    from kedro.io.core import DatasetError
except ImportError:
    # older versions
    from kedro.io.core import DataSetError as DatasetError

try:
    # kedro 0.18.12 onwards
    from kedro.io.core import AbstractDataset, AbstractVersionedDataset
except ImportError:
    # older versions:
    from kedro.io.core import AbstractDataSet as AbstractDataset
    from kedro.io.core import AbstractVersionedDataSet as AbstractVersionedDataset

__all__ = ["AbstractDataset", "AbstractVersionedDataset", "DatasetError"]
