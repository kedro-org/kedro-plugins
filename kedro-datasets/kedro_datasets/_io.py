"""
Adapter for kedro.io.core for backwards compatibility.
"""

try:
    # kedro 0.18.11 onwards
    from kedro.io.core import DatasetError
except ImportError:
    # older versions
    from kedro.io.core import DataSetError as DatasetError

__all__ = ["DatasetError"]
