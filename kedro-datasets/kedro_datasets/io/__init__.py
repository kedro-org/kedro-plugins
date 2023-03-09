"""``kedro.io`` provides functionality to read and write to a
number of data sets. At core of the library is ``AbstractDataSet``
which allows implementation of various ``AbstractDataSet``s.
"""

from .core import (
    AbstractDataSet,
    AbstractVersionedDataSet,
    DataSetAlreadyExistsError,
    DataSetError,
    DataSetNotFoundError,
    Version,
)

__all__ = [
    "AbstractDataSet",
    "AbstractVersionedDataSet",
    "DataSetAlreadyExistsError",
    "DataSetError",
    "DataSetNotFoundError",
    "Version",
]
