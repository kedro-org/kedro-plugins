"""Datasets for loading data from HTTP(S) APIs
and returns them into either as string or json Dict.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""

from typing import Any

import lazy_loader as lazy

try:
    from .api_dataset import APIDataset, PaginatedAPIDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    APIDataset: Any
    PaginatedAPIDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"api_dataset": ["APIDataset", "PaginatedAPIDataset"]}
)
