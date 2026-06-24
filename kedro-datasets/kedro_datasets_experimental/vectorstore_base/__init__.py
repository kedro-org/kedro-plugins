"""Abstract base classes for vector store dataset implementations."""

from typing import Any

import lazy_loader as lazy

try:
    from .abstract_vector_store_dataset import AbstractVectorStoreDataset
    from .vector_store_handle import VectorStoreHandle
except (ImportError, RuntimeError):
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    AbstractVectorStoreDataset: Any
    VectorStoreHandle: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "abstract_vector_store_dataset": ["AbstractVectorStoreDataset"],
        "vector_store_handle": ["VectorStoreHandle"],
    },
)
