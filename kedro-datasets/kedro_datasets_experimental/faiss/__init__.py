"""`FAISSVectorStoreDataset` connects to a FAISS index as a vector store."""

from typing import Any

import lazy_loader as lazy

try:
    from .faiss_vector_store_dataset import (
        FAISSVectorStoreDataset,
        FAISSVectorStoreHandle,
    )
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    FAISSVectorStoreDataset: Any
    FAISSVectorStoreHandle: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "faiss_vector_store_dataset": ["FAISSVectorStoreDataset", "FAISSVectorStoreHandle"]
    },
)
