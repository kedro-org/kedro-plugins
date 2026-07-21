"""``WeaviateVectorStoreDataset`` connects to a Weaviate collection as a vector store."""

from typing import Any

import lazy_loader as lazy

try:
    from .weaviate_vector_store_dataset import (
        WeaviateVectorStoreDataset,
        WeaviateVectorStoreHandle,
    )
except (ImportError, RuntimeError):
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    WeaviateVectorStoreDataset: Any
    WeaviateVectorStoreHandle: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "weaviate_vector_store_dataset": [
            "WeaviateVectorStoreDataset",
            "WeaviateVectorStoreHandle",
        ]
    },
)
