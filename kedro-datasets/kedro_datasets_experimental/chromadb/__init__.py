"""`ChromaDBDataset` connects to a ChromaDB collection as a vector store."""

from typing import Any

import lazy_loader as lazy

try:
    from .chromadb_dataset import ChromaDBDataset, ChromaVectorStoreHandle
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ChromaDBDataset: Any
    ChromaVectorStoreHandle: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "chromadb_dataset": ["ChromaDBDataset", "ChromaVectorStoreHandle"]
    },
)
