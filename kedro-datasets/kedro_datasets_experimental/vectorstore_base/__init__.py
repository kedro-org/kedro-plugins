"""Abstract base classes for vector store dataset implementations."""

import lazy_loader as lazy

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "abstract_vector_store_dataset": ["AbstractVectorStoreDataset"],
        "vector_store_handle": ["VectorStoreHandle"],
    },
)
