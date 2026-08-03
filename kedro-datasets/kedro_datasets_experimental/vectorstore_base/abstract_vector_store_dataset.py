"""Base class for all vector store dataset implementations."""

from __future__ import annotations

from abc import abstractmethod
from typing import Any

from kedro.io.core import AbstractDataset, DatasetError

from .vector_store_handle import VectorStoreHandle


class AbstractVectorStoreDataset(AbstractDataset[Any, "VectorStoreHandle"]):
    """Base class for datasets that expose a vector store as a handle.

    Subclasses must implement ``_load()`` (returning a ``VectorStoreHandle``)
    and ``_describe()``.  Saving is intentionally disabled: all write
    operations go through the handle (``handle.add()``, ``handle.delete()``).

    The returned handle owns a backend connection and must be closed after
    use, either explicitly via ``handle.close()`` or via the context manager::

        with catalog.load("my_store") as store:
            store.add([{"properties": {...}, "vector": [...]}])
            hits = store.search(vector=[...], top_k=5)
    """

    # Intentionally overrides `save` (not `_save`): AbstractDataset.__init_subclass__
    # skips alias/wrapping for methods whose qualname starts with "Abstract", so
    # defining `_save` here would be silently ignored by the wrapping machinery.
    def save(self, data: Any) -> None:
        raise DatasetError(
            f"Saving is not supported for '{type(self).__name__}'. "
            "Use the handle returned by load() to write data: "
            "handle.add() for inserts and handle.delete() for removals."
        )

    @abstractmethod
    def _load(self) -> VectorStoreHandle:
        """Return a connected handle for the target collection."""

    @abstractmethod
    def _describe(self) -> dict[str, Any]:
        """Return a dict of constructor arguments for display / logging."""

    def _exists(self) -> bool:
        return False
