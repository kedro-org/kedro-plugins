"""Contract tests for AbstractVectorStoreDataset and VectorStoreHandle.

Uses an entirely in-memory dummy implementation so there are no external deps.
These tests verify the ABC contract and Kedro integration, not business logic.
"""

from __future__ import annotations

from typing import Any

import pytest
from kedro.io.core import DatasetError

from kedro_datasets_experimental.vectorstore_base import (
    AbstractVectorStoreDataset,
    VectorStoreHandle,
)


# ---------------------------------------------------------------------------
# Dummy in-memory implementations
# ---------------------------------------------------------------------------


class DummyHandle(VectorStoreHandle):
    """Minimal in-memory handle for contract testing."""

    def __init__(self, collection: str) -> None:
        self._collection = collection
        self._records: dict[str, dict[str, Any]] = {}
        self._closed = False

    def add(self, records: list[dict[str, Any]]) -> list[str]:
        ids = []
        for i, rec in enumerate(records):
            rid = rec.get("id") or f"id-{len(self._records) + i}"
            self._records[rid] = rec
            ids.append(rid)
        return ids

    def search(
        self,
        *,
        vector: list[float] | None = None,
        text: str | None = None,
        top_k: int = 10,
        filters: Any = None,
    ) -> list[dict[str, Any]]:
        if vector is None and text is None:
            raise ValueError("Exactly one of vector or text must be provided.")
        results = list(self._records.values())[:top_k]
        return [{"id": r.get("id", ""), "properties": r.get("properties", {})} for r in results]

    def delete(self, *, ids: list[str] | None = None, filters: Any = None) -> None:
        if ids is not None:
            for rid in ids:
                self._records.pop(rid, None)

    def describe(self) -> dict[str, Any]:
        return {"collection": self._collection, "count": len(self._records)}

    @property
    def raw_client(self) -> dict[str, Any]:
        return {"store": self._records}

    def close(self) -> None:
        self._closed = True


class DummyVectorStoreDataset(AbstractVectorStoreDataset):
    """Minimal concrete dataset backed by a DummyHandle."""

    def __init__(self, collection: str = "test") -> None:
        self._collection = collection

    def _load(self) -> DummyHandle:
        return DummyHandle(self._collection)

    def _describe(self) -> dict[str, Any]:
        return {"collection": self._collection}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dataset() -> DummyVectorStoreDataset:
    return DummyVectorStoreDataset(collection="contract-test")


@pytest.fixture
def handle(dataset: DummyVectorStoreDataset) -> DummyHandle:
    return dataset.load()


# ---------------------------------------------------------------------------
# AbstractVectorStoreDataset contract
# ---------------------------------------------------------------------------


class TestAbstractVectorStoreDataset:
    def test_load_returns_handle(self, dataset):
        handle = dataset.load()
        assert isinstance(handle, VectorStoreHandle)

    def test_save_raises_dataset_error(self, dataset):
        with pytest.raises(DatasetError, match="Saving is not supported"):
            dataset.save({"anything": True})

    def test_describe_returns_dict(self, dataset):
        desc = dataset._describe()
        assert isinstance(desc, dict)
        assert "collection" in desc

    def test_exists_returns_false(self, dataset):
        assert dataset.exists() is False

    def test_cannot_instantiate_abstract_class(self):
        with pytest.raises(TypeError):
            AbstractVectorStoreDataset()  # type: ignore[abstract]

    def test_cannot_instantiate_with_missing_load(self):
        class Incomplete(AbstractVectorStoreDataset):
            def _describe(self):
                return {}

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# VectorStoreHandle contract
# ---------------------------------------------------------------------------


class TestVectorStoreHandle:
    def test_add_returns_ids(self, handle):
        records = [
            {"id": "a", "properties": {"text": "hello"}},
            {"id": "b", "properties": {"text": "world"}},
        ]
        ids = handle.add(records)
        assert ids == ["a", "b"]

    def test_search_by_vector(self, handle):
        handle.add([{"id": "x", "properties": {"text": "sample"}}])
        results = handle.search(vector=[0.1, 0.2])
        assert isinstance(results, list)
        assert all("id" in r and "properties" in r for r in results)

    def test_search_by_text(self, handle):
        handle.add([{"id": "y", "properties": {"text": "sample"}}])
        results = handle.search(text="sample query")
        assert isinstance(results, list)

    def test_search_requires_vector_or_text(self, handle):
        with pytest.raises((ValueError, DatasetError)):
            handle.search()

    def test_delete_by_ids(self, handle):
        handle.add([{"id": "del-me", "properties": {}}])
        handle.delete(ids=["del-me"])
        results = handle.search(vector=[0.0])
        assert not any(r["id"] == "del-me" for r in results)

    def test_describe_returns_dict_with_count(self, handle):
        info = handle.describe()
        assert isinstance(info, dict)
        assert "count" in info

    def test_raw_client_accessible(self, handle):
        assert handle.raw_client is not None

    def test_close_marks_handle_closed(self, handle):
        handle.close()
        assert handle._closed is True

    def test_context_manager_calls_close(self, dataset):
        with dataset.load() as h:
            assert isinstance(h, DummyHandle)
        assert h._closed is True

    def test_cannot_instantiate_abstract_handle(self):
        with pytest.raises(TypeError):
            VectorStoreHandle()  # type: ignore[abstract]
