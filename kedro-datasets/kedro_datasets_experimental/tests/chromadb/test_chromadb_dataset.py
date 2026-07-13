"""Tests for ChromaDBDataset and ChromaVectorStoreHandle.

Handle read/write tests run against a real in-process ephemeral Chroma
client (always with explicit vectors, so no embedding model is downloaded).
Error-wrapping and text-search paths use mocks.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import chromadb
import pytest
from kedro.io.core import DatasetError

from kedro_datasets_experimental.chromadb import (
    ChromaDBDataset,
    ChromaVectorStoreHandle,
)

MODULE = "kedro_datasets_experimental.chromadb.chromadb_dataset"


@pytest.fixture
def collection_name():
    """Unique name per test: ephemeral Chroma clients share in-process state."""
    return f"test_collection_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def dataset(collection_name):
    return ChromaDBDataset(collection_name=collection_name)


@pytest.fixture
def store(dataset):
    return dataset.load()


@pytest.fixture
def sample_records():
    return [
        {
            "id": "doc1",
            "properties": {"document": "A document about machine learning", "topic": "ml"},
            "vector": [1.0, 0.0, 0.0],
        },
        {
            "id": "doc2",
            "properties": {"document": "A document about data science", "topic": "ds"},
            "vector": [0.0, 1.0, 0.0],
        },
        {
            "id": "doc3",
            "properties": {"document": "A document about AI", "topic": "ai"},
            "vector": [0.0, 0.0, 1.0],
        },
    ]


# ---------------------------------------------------------------------------
# ChromaDBDataset — init and describe
# ---------------------------------------------------------------------------


class TestDatasetInit:
    def test_defaults(self, dataset, collection_name):
        assert dataset._collection_name == collection_name
        assert dataset._client_type == "ephemeral"
        assert dataset._client_settings == {}
        assert dataset._create_collection_if_missing is True
        assert dataset.metadata is None

    def test_custom_params(self):
        ds = ChromaDBDataset(
            collection_name="X",
            client_type="http",
            client_settings={"host": "myhost", "port": 9000},
            create_collection_if_missing=False,
            metadata={"owner": "team-a"},
        )
        assert ds._client_type == "http"
        assert ds._client_settings == {"host": "myhost", "port": 9000}
        assert ds._create_collection_if_missing is False
        assert ds.metadata == {"owner": "team-a"}

    def test_none_settings_become_empty(self):
        ds = ChromaDBDataset(collection_name="X", client_settings=None)
        assert ds._client_settings == {}


class TestDescribe:
    def test_describe(self, dataset, collection_name):
        assert dataset._describe() == {
            "collection_name": collection_name,
            "client_type": "ephemeral",
            "create_collection_if_missing": True,
        }

    def test_describe_omits_client_settings(self):
        # http client_settings can carry authentication headers
        ds = ChromaDBDataset(
            collection_name="X",
            client_type="http",
            client_settings={"headers": {"Authorization": "Bearer token"}},
        )
        desc = ds._describe()
        assert "client_settings" not in desc
        assert "token" not in str(desc)


# ---------------------------------------------------------------------------
# ChromaDBDataset — save() disabled
# ---------------------------------------------------------------------------


class TestSaveDisabled:
    def test_save_raises(self, dataset):
        with pytest.raises(DatasetError, match="Saving is not supported"):
            dataset.save({"documents": ["x"], "ids": ["1"]})


# ---------------------------------------------------------------------------
# ChromaDBDataset — client creation
# ---------------------------------------------------------------------------


class TestCreateClient:
    def test_ephemeral(self, dataset):
        with patch(f"{MODULE}.chromadb.EphemeralClient") as p:
            dataset._create_client()
            p.assert_called_once_with()

    def test_persistent_default_path(self, collection_name):
        ds = ChromaDBDataset(collection_name=collection_name, client_type="persistent")
        with patch(f"{MODULE}.chromadb.PersistentClient") as p:
            ds._create_client()
            p.assert_called_once_with(path="./chroma_db")

    def test_persistent_custom_path(self, collection_name, tmp_path):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="persistent",
            client_settings={"path": str(tmp_path)},
        )
        with patch(f"{MODULE}.chromadb.PersistentClient") as p:
            ds._create_client()
            p.assert_called_once_with(path=str(tmp_path))

    def test_ephemeral_settings_forwarded(self, collection_name):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_settings={"tenant": "my_tenant"},
        )
        with patch(f"{MODULE}.chromadb.EphemeralClient") as p:
            ds._create_client()
            p.assert_called_once_with(tenant="my_tenant")

    def test_http_defaults_and_extras(self, collection_name):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="http",
            client_settings={"host": "myhost", "ssl": True},
        )
        with patch(f"{MODULE}.chromadb.HttpClient") as p:
            ds._create_client()
            p.assert_called_once_with(host="myhost", port=8000, ssl=True)

    def test_http_default_host_and_custom_port(self, collection_name):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="http",
            client_settings={"port": 9000},
        )
        with patch(f"{MODULE}.chromadb.HttpClient") as p:
            ds._create_client()
            p.assert_called_once_with(host="localhost", port=9000)

    def test_unsupported_type_raises(self, collection_name):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="grpc",  # type: ignore[arg-type]
        )
        with pytest.raises(DatasetError, match="Unsupported client_type"):
            ds._create_client()

    def test_settings_not_mutated(self, collection_name, tmp_path):
        settings = {"path": str(tmp_path)}
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="persistent",
            client_settings=settings,
        )
        with patch(f"{MODULE}.chromadb.PersistentClient"):
            ds._create_client()
        assert settings == {"path": str(tmp_path)}


# ---------------------------------------------------------------------------
# ChromaDBDataset — load()
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_returns_handle(self, dataset):
        handle = dataset.load()
        assert isinstance(handle, ChromaVectorStoreHandle)

    def test_load_creates_missing_collection(self, dataset, collection_name):
        handle = dataset.load()
        assert handle.describe() == {"collection": collection_name, "count": 0}

    def test_load_missing_collection_raises_when_no_create(self, collection_name):
        ds = ChromaDBDataset(
            collection_name=collection_name, create_collection_if_missing=False
        )
        with pytest.raises(DatasetError, match="does not exist"):
            ds.load()

    def test_load_gets_existing_collection_when_no_create(
        self, dataset, collection_name, sample_records
    ):
        dataset.load().add(sample_records)
        ds = ChromaDBDataset(
            collection_name=collection_name, create_collection_if_missing=False
        )
        assert ds.load().describe()["count"] == 3

    def test_load_wraps_client_errors(self, dataset):
        mock_client = MagicMock()
        mock_client.get_or_create_collection.side_effect = RuntimeError("boom")
        with patch.object(dataset, "_create_client", return_value=mock_client):
            with pytest.raises(DatasetError, match="Failed to access Chroma collection"):
                dataset.load()


# ---------------------------------------------------------------------------
# ChromaVectorStoreHandle — lifecycle
# ---------------------------------------------------------------------------


class TestHandleLifecycle:
    def test_raw_client(self, store):
        assert isinstance(store.raw_client, chromadb.api.ClientAPI)

    def test_close_calls_client_close_when_owned(self):
        mock_client = MagicMock()
        handle = ChromaVectorStoreHandle(mock_client, MagicMock(), close_client=True)
        handle.close()
        mock_client.close.assert_called_once()

    def test_close_is_idempotent(self):
        mock_client = MagicMock()
        handle = ChromaVectorStoreHandle(mock_client, MagicMock(), close_client=True)
        handle.close()
        handle.close()
        mock_client.close.assert_called_once()

    def test_close_is_noop_for_ephemeral(self, store):
        # Ephemeral handles must not close the process-shared client.
        assert store._close_client is False
        with patch.object(store._client, "close") as mock_close:
            store.close()
        mock_close.assert_not_called()

    def test_persistent_load_closes_client(self, collection_name, tmp_path):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="persistent",
            client_settings={"path": str(tmp_path / "chroma")},
        )
        handle = ds.load()
        assert handle._close_client is True
        handle.close()

    def test_context_manager_returns_self_and_closes(self, store):
        with store as s:
            assert s is store
        assert store._closed is True

    def test_ephemeral_operations_work_after_context_exit(self, store, sample_records):
        # close() is a documented no-op for ephemeral clients.
        with store:
            store.add(sample_records)
        assert store.describe()["count"] == 3


# ---------------------------------------------------------------------------
# ChromaVectorStoreHandle — add()
# ---------------------------------------------------------------------------


class TestHandleAdd:
    def test_add_returns_ids_in_input_order(self, store, sample_records):
        assert store.add(sample_records) == ["doc1", "doc2", "doc3"]

    def test_add_generates_ids_when_missing(self, store):
        ids = store.add(
            [
                {"properties": {"document": "no id"}, "vector": [1.0, 0.0, 0.0]},
                {"properties": {"document": "also no id"}, "vector": [0.0, 1.0, 0.0]},
            ]
        )
        assert len(ids) == 2
        assert all(isinstance(record_id, str) and record_id for record_id in ids)
        assert ids[0] != ids[1]

    def test_add_maps_document_and_metadata(self, store, sample_records):
        store.add(sample_records)
        stored = store._collection.get(ids=["doc1"], include=["documents", "metadatas"])
        assert stored["documents"] == ["A document about machine learning"]
        assert stored["metadatas"] == [{"topic": "ml"}]

    def test_add_vector_only_records(self, store):
        ids = store.add([{"properties": {"topic": "ml"}, "vector": [1.0, 0.0, 0.0]}])
        assert store.describe()["count"] == 1
        stored = store._collection.get(ids=ids, include=["documents", "metadatas"])
        assert stored["documents"] == [None]
        assert stored["metadatas"] == [{"topic": "ml"}]

    def test_add_document_only_properties(self, store):
        # properties with only a document -> empty metadata must become None
        ids = store.add(
            [{"properties": {"document": "just text"}, "vector": [1.0, 0.0, 0.0]}]
        )
        stored = store._collection.get(ids=ids, include=["documents", "metadatas"])
        assert stored["documents"] == ["just text"]
        assert stored["metadatas"] == [None]

    def test_add_without_properties_key(self, store):
        ids = store.add([{"vector": [1.0, 0.0, 0.0]}])
        assert len(ids) == 1
        assert store.describe()["count"] == 1

    def test_add_with_none_properties(self, store):
        ids = store.add([{"properties": None, "vector": [1.0, 0.0, 0.0]}])
        assert len(ids) == 1
        assert store.describe()["count"] == 1

    def test_add_heterogeneous_batch(self, store):
        # One record with document+metadata, one with neither.
        ids = store.add(
            [
                {
                    "id": "full",
                    "properties": {"document": "text", "topic": "ml"},
                    "vector": [1.0, 0.0, 0.0],
                },
                {"id": "bare", "properties": {}, "vector": [0.0, 1.0, 0.0]},
            ]
        )
        assert ids == ["full", "bare"]
        stored = store._collection.get(ids=ids, include=["documents", "metadatas"])
        by_id = dict(zip(stored["ids"], zip(stored["documents"], stored["metadatas"])))
        assert by_id["full"] == ("text", {"topic": "ml"})
        assert by_id["bare"] == (None, None)

    def test_add_existing_id_raises(self, store, sample_records):
        store.add(sample_records)
        with pytest.raises(DatasetError, match="already[\\s\\S]*exists"):
            store.add(
                [{"id": "doc1", "properties": {"document": "replacement"},
                  "vector": [0.5, 0.5, 0.0]}]
            )
        # Original record untouched
        stored = store._collection.get(ids=["doc1"], include=["documents"])
        assert stored["documents"] == ["A document about machine learning"]

    def test_add_mixed_new_and_existing_ids_writes_nothing(self, store, sample_records):
        store.add(sample_records)
        with pytest.raises(DatasetError, match="doc1"):
            store.add(
                [
                    {"id": "doc1", "properties": {"document": "stale"},
                     "vector": [0.5, 0.5, 0.0]},
                    {"id": "doc4", "properties": {"document": "new"},
                     "vector": [0.0, 0.5, 0.5]},
                ]
            )
        assert store.describe()["count"] == 3

    def test_add_empty_string_id_rejected(self, store):
        # "" is passed through to Chroma, which rejects empty IDs.
        with pytest.raises(DatasetError, match="add\\(\\) failed"):
            store.add(
                [{"id": "", "properties": {"document": "x"}, "vector": [1.0, 0.0, 0.0]}]
            )

    def test_add_empty_list_returns_empty(self, store):
        assert store.add([]) == []
        assert store.describe()["count"] == 0

    def test_add_mixed_vectors_raises(self, store):
        with pytest.raises(DatasetError, match="all records or no records"):
            store.add(
                [
                    {"properties": {"document": "a"}, "vector": [1.0, 0.0, 0.0]},
                    {"properties": {"document": "b"}},
                ]
            )

    def test_add_does_not_mutate_input(self, store):
        record = {
            "id": "u1",
            "properties": {"document": "hello", "topic": "ml"},
            "vector": [1.0, 0.0, 0.0],
        }
        store.add([record])
        assert record == {
            "id": "u1",
            "properties": {"document": "hello", "topic": "ml"},
            "vector": [1.0, 0.0, 0.0],
        }

    def test_add_wraps_chroma_exception(self, store):
        with patch.object(
            store._collection, "add", side_effect=RuntimeError("disk full")
        ):
            with pytest.raises(DatasetError, match="add\\(\\) failed"):
                store.add([{"properties": {"document": "x"}, "vector": [1.0, 0.0, 0.0]}])

    def test_add_wrong_vector_dims_error_wrapped(self, store, sample_records):
        store.add(sample_records)
        with pytest.raises(DatasetError, match="add\\(\\) failed"):
            store.add([{"properties": {"document": "bad dims"}, "vector": [1.0]}])


# ---------------------------------------------------------------------------
# ChromaVectorStoreHandle — delete()
# ---------------------------------------------------------------------------


class TestHandleDelete:
    def test_delete_by_ids(self, store, sample_records):
        store.add(sample_records)
        store.delete(ids=["doc1", "doc2"])
        assert store.describe()["count"] == 1

    def test_delete_by_filters(self, store, sample_records):
        store.add(sample_records)
        store.delete(filters={"topic": "ml"})
        assert store.describe()["count"] == 2

    def test_delete_empty_ids_is_noop(self, store, sample_records):
        store.add(sample_records)
        store.delete(ids=[])
        assert store.describe()["count"] == 3

    def test_delete_requires_ids_or_filters(self, store):
        with pytest.raises(DatasetError, match="requires exactly one"):
            store.delete()

    def test_delete_rejects_both(self, store):
        with pytest.raises(DatasetError, match="not both"):
            store.delete(ids=["doc1"], filters={"topic": "ml"})

    def test_delete_wraps_chroma_exception(self, store):
        with patch.object(
            store._collection, "delete", side_effect=RuntimeError("connection lost")
        ):
            with pytest.raises(DatasetError, match="delete\\(\\) failed"):
                store.delete(ids=["doc1"])


# ---------------------------------------------------------------------------
# ChromaVectorStoreHandle — search()
# ---------------------------------------------------------------------------


class TestHandleSearch:
    def test_search_by_vector_returns_formatted_results(self, store, sample_records):
        store.add(sample_records)
        results = store.search(vector=[1.0, 0.0, 0.0], top_k=1)
        assert len(results) == 1
        assert results[0]["id"] == "doc1"
        assert results[0]["properties"] == {
            "document": "A document about machine learning",
            "topic": "ml",
        }
        assert results[0]["distance"] == pytest.approx(0.0)

    def test_search_respects_top_k(self, store, sample_records):
        store.add(sample_records)
        assert len(store.search(vector=[1.0, 0.0, 0.0], top_k=2)) == 2

    def test_search_empty_collection_returns_empty(self, store):
        assert store.search(vector=[1.0, 0.0, 0.0], top_k=5) == []

    def test_search_top_k_above_count_returns_all(self, store, sample_records):
        store.add(sample_records)
        assert len(store.search(vector=[1.0, 0.0, 0.0], top_k=50)) == 3

    def test_search_with_filters(self, store, sample_records):
        store.add(sample_records)
        results = store.search(vector=[1.0, 0.0, 0.0], top_k=3, filters={"topic": "ds"})
        assert [hit["id"] for hit in results] == ["doc2"]

    def test_search_result_without_document(self, store):
        ids = store.add([{"properties": {"topic": "ml"}, "vector": [1.0, 0.0, 0.0]}])
        results = store.search(vector=[1.0, 0.0, 0.0], top_k=1)
        assert results[0]["id"] == ids[0]
        assert results[0]["properties"] == {"topic": "ml"}
        assert "document" not in results[0]["properties"]

    def test_search_by_text_uses_query_texts(self, store):
        # Mocked: real text search would download the default embedding model.
        fake_result = {
            "ids": [["doc1"]],
            "documents": [["hello"]],
            "metadatas": [[{"topic": "ml"}]],
            "distances": [[0.25]],
        }
        with patch.object(
            store._collection, "query", return_value=fake_result
        ) as mock_query:
            results = store.search(text="machine learning", top_k=5)
        mock_query.assert_called_once_with(
            n_results=5, where=None, query_texts=["machine learning"]
        )
        assert results == [
            {
                "id": "doc1",
                "properties": {"topic": "ml", "document": "hello"},
                "distance": 0.25,
            }
        ]

    def test_search_requires_vector_or_text(self, store):
        with pytest.raises(DatasetError, match="requires exactly one"):
            store.search()

    def test_search_rejects_both(self, store):
        with pytest.raises(DatasetError, match="not both"):
            store.search(vector=[1.0, 0.0, 0.0], text="query")

    def test_search_wraps_chroma_exception(self, store):
        with patch.object(
            store._collection, "query", side_effect=RuntimeError("index error")
        ):
            with pytest.raises(DatasetError, match="search\\(\\) failed"):
                store.search(vector=[1.0, 0.0, 0.0])


# ---------------------------------------------------------------------------
# End-to-end roundtrip
# ---------------------------------------------------------------------------


class TestRoundtrip:
    def test_add_search_delete_roundtrip(self, store, sample_records):
        ids = store.add(sample_records)
        assert store.describe()["count"] == 3

        hits = store.search(vector=[0.0, 1.0, 0.0], top_k=1)
        assert hits[0]["id"] == "doc2"
        assert hits[0]["properties"]["topic"] == "ds"

        store.delete(ids=ids)
        assert store.describe()["count"] == 0

    def test_persistent_roundtrip(self, collection_name, tmp_path, sample_records):
        ds = ChromaDBDataset(
            collection_name=collection_name,
            client_type="persistent",
            client_settings={"path": str(tmp_path / "chroma")},
        )
        ds.load().add(sample_records)

        # A fresh load against the same path sees the persisted data.
        assert ds.load().describe()["count"] == 3
