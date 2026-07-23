"""Tests for FAISSVectorStoreDataset and FAISSVectorStoreHandle.

Handle read/write tests run against a real in-process FAISS index (FAISS has
no server, so there's nothing to mock for the happy paths). Mocks are
reserved for simulated error conditions (exception wrapping).
"""

from __future__ import annotations

from unittest.mock import patch

import faiss
import numpy as np
import pytest
from kedro.io.core import DatasetError

from kedro_datasets_experimental.faiss import (
    FAISSVectorStoreDataset,
    FAISSVectorStoreHandle,
)

MODULE = "kedro_datasets_experimental.faiss.faiss_vector_store_dataset"


@pytest.fixture
def dataset():
    return FAISSVectorStoreDataset(dimension=4)


@pytest.fixture
def store(dataset):
    return dataset.load()


@pytest.fixture
def sample_records():
    return [
        {
            "id": "doc1",
            "properties": {"topic": "ml"},
            "vector": [1.0, 0.0, 0.0, 0.0],
        },
        {
            "id": "doc2",
            "properties": {"topic": "ds"},
            "vector": [0.0, 1.0, 0.0, 0.0],
        },
        {
            "id": "doc3",
            "properties": {"topic": "ai"},
            "vector": [0.0, 0.0, 1.0, 0.0],
        },
    ]


class TestDatasetInit:
    def test_defaults(self, dataset):
        assert dataset._dimension == 4
        assert dataset._index_path is None
        assert dataset._index_factory == "Flat"
        assert dataset._metric == "l2"
        assert dataset.metadata is None

    def test_custom_params(self, tmp_path):
        index_path = str(tmp_path / "x.index")
        ds = FAISSVectorStoreDataset(
            dimension=8,
            index_path=index_path,
            index_factory="IVF4,Flat",
            metric="ip",
            metadata={"owner": "me"},
        )
        assert ds._dimension == 8
        assert ds._index_path == index_path
        assert ds._index_factory == "IVF4,Flat"
        assert ds._metric == "ip"
        assert ds.metadata == {"owner": "me"}

    def test_invalid_metric_raises(self):
        with pytest.raises(DatasetError, match="Unknown metric"):
            FAISSVectorStoreDataset(dimension=4, metric="cosine")

    def test_describe(self, dataset):
        assert dataset._describe() == {
            "dimension": 4,
            "index_path": None,
            "index_factory": "Flat",
            "metric": "l2",
        }

    def test_save_raises(self, dataset):
        with pytest.raises(DatasetError, match="Saving is not supported"):
            dataset.save({})


class TestDatasetLoad:
    def test_load_returns_handle(self, dataset):
        store = dataset.load()
        assert isinstance(store, FAISSVectorStoreHandle)
        assert store.describe() == {"collection": "<in-memory>", "count": 0}

    def test_load_fresh_when_no_index_path(self, dataset):
        store = dataset.load()
        assert store.raw_client.is_trained
        assert store.raw_client.ntotal == 0

    def test_load_fresh_when_path_given_but_absent(self, tmp_path):
        ds = FAISSVectorStoreDataset(dimension=4, index_path=str(tmp_path / "missing.index"))
        store = ds.load()
        assert store.describe()["count"] == 0

    def test_load_hydrates_existing_index(self, tmp_path, sample_records):
        index_path = str(tmp_path / "my.index")
        ds1 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store1 = ds1.load()
        store1.add(sample_records)
        store1.save()

        ds2 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store2 = ds2.load()
        assert store2.describe()["count"] == 3
        hits = store2.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=1)
        assert hits[0]["id"] == "doc1"
        assert hits[0]["properties"] == {"topic": "ml"}

    def test_load_dimension_mismatch_raises(self, tmp_path):
        index_path = str(tmp_path / "my.index")
        ds1 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        ds1.load().save()

        ds2 = FAISSVectorStoreDataset(dimension=8, index_path=index_path)
        with pytest.raises(DatasetError, match="does not match the configured dimension"):
            ds2.load()

    def test_load_wraps_index_factory_error(self, dataset):
        dataset._index_factory = "NotARealFactoryString"
        with pytest.raises(DatasetError, match="Failed to create FAISS index"):
            dataset.load()

    def test_load_raises_when_only_index_file_present(self, tmp_path):
        index_path = str(tmp_path / "my.index")
        ds1 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        ds1.load().save()
        (tmp_path / "my.index.meta.json").unlink()

        ds2 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        with pytest.raises(DatasetError, match="Found one persistence file but not the other"):
            ds2.load()

    def test_load_raises_when_only_meta_file_present(self, tmp_path):
        index_path = str(tmp_path / "my.index")
        ds1 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        ds1.load().save()
        (tmp_path / "my.index").unlink()

        ds2 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        with pytest.raises(DatasetError, match="Found one persistence file but not the other"):
            ds2.load()


class TestHandleLifecycle:
    def test_raw_client_is_index_id_map(self, store):
        assert isinstance(store.raw_client, faiss.IndexIDMap)

    def test_close_is_noop(self, store):
        store.close()  # must not raise
        store.close()  # idempotent

    def test_close_does_not_persist(self, tmp_path, sample_records):
        index_path = str(tmp_path / "no_persist.index")
        ds = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store = ds.load()
        store.add(sample_records)
        store.close()
        assert not (tmp_path / "no_persist.index").exists()
        assert not (tmp_path / "no_persist.index.meta.json").exists()

    def test_context_manager_returns_self_and_closes(self, store):
        with store as s:
            assert s is store

    def test_describe_in_memory(self, store):
        assert store.describe() == {"collection": "<in-memory>", "count": 0}

    def test_describe_with_index_path(self, tmp_path):
        index_path = str(tmp_path / "d.index")
        ds = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store = ds.load()
        assert store.describe()["collection"] == index_path


class TestHandleTrain:
    def test_flat_index_already_trained(self, store):
        assert store.raw_client.is_trained

    def test_untrained_ivf_rejects_add(self):
        ds = FAISSVectorStoreDataset(dimension=8, index_factory="IVF4,Flat")
        store = ds.load()
        assert not store.raw_client.is_trained
        with pytest.raises(DatasetError, match="not trained"):
            store.add([{"vector": [0.1] * 8}])

    def test_train_then_add_succeeds(self):
        ds = FAISSVectorStoreDataset(dimension=8, index_factory="IVF4,Flat")
        store = ds.load()
        train_vecs = np.random.rand(200, 8).astype("float32").tolist()
        store.train(train_vecs)
        assert store.raw_client.is_trained
        ids = store.add([{"vector": [0.1] * 8}])
        assert len(ids) == 1

    def test_train_wraps_exception(self, store):
        with patch.object(store._index, "train", side_effect=RuntimeError("boom")):
            with pytest.raises(DatasetError, match="train\\(\\) failed"):
                store.train([[0.1, 0.2, 0.3, 0.4]])


class TestHandleAdd:
    def test_add_returns_ids_in_order(self, store, sample_records):
        ids = store.add(sample_records)
        assert ids == ["doc1", "doc2", "doc3"]
        assert store.describe()["count"] == 3

    def test_add_generates_ids_when_missing(self, store):
        ids = store.add([{"vector": [1.0, 0.0, 0.0, 0.0]}])
        assert len(ids) == 1
        assert isinstance(ids[0], str) and len(ids[0]) > 0

    def test_add_empty_list_returns_empty(self, store):
        assert store.add([]) == []
        assert store.describe()["count"] == 0

    def test_add_missing_vector_raises(self, store):
        with pytest.raises(DatasetError, match="requires a 'vector'"):
            store.add([{"id": "x", "properties": {}}])

    def test_add_wrong_dimension_raises(self, store):
        with pytest.raises(DatasetError, match="does not match the index dimension"):
            store.add([{"id": "x", "vector": [1.0, 0.0]}])

    def test_add_ragged_batch_raises_dataset_error(self, store):
        with pytest.raises(DatasetError, match="uniform array"):
            store.add(
                [
                    {"id": "a", "vector": [1.0, 0.0, 0.0, 0.0]},
                    {"id": "b", "vector": [1.0, 0.0]},
                ]
            )
        assert store.describe()["count"] == 0

    def test_add_existing_id_raises_and_writes_nothing(self, store, sample_records):
        store.add(sample_records)
        with pytest.raises(DatasetError, match="colliding"):
            store.add([{"id": "doc1", "vector": [0.0, 0.0, 0.0, 1.0]}])
        assert store.describe()["count"] == 3

    def test_add_within_batch_duplicate_raises_and_writes_nothing(self, store):
        with pytest.raises(DatasetError, match="colliding"):
            store.add(
                [
                    {"id": "dup", "vector": [1.0, 0.0, 0.0, 0.0]},
                    {"id": "dup", "vector": [0.0, 1.0, 0.0, 0.0]},
                ]
            )
        assert store.describe()["count"] == 0

    def test_add_does_not_mutate_input(self, store):
        record = {"id": "u1", "properties": {"a": 1}, "vector": [1.0, 0.0, 0.0, 0.0]}
        store.add([record])
        assert record == {"id": "u1", "properties": {"a": 1}, "vector": [1.0, 0.0, 0.0, 0.0]}

    def test_add_wraps_faiss_exception(self, store):
        with patch.object(store._index, "add_with_ids", side_effect=RuntimeError("disk full")):
            with pytest.raises(DatasetError, match="add\\(\\) failed"):
                store.add([{"vector": [1.0, 0.0, 0.0, 0.0]}])


class TestHandleDelete:
    def test_delete_by_ids(self, store, sample_records):
        store.add(sample_records)
        store.delete(ids=["doc1", "doc2"])
        assert store.describe()["count"] == 1

    def test_delete_unknown_ids_is_noop(self, store, sample_records):
        store.add(sample_records)
        store.delete(ids=["does-not-exist"])
        assert store.describe()["count"] == 3

    def test_delete_by_filters(self, store, sample_records):
        store.add(sample_records)
        store.delete(filters=lambda props: props.get("topic") == "ml")
        assert store.describe()["count"] == 2
        remaining = store.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=10)
        assert all(r["id"] != "doc1" for r in remaining)

    def test_delete_filters_exception_wrapped(self, store, sample_records):
        store.add(sample_records)
        with pytest.raises(DatasetError, match="filters\\(\\) raised an exception"):
            store.delete(filters=lambda props: props["missing_key"] == "x")
        assert store.describe()["count"] == 3

    def test_delete_requires_ids_or_filters(self, store):
        with pytest.raises(DatasetError, match="requires exactly one"):
            store.delete()

    def test_delete_rejects_both(self, store):
        with pytest.raises(DatasetError, match="not both"):
            store.delete(ids=["a"], filters=lambda p: True)

    def test_delete_wraps_faiss_exception(self, store, sample_records):
        store.add(sample_records)
        with patch.object(store._index, "remove_ids", side_effect=RuntimeError("boom")):
            with pytest.raises(DatasetError, match="delete\\(\\) failed"):
                store.delete(ids=["doc1"])


class TestHandleSearch:
    def test_search_by_vector(self, store, sample_records):
        store.add(sample_records)
        hits = store.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=1)
        assert len(hits) == 1
        assert hits[0]["id"] == "doc1"
        assert hits[0]["properties"] == {"topic": "ml"}
        assert hits[0]["distance"] == 0.0

    def test_search_respects_top_k(self, store, sample_records):
        store.add(sample_records)
        hits = store.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=2)
        assert len(hits) == 2

    def test_search_empty_index_returns_empty(self, store):
        assert store.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=5) == []

    def test_search_with_filters(self, store, sample_records):
        store.add(sample_records)
        hits = store.search(
            vector=[0.0, 0.0, 0.0, 0.0],
            top_k=10,
            filters=lambda props: props.get("topic") == "ds",
        )
        assert [h["id"] for h in hits] == ["doc2"]

    def test_search_filters_matching_nothing_returns_empty(self, store, sample_records):
        store.add(sample_records)
        hits = store.search(
            vector=[0.0, 0.0, 0.0, 0.0], top_k=10, filters=lambda props: False
        )
        assert hits == []

    def test_search_filters_exception_wrapped(self, store, sample_records):
        store.add(sample_records)
        with pytest.raises(DatasetError, match="filters\\(\\) raised an exception"):
            store.search(
                vector=[1.0, 0.0, 0.0, 0.0],
                top_k=10,
                filters=lambda props: props["missing_key"] == "x",
            )

    def test_search_text_raises_not_implemented(self, store):
        with pytest.raises(NotImplementedError, match="no embedding function"):
            store.search(text="hello")

    def test_search_requires_vector_or_text(self, store):
        with pytest.raises(DatasetError, match="requires exactly one"):
            store.search()

    def test_search_rejects_both(self, store):
        with pytest.raises(DatasetError, match="not both"):
            store.search(vector=[1.0, 0.0, 0.0, 0.0], text="hello")

    def test_search_wraps_faiss_exception(self, store, sample_records):
        store.add(sample_records)
        with patch.object(store._index, "search", side_effect=RuntimeError("boom")):
            with pytest.raises(DatasetError, match="search\\(\\) failed"):
                store.search(vector=[1.0, 0.0, 0.0, 0.0])


class TestHandleSave:
    def test_save_requires_path_when_no_index_path(self, store):
        with pytest.raises(DatasetError, match="requires a 'path' argument"):
            store.save()

    def test_save_writes_index_and_sidecar(self, tmp_path, store, sample_records):
        store.add(sample_records)
        target = str(tmp_path / "out.index")
        store.save(path=target)
        assert (tmp_path / "out.index").exists()
        assert (tmp_path / "out.index.meta.json").exists()
        assert not (tmp_path / "out.index.tmp").exists()

    def test_save_uses_constructor_index_path_by_default(self, tmp_path, sample_records):
        index_path = str(tmp_path / "ctor.index")
        ds = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store = ds.load()
        store.add(sample_records)
        store.save()
        assert (tmp_path / "ctor.index").exists()

    def test_save_path_override_is_not_remembered(self, tmp_path, store, sample_records):
        store.add(sample_records)
        other_path = str(tmp_path / "override.index")
        store.save(path=other_path)
        # a later bare save() still requires a path, since the override wasn't remembered
        with pytest.raises(DatasetError, match="requires a 'path' argument"):
            store.save()

    def test_save_reload_roundtrip_preserves_properties(self, tmp_path, sample_records):
        index_path = str(tmp_path / "rt.index")
        ds1 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store1 = ds1.load()
        store1.add(sample_records)
        store1.save()

        ds2 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        store2 = ds2.load()
        store2.delete(ids=["doc2"])
        assert store2.describe()["count"] == 2
        hits = store2.search(vector=[0.0, 0.0, 1.0, 0.0], top_k=1)
        assert hits[0]["id"] == "doc3"
        assert hits[0]["properties"] == {"topic": "ai"}

    def test_save_reload_roundtrip_through_trained_ivf(self, tmp_path):
        index_path = str(tmp_path / "ivf_rt.index")
        ds1 = FAISSVectorStoreDataset(
            dimension=8, index_factory="IVF4,Flat", index_path=index_path
        )
        store1 = ds1.load()
        train_vecs = np.random.rand(200, 8).astype("float32").tolist()
        store1.train(train_vecs)
        store1.add([{"id": "v0", "vector": [0.1] * 8}])
        store1.save()

        ds2 = FAISSVectorStoreDataset(
            dimension=8, index_factory="IVF4,Flat", index_path=index_path
        )
        store2 = ds2.load()
        assert store2.raw_client.is_trained
        assert store2.describe()["count"] == 1

    def test_save_wraps_index_write_exception(self, tmp_path, store, sample_records):
        store.add(sample_records)
        with patch(f"{MODULE}.faiss.write_index", side_effect=RuntimeError("disk full")):
            with pytest.raises(DatasetError, match="writing the FAISS index"):
                store.save(path=str(tmp_path / "should_not_matter.index"))


class TestEndToEnd:
    def test_add_search_delete_save_reload_roundtrip(self, tmp_path):
        index_path = str(tmp_path / "e2e.index")
        ds = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        with ds.load() as store:
            ids = store.add(
                [
                    {"id": "a", "properties": {"topic": "x"}, "vector": [1.0, 0.0, 0.0, 0.0]},
                    {"id": "b", "properties": {"topic": "y"}, "vector": [0.0, 1.0, 0.0, 0.0]},
                ]
            )
            assert ids == ["a", "b"]
            hits = store.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=1)
            assert hits[0]["id"] == "a"
            store.delete(ids=["b"])
            store.save()

        ds2 = FAISSVectorStoreDataset(dimension=4, index_path=index_path)
        with ds2.load() as store2:
            assert store2.describe()["count"] == 1
            assert store2.search(vector=[1.0, 0.0, 0.0, 0.0], top_k=5)[0]["id"] == "a"
