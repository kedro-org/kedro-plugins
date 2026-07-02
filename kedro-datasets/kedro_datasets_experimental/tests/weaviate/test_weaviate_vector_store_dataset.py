"""Tests for WeaviateVectorStoreDataset and WeaviateVectorStoreHandle (ST2).

All tests use mocks — no real Weaviate connection is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from kedro.io.core import DatasetError

from kedro_datasets_experimental.weaviate.weaviate_vector_store_dataset import (
    WeaviateVectorStoreDataset,
    WeaviateVectorStoreHandle,
)

MODULE = "kedro_datasets_experimental.weaviate.weaviate_vector_store_dataset"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_client():
    client = MagicMock()
    client.collections.exists.return_value = False
    client.collections.create.return_value = MagicMock(name="MyCollection")
    client.collections.get.return_value = MagicMock(name="MyCollection")
    return client


@pytest.fixture
def mock_collection(mock_client):
    collection = mock_client.collections.create.return_value
    collection.name = "MyCollection"
    agg_result = MagicMock()
    agg_result.total_count = 42
    collection.aggregate.over_all.return_value = agg_result
    return collection


@pytest.fixture
def local_dataset():
    return WeaviateVectorStoreDataset(collection_name="MyCollection")


@pytest.fixture
def cloud_dataset():
    return WeaviateVectorStoreDataset(
        collection_name="MyCollection",
        connection_type="cloud",
        url="https://my-cluster.weaviate.network",
        credentials={"api_key": "secret"},  # pragma: allowlist secret
    )


# ---------------------------------------------------------------------------
# WeaviateVectorStoreDataset — init and describe
# ---------------------------------------------------------------------------


class TestDatasetInit:
    def test_defaults(self, local_dataset):
        assert local_dataset._collection_name == "MyCollection"
        assert local_dataset._connection_type == "local"
        assert local_dataset._url is None
        assert local_dataset._connection_params == {}
        assert local_dataset._credentials == {}
        assert local_dataset._create_collection_if_missing is True
        assert local_dataset.metadata is None

    def test_cloud_params(self, cloud_dataset):
        assert cloud_dataset._connection_type == "cloud"
        assert cloud_dataset._url == "https://my-cluster.weaviate.network"
        assert cloud_dataset._credentials == {"api_key": "secret"}  # pragma: allowlist secret

    def test_none_dicts_become_empty(self):
        ds = WeaviateVectorStoreDataset(
            collection_name="X",
            connection_params=None,
            credentials=None,
        )
        assert ds._connection_params == {}
        assert ds._credentials == {}

    def test_metadata_stored(self):
        ds = WeaviateVectorStoreDataset(
            collection_name="X", metadata={"owner": "team-a"}
        )
        assert ds.metadata == {"owner": "team-a"}


class TestDescribe:
    def test_local_describe(self, local_dataset):
        desc = local_dataset._describe()
        assert desc == {
            "collection_name": "MyCollection",
            "connection_type": "local",
            "url": None,
            "create_collection_if_missing": True,
        }

    def test_cloud_describe(self, cloud_dataset):
        desc = cloud_dataset._describe()
        assert desc["connection_type"] == "cloud"
        assert desc["url"] == "https://my-cluster.weaviate.network"

    def test_describe_omits_credentials(self, cloud_dataset):
        desc = cloud_dataset._describe()
        assert "credentials" not in desc
        assert "api_key" not in str(desc)


# ---------------------------------------------------------------------------
# WeaviateVectorStoreDataset — save() disabled
# ---------------------------------------------------------------------------


class TestSaveDisabled:
    def test_save_raises(self, local_dataset):
        with pytest.raises(DatasetError, match="Saving is not supported"):
            local_dataset.save({"data": "value"})


# ---------------------------------------------------------------------------
# WeaviateVectorStoreDataset — _connect
# ---------------------------------------------------------------------------


class TestConnect:
    def test_local_default_host(self, mock_client):
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client) as p:
            ds = WeaviateVectorStoreDataset(collection_name="C")
            ds._connect()
            p.assert_called_once_with(host="localhost")

    def test_local_custom_host(self, mock_client):
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client) as p:
            ds = WeaviateVectorStoreDataset(collection_name="C", url="myhost")
            ds._connect()
            p.assert_called_once_with(host="myhost")

    def test_local_extra_params_forwarded(self, mock_client):
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client) as p:
            ds = WeaviateVectorStoreDataset(
                collection_name="C",
                connection_params={"port": 9090, "grpc_port": 50052},
            )
            ds._connect()
            p.assert_called_once_with(host="localhost", port=9090, grpc_port=50052)

    def test_cloud_with_api_key(self, mock_client):
        mock_auth = MagicMock()
        with (
            patch(f"{MODULE}.weaviate.connect_to_weaviate_cloud", return_value=mock_client) as p,
            patch(f"{MODULE}.wvc.init.Auth.api_key", return_value=mock_auth) as auth_p,
        ):
            ds = WeaviateVectorStoreDataset(
                collection_name="C",
                connection_type="cloud",
                url="https://cluster.weaviate.network",
                credentials={"api_key": "mykey"},  # pragma: allowlist secret
            )
            ds._connect()
            auth_p.assert_called_once_with("mykey")  # pragma: allowlist secret
            p.assert_called_once_with(
                cluster_url="https://cluster.weaviate.network",
                auth_credentials=mock_auth,
            )

    def test_cloud_without_api_key(self, mock_client):
        with patch(f"{MODULE}.weaviate.connect_to_weaviate_cloud", return_value=mock_client) as p:
            ds = WeaviateVectorStoreDataset(
                collection_name="C",
                connection_type="cloud",
                url="https://cluster.weaviate.network",
            )
            ds._connect()
            p.assert_called_once_with(
                cluster_url="https://cluster.weaviate.network",
                auth_credentials=None,
            )

    def test_cloud_missing_url_raises(self):
        ds = WeaviateVectorStoreDataset(
            collection_name="C", connection_type="cloud"
        )
        with pytest.raises(DatasetError, match="'url' is required"):
            ds._connect()

    def test_unknown_connection_type_raises(self):
        ds = WeaviateVectorStoreDataset(
            collection_name="C", connection_type="grpc"  # type: ignore[arg-type]
        )
        with pytest.raises(DatasetError, match="Unknown connection_type"):
            ds._connect()

    def test_custom_params_forwarded(self, mock_client):
        params = {
            "http_host": "h",
            "http_port": 80,
            "http_secure": False,
            "grpc_host": "h",
            "grpc_port": 50051,
            "grpc_secure": False,
        }
        with patch(f"{MODULE}.weaviate.connect_to_custom", return_value=mock_client) as p:
            ds = WeaviateVectorStoreDataset(
                collection_name="C",
                connection_type="custom",
                connection_params=params,
            )
            ds._connect()
            p.assert_called_once_with(**params)

    def test_connection_error_wrapped(self):
        with patch(
            f"{MODULE}.weaviate.connect_to_local",
            side_effect=RuntimeError("timeout"),
        ):
            ds = WeaviateVectorStoreDataset(collection_name="C")
            with pytest.raises(DatasetError, match="Failed to connect to Weaviate"):
                ds._connect()


# ---------------------------------------------------------------------------
# WeaviateVectorStoreDataset — _load
# ---------------------------------------------------------------------------


class TestLoad:
    def test_load_creates_collection_when_missing(self, mock_client, mock_collection):
        mock_client.collections.exists.return_value = False
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client):
            ds = WeaviateVectorStoreDataset(collection_name="MyCollection")
            handle = ds._load()
            assert isinstance(handle, WeaviateVectorStoreHandle)
            mock_client.collections.exists.assert_called_once_with("MyCollection")
            mock_client.collections.create.assert_called_once_with("MyCollection")

    def test_load_gets_existing_collection(self, mock_client, mock_collection):
        mock_client.collections.exists.return_value = True
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client):
            ds = WeaviateVectorStoreDataset(collection_name="MyCollection")
            handle = ds._load()
            assert isinstance(handle, WeaviateVectorStoreHandle)
            mock_client.collections.get.assert_called_once_with("MyCollection")
            mock_client.collections.create.assert_not_called()

    def test_load_get_only_when_no_create(self, mock_client):
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client):
            ds = WeaviateVectorStoreDataset(
                collection_name="MyCollection",
                create_collection_if_missing=False,
            )
            ds._load()
            mock_client.collections.get.assert_called_once_with("MyCollection")
            mock_client.collections.exists.assert_not_called()

    def test_collection_error_closes_client_and_raises(self, mock_client):
        mock_client.collections.exists.side_effect = RuntimeError("not found")
        with patch(f"{MODULE}.weaviate.connect_to_local", return_value=mock_client):
            ds = WeaviateVectorStoreDataset(collection_name="Missing")
            with pytest.raises(DatasetError, match="Failed to access Weaviate collection"):
                ds._load()
            mock_client.close.assert_called_once()


# ---------------------------------------------------------------------------
# WeaviateVectorStoreHandle — lifecycle
# ---------------------------------------------------------------------------


class TestHandle:
    @pytest.fixture
    def handle(self, mock_client, mock_collection):
        return WeaviateVectorStoreHandle(mock_client, mock_collection)

    def test_raw_client_returns_client(self, handle, mock_client):
        assert handle.raw_client is mock_client

    def test_close_calls_client_close(self, handle, mock_client):
        handle.close()
        mock_client.close.assert_called_once()

    def test_close_is_idempotent(self, handle, mock_client):
        handle.close()
        handle.close()
        mock_client.close.assert_called_once()

    def test_context_manager_closes_on_exit(self, handle, mock_client):
        with handle:
            pass
        mock_client.close.assert_called_once()

    def test_context_manager_closes_on_exception(self, handle, mock_client):
        with pytest.raises(ValueError):
            with handle:
                raise ValueError("boom")
        mock_client.close.assert_called_once()

    def test_context_manager_returns_self(self, handle):
        with handle as h:
            assert h is handle

    def test_describe_returns_name_and_count(self, handle, mock_collection):
        result = handle.describe()
        assert result == {"collection": "MyCollection", "count": 42}
        mock_collection.aggregate.over_all.assert_called_once_with(total_count=True)

    def test_search_raises_not_implemented(self, handle):
        with pytest.raises(NotImplementedError, match="ST4"):
            handle.search(vector=[0.1, 0.2])


# ---------------------------------------------------------------------------
# WeaviateVectorStoreHandle — add()
# ---------------------------------------------------------------------------


class TestHandleAdd:
    @pytest.fixture
    def handle(self, mock_client, mock_collection):
        return WeaviateVectorStoreHandle(mock_client, mock_collection)

    @pytest.fixture
    def insert_result(self):
        import uuid
        result = MagicMock()
        result.uuids = {0: uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
                        1: uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")}
        result.errors = {}
        return result

    def test_add_calls_insert_many(self, handle, mock_collection, insert_result):
        mock_collection.data.insert_many.return_value = insert_result
        records = [
            {"vector": [0.1, 0.2], "text": "hello"},
            {"vector": [0.3, 0.4], "text": "world"},
        ]
        handle.add(records)
        mock_collection.data.insert_many.assert_called_once()
        objects = mock_collection.data.insert_many.call_args[0][0]
        assert len(objects) == 2
        assert objects[0].properties == {"text": "hello"}
        assert objects[0].vector == [0.1, 0.2]
        assert objects[1].properties == {"text": "world"}

    def test_add_returns_uuid_strings(self, handle, mock_collection, insert_result):
        mock_collection.data.insert_many.return_value = insert_result
        uuids = handle.add([{"vector": [0.1], "text": "a"}, {"vector": [0.2], "text": "b"}])
        assert uuids == [
            "aaaaaaaa-0000-0000-0000-000000000001",
            "bbbbbbbb-0000-0000-0000-000000000002",
        ]

    def test_add_passes_optional_id(self, handle, mock_collection, insert_result):
        mock_collection.data.insert_many.return_value = insert_result
        handle.add([{"id": "my-uuid", "vector": [0.1], "text": "x"}])
        obj = mock_collection.data.insert_many.call_args[0][0][0]
        assert obj.uuid == "my-uuid"
        assert "id" not in obj.properties

    def test_add_raises_on_partial_errors(self, handle, mock_collection):
        result = MagicMock()
        result.uuids = {}
        result.errors = {0: "connection reset"}
        mock_collection.data.insert_many.return_value = result
        with pytest.raises(DatasetError, match="add\\(\\) failed for 1 record"):
            handle.add([{"vector": [0.1], "text": "bad"}])


# ---------------------------------------------------------------------------
# WeaviateVectorStoreHandle — delete()
# ---------------------------------------------------------------------------


class TestHandleDelete:
    @pytest.fixture
    def handle(self, mock_client, mock_collection):
        return WeaviateVectorStoreHandle(mock_client, mock_collection)

    def test_delete_by_ids_calls_delete_by_id(self, handle, mock_collection):
        handle.delete(ids=["uuid-1", "uuid-2"])
        assert mock_collection.data.delete_by_id.call_count == 2
        mock_collection.data.delete_by_id.assert_any_call("uuid-1")
        mock_collection.data.delete_by_id.assert_any_call("uuid-2")

    def test_delete_by_filter_calls_delete_many(self, handle, mock_collection):
        f = MagicMock()
        handle.delete(filters=f)
        mock_collection.data.delete_many.assert_called_once_with(where=f)

    def test_delete_requires_ids_or_filters(self, handle):
        with pytest.raises(DatasetError, match="requires exactly one"):
            handle.delete()

    def test_delete_rejects_both(self, handle):
        with pytest.raises(DatasetError, match="not both"):
            handle.delete(ids=["x"], filters=MagicMock())
