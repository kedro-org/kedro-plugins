"""``WeaviateVectorStoreDataset`` connects to a Weaviate collection as a vector store."""

from __future__ import annotations

from typing import Any, Literal

import weaviate
import weaviate.classes as wvc
from kedro.io.core import DatasetError
from weaviate.classes.data import DataObject

from kedro_datasets_experimental.vectorstore_base import (
    AbstractVectorStoreDataset,
    VectorStoreHandle,
)


class WeaviateVectorStoreHandle(VectorStoreHandle):
    """Handle for interacting with a Weaviate collection.

    Returned by ``WeaviateVectorStoreDataset.load()``. Owns the gRPC connection
    to Weaviate; the connection must be closed after use, either by calling
    ``close()`` explicitly or by using the handle as a context manager::

        with catalog.load("my_store") as store:
            store.describe()

    The ``raw_client`` property exposes the underlying ``weaviate.WeaviateClient``
    for operations outside this interface.
    """

    def __init__(
        self,
        client: weaviate.WeaviateClient,
        collection: Any,
    ) -> None:
        self._client = client
        self._collection = collection
        self._closed = False

    @property
    def raw_client(self) -> weaviate.WeaviateClient:
        """The underlying ``weaviate.WeaviateClient`` instance."""
        return self._client

    def close(self) -> None:
        """Close the gRPC connection. Safe to call more than once."""
        if not self._closed:
            self._client.close()
            self._closed = True

    def describe(self) -> dict[str, Any]:
        """Return collection name and current record count."""
        result = self._collection.aggregate.over_all(total_count=True)
        return {
            "collection": self._collection.name,
            "count": result.total_count,
        }

    def add(self, records: list[dict[str, Any]]) -> list[str]:
        """Insert records into the collection and return their UUIDs.

        Each record is a plain dict with the following keys:

        - ``"properties"`` — the object's properties (``dict``).
        - ``"vector"`` — the embedding (``list[float]``); optional when the
          collection has a server-side vectorizer configured.
        - ``"id"`` — an optional UUID string; Weaviate auto-generates one if absent.

        Args:
            records: Records to insert.

        Returns:
            List of UUID strings for the inserted objects, in the same order
            as the input records.

        Raises:
            DatasetError: If the batch insert call fails, or if Weaviate
                returns errors for one or more objects.
        """
        objects = []
        for record in records:
            props = record.get("properties", {})
            vector = record.get("vector")
            uid = record.get("id")
            objects.append(DataObject(properties=props, uuid=uid, vector=vector))

        try:
            result = self._collection.data.insert_many(objects)
        except Exception as e:
            raise DatasetError(f"add() failed: {e}") from e

        if result.errors:
            raise DatasetError(
                f"add() failed for {len(result.errors)} record(s): "
                + ", ".join(f"index {i}: {e}" for i, e in result.errors.items())
            )
        return [str(uid) for uid in result.uuids.values()]

    def delete(
        self,
        *,
        ids: list[str] | None = None,
        filters: Any = None,
    ) -> None:
        """Delete objects from the collection by ID or filter.

        Exactly one of ``ids`` or ``filters`` must be provided.

        Args:
            ids: List of UUID strings to delete individually.
            filters: A ``weaviate.classes.query.Filter`` expression that
                selects objects to delete (passed directly to
                ``collection.data.delete_many(where=filters)``).

        Raises:
            DatasetError: If neither or both arguments are supplied, or if
                the deletion call to Weaviate fails.
        """
        if ids is None and filters is None:
            raise DatasetError("delete() requires exactly one of 'ids' or 'filters'.")
        if ids is not None and filters is not None:
            raise DatasetError("delete() accepts 'ids' or 'filters', not both.")

        try:
            if ids is not None:
                for uid in ids:
                    self._collection.data.delete_by_id(uid)
            else:
                self._collection.data.delete_many(where=filters)
        except Exception as e:
            raise DatasetError(f"delete() failed: {e}") from e

    def search(
        self,
        *,
        vector: list[float] | None = None,
        text: str | None = None,
        top_k: int = 10,
        filters: Any = None,
    ) -> list[dict[str, Any]]:
        """Search the collection by vector or text and return the top matches.

        Exactly one of ``vector`` or ``text`` must be provided.  ``vector``
        triggers ``near_vector`` search; ``text`` triggers ``near_text``
        (requires a vectorizer configured on the collection).

        Args:
            vector: Query embedding for similarity search.
            text: Query string for near-text search.
            top_k: Maximum number of results to return. Defaults to 10.
            filters: A ``weaviate.classes.query.Filter`` expression to restrict
                the search scope (passed directly to the underlying query).

        Returns:
            List of result dicts, each containing ``"id"`` (UUID string),
            ``"properties"`` (dict of stored object properties), and
            ``"distance"`` (float).

        Raises:
            DatasetError: If neither or both of ``vector``/``text`` are
                supplied, or if the query call to Weaviate fails.
        """
        if vector is None and text is None:
            raise DatasetError("search() requires exactly one of 'vector' or 'text'.")
        if vector is not None and text is not None:
            raise DatasetError("search() accepts 'vector' or 'text', not both.")

        common_kwargs = dict(
            limit=top_k,
            filters=filters,
            return_metadata=wvc.query.MetadataQuery(distance=True),
        )

        try:
            if vector is not None:
                results = self._collection.query.near_vector(near_vector=vector, **common_kwargs)
            else:
                results = self._collection.query.near_text(query=text, **common_kwargs)
        except Exception as e:
            raise DatasetError(f"search() failed: {e}") from e

        return [
            {
                "id": str(obj.uuid),
                "properties": dict(obj.properties),
                "distance": obj.metadata.distance,
            }
            for obj in results.objects
        ]


class WeaviateVectorStoreDataset(AbstractVectorStoreDataset):
    """Connect to a Weaviate collection and return a ``WeaviateVectorStoreHandle``.

    ``load()`` opens a connection to Weaviate, resolves (or creates) the target
    collection, and returns a handle.  All read/write operations go through the
    handle.  ``save()`` is intentionally disabled and raises ``DatasetError``.

    The handle owns the underlying gRPC connection; callers **must** close it::

        with catalog.load("my_store") as store:
            store.describe()

    Three connection modes are supported, selected with ``connection_type``:

    - ``"local"`` (default) — connects to a locally running Weaviate instance.
    - ``"cloud"`` — connects to Weaviate Cloud; requires ``url`` (cluster URL)
      and, typically, ``credentials: {api_key: ...}``.
    - ``"custom"`` — passes ``connection_params`` directly to
      ``weaviate.connect_to_custom()`` for self-hosted deployments with
      non-standard networking.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        Local instance (default):

        ```yaml
        my_store:
          type: weaviate.WeaviateVectorStoreDataset
          collection_name: MyCollection
        ```

        Weaviate Cloud:

        ```yaml
        my_store:
          type: weaviate.WeaviateVectorStoreDataset
          collection_name: MyCollection
          connection_type: cloud
          url: "https://my-cluster.weaviate.network"
          credentials:
            api_key: "${WEAVIATE_API_KEY}"
        ```

        Self-hosted with custom networking:

        ```yaml
        my_store:
          type: weaviate.WeaviateVectorStoreDataset
          collection_name: MyCollection
          connection_type: custom
          connection_params:
            http_host: my-host.internal
            http_port: 8080
            http_secure: false
            grpc_host: my-host.internal
            grpc_port: 50051
            grpc_secure: false
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets_experimental.weaviate import WeaviateVectorStoreDataset
        >>> dataset = WeaviateVectorStoreDataset(collection_name="MyCollection")
        >>> with dataset.load() as store:
        ...     print(store.describe())
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        collection_name: str,
        connection_type: Literal["local", "cloud", "custom"] = "local",
        url: str | None = None,
        connection_params: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        create_collection_if_missing: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Create a new ``WeaviateVectorStoreDataset``.

        Args:
            collection_name: Name of the Weaviate collection to connect to.
            connection_type: How to connect — ``"local"``, ``"cloud"``, or
                ``"custom"``. Defaults to ``"local"``.
            url: For ``"cloud"``: the Weaviate Cloud cluster URL (e.g.
                ``"https://my-cluster.weaviate.network"``).
                For ``"local"``: the host name (defaults to ``"localhost"``).
                Ignored for ``"custom"``.
            connection_params: Extra keyword arguments forwarded to the
                underlying ``weaviate.connect_to_*`` function.  For
                ``"local"``: optional overrides such as ``port`` or
                ``grpc_port``.  For ``"custom"``: all required networking
                parameters (``http_host``, ``http_port``, ``grpc_host``, etc.).
            credentials: Sensitive connection values, typically supplied through
                Kedro's credentials store.  Recognised key: ``"api_key"``
                (used for ``"cloud"`` connections).
            create_collection_if_missing: When ``True`` (default), the
                collection is created if it does not already exist.  When
                ``False``, ``load()`` raises if the collection is absent.
            metadata: Arbitrary metadata passed through by Kedro; ignored by
                this dataset.
        """
        self._collection_name = collection_name
        self._connection_type = connection_type
        self._url = url
        self._connection_params = connection_params or {}
        self._credentials = credentials or {}
        self._create_collection_if_missing = create_collection_if_missing
        self.metadata = metadata

    def _connect(self) -> weaviate.WeaviateClient:
        try:
            if self._connection_type == "cloud":
                if not self._url:
                    raise DatasetError(
                        "'url' is required when connection_type='cloud'."
                    )
                api_key = self._credentials.get("api_key")
                auth = wvc.init.Auth.api_key(api_key) if api_key else None
                return weaviate.connect_to_weaviate_cloud(
                    cluster_url=self._url,
                    auth_credentials=auth,
                    **self._connection_params,
                )
            elif self._connection_type == "custom":
                return weaviate.connect_to_custom(**self._connection_params)
            elif self._connection_type == "local":
                return weaviate.connect_to_local(
                    host=self._url or "localhost",
                    **self._connection_params,
                )
            else:
                raise DatasetError(
                    f"Unknown connection_type: '{self._connection_type}'. "
                    "Must be one of: 'local', 'cloud', 'custom'."
                )
        except DatasetError:
            raise
        except Exception as e:
            raise DatasetError(
                f"Failed to connect to Weaviate "
                f"(connection_type='{self._connection_type}'): {e}"
            ) from e

    def _load(self) -> WeaviateVectorStoreHandle:
        client = self._connect()
        try:
            if self._create_collection_if_missing:
                if client.collections.exists(self._collection_name):
                    collection = client.collections.get(self._collection_name)
                else:
                    collection = client.collections.create(self._collection_name)
            else:
                collection = client.collections.get(self._collection_name)
        except Exception as e:
            client.close()
            raise DatasetError(
                f"Failed to access Weaviate collection '{self._collection_name}': {e}"
            ) from e
        return WeaviateVectorStoreHandle(client, collection)

    def _describe(self) -> dict[str, Any]:
        return {
            "collection_name": self._collection_name,
            "connection_type": self._connection_type,
            "url": self._url,
            "create_collection_if_missing": self._create_collection_if_missing,
        }
