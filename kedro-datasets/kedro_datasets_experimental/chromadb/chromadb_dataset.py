"""`ChromaDBDataset` connects to a ChromaDB collection as a vector store."""

from __future__ import annotations

import uuid
from typing import Any, Literal

import chromadb
from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.errors import NotFoundError
from kedro.io.core import DatasetError

from kedro_datasets_experimental.vectorstore_base import (
    AbstractVectorStoreDataset,
    VectorStoreHandle,
)


class ChromaVectorStoreHandle(VectorStoreHandle):
    """Handle for interacting with a ChromaDB collection.

    Returned by `ChromaDBDataset.load()`. All reads and writes go through
    the handle; the dataset only configures the connection.

    Records passed to `add()` are dicts of the form
    `{"properties": dict, "vector": list[float], "id": str}`. The reserved
    `"document"` key inside `properties` maps to Chroma's documents field;
    all other keys are stored as Chroma metadata. `search()` merges them
    back into a single `"properties"` dict. When records carry no
    `"vector"`, Chroma embeds each record's `"document"` with the
    collection's embedding function (by default
    [all-MiniLM-L6-v2](https://docs.trychroma.com/docs/embeddings/embedding-functions),
    downloaded on first use).

    `close()` releases the client's resources (SQLite and HNSW file handles
    for persistent clients, the connection pool for HTTP clients); a closed
    handle must not be reused. For ephemeral clients `close()` is a no-op,
    since their in-memory store is shared by every ephemeral client in the
    process. Kedro never closes the handle for you — close it explicitly or
    use it as a context manager::

        with catalog.load("my_store") as store:
            store.add([{"properties": {"document": "hello"}, "vector": [0.1, 0.2]}])
            hits = store.search(vector=[0.1, 0.2], top_k=5)

    `raw_client` exposes the underlying `chromadb` client for operations
    outside this interface (e.g. `where_document` filters or `upsert`).
    """

    def __init__(
        self,
        client: ClientAPI,
        collection: Collection,
        close_client: bool = True,
    ) -> None:
        self._client = client
        self._collection = collection
        self._close_client = close_client
        self._closed = False

    @property
    def raw_client(self) -> ClientAPI:
        """The underlying `chromadb` client instance."""
        return self._client

    def close(self) -> None:
        """Release the client's resources. Idempotent; a no-op for ephemeral clients."""
        if not self._closed:
            if self._close_client:
                self._client.close()
            self._closed = True

    def describe(self) -> dict[str, Any]:
        """Return collection name and current record count."""
        return {
            "collection": self._collection.name,
            "count": self._collection.count(),
        }

    def add(self, records: list[dict[str, Any]]) -> list[str]:
        """Insert records into the collection and return their IDs.

        See the class docstring for the record format. Either all records
        in the batch carry a `"vector"` or none do. A UUID is generated for
        any record without an `"id"`. Records whose ID already exists in
        the collection are rejected (Chroma would otherwise skip them
        silently); to update existing records, use `raw_client` with
        `collection.upsert()`.

        Args:
            records: Records to insert.

        Returns:
            List of ID strings for the inserted records, in input order.

        Raises:
            DatasetError: If some records have a `"vector"` and others do
                not, if a record's ID already exists in the collection, or
                if the insert call to Chroma fails.
        """
        if not records:
            return []

        with_vector = sum(1 for record in records if record.get("vector") is not None)
        if 0 < with_vector < len(records):
            raise DatasetError(
                f"add() requires either all records or no records to carry a "
                f"'vector': got {with_vector} of {len(records)} with one. "
                "Chroma cannot mix precomputed and function-computed "
                "embeddings in a single batch."
            )

        ids: list[str] = []
        documents: list[str | None] = []
        metadatas: list[dict[str, Any] | None] = []
        embeddings: list[list[float]] = []
        for record in records:
            props = dict(record.get("properties") or {})
            documents.append(props.pop("document", None))
            metadatas.append(props or None)
            record_id = record.get("id")
            ids.append(uuid.uuid4().hex if record_id is None else record_id)
            if with_vector:
                embeddings.append(record["vector"])

        explicit_ids = [
            record["id"] for record in records if record.get("id") is not None
        ]
        if explicit_ids:
            try:
                existing = self._collection.get(ids=explicit_ids, include=[])["ids"]
            except Exception as e:
                raise DatasetError(f"add() failed: {e}") from e
            if existing:
                raise DatasetError(
                    f"add() rejected {len(existing)} record(s) whose ID already "
                    f"exists in the collection: {existing}. Chroma silently "
                    "skips existing IDs on insert; use raw_client with "
                    "collection.upsert() to update existing records."
                )

        add_kwargs: dict[str, Any] = {"ids": ids}
        if with_vector:
            add_kwargs["embeddings"] = embeddings
        if any(document is not None for document in documents):
            add_kwargs["documents"] = documents
        if any(metadata is not None for metadata in metadatas):
            add_kwargs["metadatas"] = metadatas

        try:
            self._collection.add(**add_kwargs)
        except Exception as e:
            raise DatasetError(f"add() failed: {e}") from e
        return ids

    def delete(
        self,
        *,
        ids: list[str] | None = None,
        filters: Any = None,
    ) -> None:
        """Delete records from the collection by ID or metadata filter.

        Exactly one of `ids` or `filters` must be provided.

        Args:
            ids: List of ID strings to delete.
            filters: A Chroma `where` filter dict (backend-native,
                MongoDB-style — e.g. `{"topic": "ml"}` or
                `{"$and": [...]}`) selecting the records to delete. For
                `where_document` full-text filters, use `raw_client`.

        Raises:
            DatasetError: If neither or both arguments are supplied, or if
                the deletion call to Chroma fails.
        """
        if ids is None and filters is None:
            raise DatasetError("delete() requires exactly one of 'ids' or 'filters'.")
        if ids is not None and filters is not None:
            raise DatasetError("delete() accepts 'ids' or 'filters', not both.")
        if ids is not None and not ids:
            return

        try:
            if ids is not None:
                self._collection.delete(ids=ids)
            else:
                self._collection.delete(where=filters)
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

        Exactly one of `vector` or `text` must be provided. `vector`
        queries with the embedding directly; `text` is embedded first with
        the collection's embedding function (downloading the default model
        on first use if the collection has no custom one).

        Args:
            vector: Query embedding for similarity search.
            text: Query string, embedded with the collection's embedding
                function.
            top_k: Maximum number of results to return. Defaults to 10.
            filters: A Chroma `where` filter dict (backend-native,
                MongoDB-style) restricting the search scope. For
                `where_document` full-text filters, use `raw_client`.

        Returns:
            List of result dicts, each containing `"id"` (str),
            `"properties"` (dict of stored metadata, plus the
            `"document"` key when the record has one), and `"distance"`
            (float).

        Raises:
            DatasetError: If neither or both of `vector`/`text` are
                supplied, or if the query call to Chroma fails.
        """
        if vector is None and text is None:
            raise DatasetError("search() requires exactly one of 'vector' or 'text'.")
        if vector is not None and text is not None:
            raise DatasetError("search() accepts 'vector' or 'text', not both.")

        query_kwargs: dict[str, Any] = {"n_results": top_k, "where": filters}
        if vector is not None:
            query_kwargs["query_embeddings"] = [vector]
        else:
            query_kwargs["query_texts"] = [text]

        try:
            result = self._collection.query(**query_kwargs)
        except Exception as e:
            raise DatasetError(f"search() failed: {e}") from e

        # Chroma returns one inner list per query; we always send exactly one.
        ids = result["ids"][0]
        documents = (result.get("documents") or [[None] * len(ids)])[0]
        metadatas = (result.get("metadatas") or [[None] * len(ids)])[0]
        distances = (result.get("distances") or [[None] * len(ids)])[0]

        hits = []
        for record_id, document, metadata, distance in zip(
            ids, documents, metadatas, distances
        ):
            properties = dict(metadata or {})
            if document is not None:
                properties["document"] = document
            hits.append(
                {"id": record_id, "properties": properties, "distance": distance}
            )
        return hits


class ChromaDBDataset(AbstractVectorStoreDataset):
    """Connect to a ChromaDB collection and return a `ChromaVectorStoreHandle`.

    `load()` creates a Chroma client, resolves (or creates) the target
    collection, and returns a handle. All read/write operations go through
    the handle. `save()` is intentionally disabled and raises
    `DatasetError`.

    Three client types are supported, selected with `client_type`:

    - `"ephemeral"` (default) — an in-memory store. Note that ephemeral
      clients created within the same process share state, and the data is
      lost when the process exits.
    - `"persistent"` — an on-disk store; set the location with
      `client_settings: {path: ...}`.
    - `"http"` — a running Chroma server; set `client_settings:
      {host: ..., port: ...}`.

    `collection_name` is a lookup key. When
    `create_collection_if_missing=True` (the default) the collection is
    created on first use with Chroma's defaults — including the default
    embedding function ([all-MiniLM-L6-v2](https://docs.trychroma.com/docs/embeddings/embedding-functions),
    downloaded on first use), which serves text search and vector-less
    `add()` calls. To use a custom embedding function or collection
    configuration, create the collection yourself first via the Chroma
    client.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        my_store:
          type: kedro_datasets_experimental.chromadb.ChromaDBDataset
          collection_name: documents
          client_type: persistent
          client_settings:
            path: ./chroma_db
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets_experimental.chromadb import ChromaDBDataset
        >>>
        >>> dataset = ChromaDBDataset(collection_name="documents")
        >>> store = dataset.load()
        >>> ids = store.add(
        ...     [
        ...         {
        ...             "properties": {"document": "A document about ML", "topic": "ml"},
        ...             "vector": [0.1, 0.2, 0.3],
        ...         }
        ...     ]
        ... )
        >>> hits = store.search(vector=[0.1, 0.2, 0.3], top_k=5)
        >>> store.delete(ids=ids)
    """

    def __init__(
        self,
        *,
        collection_name: str,
        client_type: Literal["ephemeral", "persistent", "http"] = "ephemeral",
        client_settings: dict[str, Any] | None = None,
        create_collection_if_missing: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Create a new `ChromaDBDataset`.

        Args:
            collection_name: Name of the ChromaDB collection to connect to.
            client_type: Type of ChromaDB client — `"ephemeral"`,
                `"persistent"`, or `"http"`. Defaults to `"ephemeral"`.
            client_settings: Keyword arguments for the client. For
                `"persistent"`: `{"path": "/path/to/db"}` (default path
                `"./chroma_db"`). For `"http"`: `{"host": ..., "port": ...}`
                (defaults `"localhost"` and `8000`). Extra keys are
                forwarded to the underlying `chromadb` client constructor.
            create_collection_if_missing: When `True` (default), the
                collection is created — with Chroma's default configuration
                and embedding function — if it does not already exist. When
                `False`, `load()` raises if the collection is absent.
            metadata: Arbitrary metadata passed through by Kedro; ignored by
                this dataset.
        """
        self._collection_name = collection_name
        self._client_type = client_type
        self._client_settings = client_settings or {}
        self._create_collection_if_missing = create_collection_if_missing
        self.metadata = metadata

    def _create_client(self) -> ClientAPI:
        settings = dict(self._client_settings)
        if self._client_type == "ephemeral":
            return chromadb.EphemeralClient(**settings)
        elif self._client_type == "persistent":
            return chromadb.PersistentClient(
                path=settings.pop("path", "./chroma_db"), **settings
            )
        elif self._client_type == "http":
            return chromadb.HttpClient(
                host=settings.pop("host", "localhost"),
                port=settings.pop("port", 8000),
                **settings,
            )
        else:
            raise DatasetError(
                f"Unsupported client_type: '{self._client_type}'. "
                "Must be one of: 'ephemeral', 'persistent', 'http'."
            )

    def _load(self) -> ChromaVectorStoreHandle:
        client = self._create_client()
        try:
            if self._create_collection_if_missing:
                collection = client.get_or_create_collection(
                    name=self._collection_name
                )
            else:
                collection = client.get_collection(name=self._collection_name)
        except NotFoundError as e:
            raise DatasetError(
                f"Chroma collection '{self._collection_name}' does not exist "
                "and create_collection_if_missing is False."
            ) from e
        except Exception as e:
            raise DatasetError(
                f"Failed to access Chroma collection '{self._collection_name}': {e}"
            ) from e
        # Closing an ephemeral client would destroy the process-shared
        # in-memory store, so only persistent/http handles close theirs.
        return ChromaVectorStoreHandle(
            client, collection, close_client=self._client_type != "ephemeral"
        )

    def _describe(self) -> dict[str, Any]:
        # client_settings is withheld: for http clients it can carry
        # authentication headers.
        return {
            "collection_name": self._collection_name,
            "client_type": self._client_type,
            "create_collection_if_missing": self._create_collection_if_missing,
        }
