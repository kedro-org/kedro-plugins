"""``ChromaDBDataset`` loads and saves data from/to ChromaDB collections."""

from __future__ import annotations

from typing import Any

import chromadb
from chromadb.api.models.Collection import Collection
from chromadb.errors import NotFoundError
from kedro.io.core import AbstractDataset, DatasetError


class ChromaDBDataset(AbstractDataset[dict[str, Any], dict[str, Any]]):
    """``ChromaDBDataset`` loads and saves data from/to ChromaDB collections.

    ChromaDB is a vector database for building AI applications. This dataset allows you to
    interact with ChromaDB collections for storing and retrieving documents with embeddings.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        my_collection:
          type: chromadb.ChromaDBDataset
          collection_name: "documents"
          client_type: "persistent"
          client_settings:
            path: "./chroma_db"
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets_experimental.chromadb import ChromaDBDataset
        >>>
        >>> # Save data to ChromaDB
        >>> data = {
        ...     "documents": ["This is a document", "This is another document"],
        ...     "metadatas": [{"type": "text"}, {"type": "text"}],
        ...     "ids": ["doc1", "doc2"]
        ... }
        >>> dataset = ChromaDBDataset(collection_name="test_collection")
        >>> dataset.save(data)
        >>>
        >>> # Load data from ChromaDB
        >>> loaded_data = dataset.load()
        >>> print(loaded_data["documents"])  # ['This is a document', 'This is another document']
        >>>
        >>> # Query for similar vectors (efficient for large datasets)
        >>> query_dataset = ChromaDBDataset(
        ...     collection_name="documents",
        ...     load_args={
        ...         "query_texts": ["machine learning"],
        ...         "n_results": 5,
        ...         "include": ["documents", "metadatas", "distances"]
        ...     }
        ... )
        >>> results = query_dataset.load()  # Returns top-5 similar documents

    """
    # Attribute annotations for IDEs / type-checkers (instance values set in __init__)
    _client: chromadb.Client | None
    _collection: Collection | None

    def __init__(  # noqa: PLR0913
        self,
        *,
        collection_name: str,
        client_type: str = "ephemeral",
        client_settings: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``ChromaDBDataset``.

        Args:
            collection_name: The name of the ChromaDB collection.
            client_type: Type of ChromaDB client. Options: "ephemeral", "persistent", "http".
                Defaults to "ephemeral".
            client_settings: Settings for the ChromaDB client. For "persistent", use {"path": "/path/to/db"}.
                For "http", use {"host": "localhost", "port": 8000}.
            load_args: Additional arguments for loading data from ChromaDB collection.
                Can include "where", "where_document", "include", "n_results", etc.
                For vector similarity queries, use:
                - "query_embeddings": List of embeddings to query for similarity
                - "query_texts": List of texts to query for similarity
                - "n_results": Number of results to return (default: 10)
                - "where": Metadata filter conditions
                - "where_document": Document content filter conditions
            save_args: Additional arguments for saving data to ChromaDB collection.
                Can include "embeddings" if you want to provide custom embeddings.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._collection_name = collection_name
        self._client_type = client_type
        self._client_settings = client_settings or {}
        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self.metadata = metadata
        # Initialize instance attributes (actual annotations are at class-level)
        self._client = None
        self._collection = None

    def _create_client(self) -> chromadb.Client:
        """Create ChromaDB client based on configuration."""
        if self._client_type == "ephemeral":
            return chromadb.EphemeralClient()
        elif self._client_type == "persistent":
            path = self._client_settings.get("path", "./chroma_db")
            return chromadb.PersistentClient(path=path, **{
                k: v for k, v in self._client_settings.items() if k != "path"
            })
        elif self._client_type == "http":
            host = self._client_settings.get("host", "localhost")
            port = self._client_settings.get("port", 8000)
            return chromadb.HttpClient(host=host, port=port, **{
                k: v for k, v in self._client_settings.items() if k not in ["host", "port"]
            })
        else:
            raise DatasetError(
                f"Unsupported client_type: {self._client_type}. "
                f"Must be one of: 'ephemeral', 'persistent', 'http'"
            )

    def _get_client(self) -> chromadb.Client:
        """Get or create the ChromaDB client."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _get_collection(self, create_if_missing: bool = True) -> Collection | None:
        """Get or create the ChromaDB collection.

        Args:
            create_if_missing: If True, creates the collection if it doesn't exist.
                              If False, returns None when collection is not found.

        Returns:
            Collection object if found/created, None if not found and create_if_missing=False.
        """
        if self._collection is None:
            client = self._get_client()
            try:
                self._collection = client.get_collection(name=self._collection_name)
            except NotFoundError:
                if create_if_missing:
                    # Collection doesn't exist, create it
                    self._collection = client.create_collection(name=self._collection_name)
                else:
                    # Don't create collection, return None instead of raising
                    return None
        return self._collection

    def _describe(self) -> dict[str, Any]:
        """Returns a dictionary describing the dataset configuration."""
        return {
            "collection_name": self._collection_name,
            "client_type": self._client_type,
            "client_settings": self._client_settings,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def load(self) -> dict[str, Any]:
        """Loads data from the ChromaDB collection.

        Returns:
            dict[str, Any]: A dictionary containing the collection data with keys:
                - "documents": List of document texts
                - "metadatas": List of metadata dictionaries
                - "ids": List of document IDs
                - "embeddings": List of embeddings (if included)
        """
        collection = self._get_collection(create_if_missing=False)

        # If collection doesn't exist, return empty result rather than creating it
        if collection is None:
            return {"documents": [], "metadatas": [], "ids": [], "embeddings": []}

        # Prepare load arguments
        load_args = {
            "include": ["documents", "metadatas", "embeddings"],
            **self._load_args
        }

        try:
            # Use query() for vector similarity search or filtering
            if any(key in load_args for key in ["query_embeddings", "query_texts", "where", "where_document"]):
                # Vector similarity query - more efficient for large datasets
                if "n_results" not in load_args:
                    load_args["n_results"] = 10  # Default limit for queries
                result = collection.query(**load_args)
            else:
                # Use get() for retrieving all documents (not recommended for large collections)
                if "n_results" in load_args:
                    # Convert n_results to limit for get() method
                    load_args["limit"] = load_args.pop("n_results")
                result = collection.get(**load_args)

            return {
                "documents": result.get("documents", []),
                "metadatas": result.get("metadatas", []),
                "ids": result.get("ids", []),
                "embeddings": result.get("embeddings", [])
            }
        except Exception as e:
            raise DatasetError(
                f"Failed to load data from ChromaDB collection '{self._collection_name}': {e}"
            ) from e

    def save(self, data: dict[str, Any]) -> None:
        """Saves data to the ChromaDB collection.

        Args:
            data: A dictionary containing the data to save. Expected keys:
                - "documents": List of document texts (required)
                - "ids": List of document IDs (required)
                - "metadatas": List of metadata dictionaries (optional)
                - "embeddings": List of embeddings (optional, will be auto-generated if not provided)
        """
        if not isinstance(data, dict):
            raise DatasetError(f"Data must be a dictionary, got {type(data)}")

        if "documents" not in data or "ids" not in data:
            raise DatasetError("Data must contain 'documents' and 'ids' keys")

        collection = self._get_collection(create_if_missing=True)

        if collection is None:
            raise DatasetError(f"Failed to access or create ChromaDB collection '{self._collection_name}'")

        try:
            # Prepare the data for ChromaDB
            add_kwargs = {
                "documents": data["documents"],
                "ids": data["ids"],
                **self._save_args
            }

            # Add optional fields if present
            if "metadatas" in data:
                add_kwargs["metadatas"] = data["metadatas"]
            if "embeddings" in data:
                add_kwargs["embeddings"] = data["embeddings"]

            # Add documents to collection
            collection.add(**add_kwargs)

        except Exception as e:
            raise DatasetError(
                f"Failed to save data to ChromaDB collection '{self._collection_name}': {e}"
            ) from e

    def exists(self) -> bool:
        """Checks if the collection exists and contains data."""
        try:
            collection = self._collection or self._get_collection(create_if_missing=False)
            # In case both return None
            if collection is None:
                return False
            return collection.count() > 0
        except Exception:
            return False
            # Use the same collection instance if we already have it
            if self._collection is not None:
                count = self._collection.count()
                return count > 0

            # Otherwise try to get the collection from the client
            collection = self._get_collection(create_if_missing=False)
            count = collection.count()
            return count > 0
        except Exception:
            return False
